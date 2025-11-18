#!/usr/bin/env python3
"""
Elasticsearch Ingest Statistics

Calculates daily ingest volume based on cluster storage and document counts.

Configuration:
    Create a .env file with the following variables:

    # Required - Elasticsearch URL
    ES_URL=https://es.example.com:9200

    # Required - Authentication (choose one method)
    # Method 1: Basic auth with username/password
    ES_USER=myuser
    ES_PASS=mypassword
    # Method 2: API key authentication
    ES_API_KEY=your_base64_encoded_api_key

    # Optional - Time window for average calculation
    DAYS_TO_AVERAGE=7  # Default: 7 days (valid range: 1-365)

    # Optional - SSH jumphost configuration
    SSH_USER=sshuser
    SSH_HOST=jumphost.example.com
    SSH_PASS=sshpassword
    # OR
    SSH_KEY=/path/to/key

    # Optional - Index pattern filter
    INDEX_PATTERN=logs-endpoint*
    # If not set or empty, analyzes all indices

Usage:
    python3 elastic_ingest_per_day.py
"""

import os
import re
import sys
from typing import Optional, Tuple
from urllib.parse import urlparse

import requests
import urllib3
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables from .env file
load_dotenv()


def get_primary_store_bytes(
    endpoint: str,
    auth: Optional[Tuple[str, str]] = None,
    headers: Optional[dict] = None,
    index_pattern: Optional[str] = None,
) -> int:
    """Sum the store size of all primary shards (bytes)."""
    # Build URL with optional index pattern
    if index_pattern:
        url = f"{endpoint.rstrip('/')}/_cat/shards/{index_pattern}"
    else:
        url = f"{endpoint.rstrip('/')}/_cat/shards"

    params = {
        "bytes": "b",  # return raw bytes, no units
        "format": "json",  # JSON so we can parse reliably
        "h": "prirep,store",  # only the columns we need
    }
    resp = requests.get(url, params=params, auth=auth, headers=headers, timeout=20, verify=False)
    resp.raise_for_status()
    total = 0
    for shard in resp.json():
        # keep only primary shards
        if shard.get("prirep") == "p":
            # value is already in bytes (e.g. "123456"), but be liberal just in case
            match = re.match(r"(\d+)", str(shard.get("store", "")))
            if match:
                total += int(match.group(1))
    return total


def get_total_docs(
    endpoint: str,
    auth: Optional[Tuple[str, str]] = None,
    headers: Optional[dict] = None,
    index_pattern: Optional[str] = None,
) -> int:
    """Return total document count from all primary shards."""
    # Build URL with optional index pattern
    if index_pattern:
        url = f"{endpoint.rstrip('/')}/{index_pattern}/_stats"
    else:
        url = f"{endpoint.rstrip('/')}/_stats"

    params = {"filter_path": "_all.primaries.docs.count"}
    resp = requests.get(url, params=params, auth=auth, headers=headers, timeout=20, verify=False)
    resp.raise_for_status()
    return resp.json()["_all"]["primaries"]["docs"]["count"]


def get_docs_last_nd(
    endpoint: str,
    days: int,
    auth: Optional[Tuple[str, str]] = None,
    headers: Optional[dict] = None,
    timestamp_field: str = "@timestamp",
    index_pattern: str = "_all",
) -> int:
    """
    Count documents with `timestamp_field` in the range:
        (now-{days}d, now) – *rounded to day boundaries*
    Equivalent Kibana KQL:  @timestamp >= now-{days}d/d and @timestamp <  now/d
    """
    url = f"{endpoint.rstrip('/')}/{index_pattern}/_count"
    query = {
        "query": {
            "range": {
                timestamp_field: {
                    "gte": f"now-{days}d/d",
                    "lt": "now/d",
                }
            }
        }
    }
    resp = requests.post(
        url, json=query, params={"filter_path": "count"}, auth=auth, headers=headers, timeout=30, verify=False
    )
    resp.raise_for_status()
    return resp.json()["count"]


# --------------------------------------------------------------------------- #
# SSH Tunnel Setup                                                            #
# --------------------------------------------------------------------------- #
def setup_ssh_tunnel(
    ssh_host: str,
    ssh_user: str,
    ssh_pass: Optional[str],
    ssh_key: Optional[str],
    remote_host: str,
    remote_port: int,
    local_port: int = 19200,
) -> SSHTunnelForwarder:
    """
    Create SSH tunnel through jumphost.
    Returns SSHTunnelForwarder object.

    Important: Disables SSH agent and key directories to prevent "Too many authentication
    failures" errors when user has many SSH keys. Uses only the specified auth method.
    """
    if ssh_pass:
        # Password authentication: disable all key-based auth
        tunnel = SSHTunnelForwarder(
            ssh_host,
            ssh_username=ssh_user,
            ssh_password=ssh_pass,
            remote_bind_address=(remote_host, remote_port),
            local_bind_address=("127.0.0.1", local_port),
            allow_agent=False,
            host_pkey_directories=[],
        )
    elif ssh_key:
        # Key authentication: use only the specified key
        tunnel = SSHTunnelForwarder(
            ssh_host,
            ssh_username=ssh_user,
            ssh_pkey=ssh_key,
            remote_bind_address=(remote_host, remote_port),
            local_bind_address=("127.0.0.1", local_port),
            allow_agent=False,
            host_pkey_directories=[],
        )
    else:
        raise ValueError("Either --ssh-pass or --ssh-key must be provided for SSH authentication")

    try:
        tunnel.start()
        return tunnel
    except Exception as e:
        print(f"✗ SSH tunnel failed: {type(e).__name__}: {e}", file=sys.stderr)
        raise


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #
def main() -> None:
    # Load configuration from environment
    es_url = os.getenv('ES_URL')
    es_user = os.getenv('ES_USER')
    es_pass = os.getenv('ES_PASS')
    es_api_key = os.getenv('ES_API_KEY')
    days_str = os.getenv('DAYS_TO_AVERAGE', '7')
    ssh_user = os.getenv('SSH_USER')
    ssh_host = os.getenv('SSH_HOST')
    ssh_pass = os.getenv('SSH_PASS')
    ssh_key = os.getenv('SSH_KEY')
    index_pattern = os.getenv('INDEX_PATTERN')  # Optional: filter to specific indices

    # Normalize empty string to None for index_pattern
    if index_pattern and not index_pattern.strip():
        index_pattern = None

    # Validate and parse days
    try:
        days = int(days_str)
        if days < 1 or days > 365:
            print(f"ERROR: DAYS_TO_AVERAGE must be between 1 and 365 (got: {days})", file=sys.stderr)
            sys.exit(1)
    except ValueError:
        print(f"ERROR: DAYS_TO_AVERAGE must be a valid integer (got: {days_str})", file=sys.stderr)
        sys.exit(1)

    # Validate required Elasticsearch URL
    if not es_url:
        print("ERROR: ES_URL must be set in .env file", file=sys.stderr)
        sys.exit(1)

    # Setup authentication (API key OR basic auth)
    auth_tuple: Optional[Tuple[str, str]] = None
    auth_headers: Optional[dict] = None

    if es_api_key:
        # Use API key authentication
        auth_headers = {'Authorization': f'ApiKey {es_api_key}'}
    elif es_user and es_pass:
        # Use basic authentication
        auth_tuple = (es_user, es_pass)
    else:
        print("ERROR: Must provide either ES_API_KEY or both ES_USER and ES_PASS", file=sys.stderr)
        print("\nCreate a .env file with one of:", file=sys.stderr)
        print("  Method 1 - API Key:", file=sys.stderr)
        print("    ES_URL=https://es.example.com:9200", file=sys.stderr)
        print("    ES_API_KEY=your_base64_encoded_api_key", file=sys.stderr)
        print("\n  Method 2 - Basic Auth:", file=sys.stderr)
        print("    ES_URL=https://es.example.com:9200", file=sys.stderr)
        print("    ES_USER=myuser", file=sys.stderr)
        print("    ES_PASS=mypassword", file=sys.stderr)
        sys.exit(1)

    # Validate SSH configuration if any SSH var is set
    use_ssh = any([ssh_user, ssh_host, ssh_pass, ssh_key])
    if use_ssh:
        if not ssh_user or not ssh_host:
            print("ERROR: SSH_USER and SSH_HOST are required when using SSH jumphost", file=sys.stderr)
            sys.exit(1)
        if not ssh_pass and not ssh_key:
            print("ERROR: Either SSH_PASS or SSH_KEY must be set in .env", file=sys.stderr)
            sys.exit(1)

    tunnel: Optional[SSHTunnelForwarder] = None
    endpoint = es_url

    try:
        # Set up SSH tunnel if needed
        if use_ssh:
            parsed = urlparse(es_url)
            remote_host = parsed.hostname
            remote_port = parsed.port or (443 if parsed.scheme == "https" else 80)
            local_port = 19200

            print(f"\n→ Setting up SSH tunnel through {ssh_host}")
            print(f"  localhost:{local_port} → {remote_host}:{remote_port}")

            tunnel = setup_ssh_tunnel(
                ssh_host=ssh_host,
                ssh_user=ssh_user,
                ssh_pass=ssh_pass,
                ssh_key=ssh_key,
                remote_host=remote_host,
                remote_port=remote_port,
                local_port=local_port,
            )
            print(f"  ✓ SSH tunnel established\n")

            # Modify endpoint to use tunnel
            endpoint = f"{parsed.scheme}://localhost:{local_port}"

        # cluster-wide store + doc metrics (optionally filtered by index pattern)
        total_bytes = get_primary_store_bytes(endpoint, auth_tuple, auth_headers, index_pattern)
        total_docs = get_total_docs(endpoint, auth_tuple, auth_headers, index_pattern)
        avg_bytes_per_doc = total_bytes / total_docs if total_docs else 0

        # past N days metrics (optionally filtered by index pattern)
        docs_last_days = get_docs_last_nd(endpoint, days, auth_tuple, auth_headers, index_pattern=index_pattern or "_all")
        avg_docs_per_day = docs_last_days / days
        avg_bytes_per_day = avg_docs_per_day * avg_bytes_per_doc

        # ────────────────────────────── output ───────────────────────────── #
        print()
        if index_pattern:
            print(f"Index pattern: {index_pattern}")
            print()
        print("1) Overall cluster statistics:")
        print(f"Total primary storage     : {(total_bytes / (1024**3)):,.2f} GiB")
        print(f"Total document count      : {total_docs:,}")
        print(f"Average bytes per doc     : {avg_bytes_per_doc:,.2f} Bytes")
        print()
        print(f"2) Last {days} days statistics:")
        print(f"  Documents ingested      : {docs_last_days:,}")
        print(f"  Avg docs per day        : {avg_docs_per_day:,.2f}")
        print(f"  Avg ingest per day      : {(avg_bytes_per_day / (1024**3)):,.2f} GiB")
        print()

    except requests.exceptions.RequestException as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        sys.exit(2)
    except KeyError:
        print("Unexpected response structure from Elasticsearch.", file=sys.stderr)
        sys.exit(3)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(4)
    finally:
        # Clean up SSH tunnel
        if tunnel:
            tunnel.stop()
            print("→ SSH tunnel closed")


if __name__ == "__main__":
    main()
