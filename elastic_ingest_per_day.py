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
    # Uses exact 24-hour multiples from current time (not day boundaries)

    # Optional - SSH jumphost configuration
    SSH_USER=sshuser
    SSH_HOST=jumphost.example.com
    SSH_PASS=sshpassword
    # OR
    SSH_KEY=/path/to/key

    # Optional - Index pattern filter (disk-based measurement)
    INDEX_PATTERN=logs-endpoint*
    # If not set or empty, analyzes all indices

    # Optional - Ingest pipeline byte tracking (network-level measurement)
    TRACK_INGEST_BYTES=true
    # Requires data flowing through ingest pipelines
    # Stores snapshots in 'ingest-tracking-snapshots' index
    # NOTE: First run only stores snapshot, second run calculates rate

    INGEST_LOOKBACK_DAYS=7
    # When set: uses snapshot closest to N days ago for calculation
    # When 0 or not set: uses most recent snapshot (default)
    # Valid range: 0-365

    # Optional - Pipeline pattern filter (network-level measurement only)
    PIPELINE_PATTERN=logs-*
    # If not set or empty, includes all pipelines
    # Supports wildcards: logs-* matches logs-endpoint, logs-network, etc.

    # Optional - Show per-index breakdown in output
    SHOW_PER_INDEX_BREAKDOWN=true
    # If set to true, displays detailed statistics for each index
    # Default: false (shows only aggregated totals)

Usage:
    python3 elastic_ingest_per_day.py
"""

import fnmatch
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, List, Any
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
        [now - (days * 24h), now]
    Uses exact 24-hour multiples from current time (no day boundary rounding).
    """
    url = f"{endpoint.rstrip('/')}/{index_pattern}/_count"
    query = {
        "query": {
            "range": {
                timestamp_field: {
                    "gte": f"now-{days}d",
                    "lte": "now",
                }
            }
        }
    }
    resp = requests.post(
        url, json=query, params={"filter_path": "count"}, auth=auth, headers=headers, timeout=30, verify=False
    )
    resp.raise_for_status()
    return resp.json()["count"]


def get_per_index_stats(
    endpoint: str,
    auth: Optional[Tuple[str, str]] = None,
    headers: Optional[dict] = None,
    index_pattern: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Get per-index statistics: storage bytes, doc count, and avg bytes per doc.

    Returns:
        Dict mapping index_name -> {
            'bytes': int,
            'docs': int,
            'avg_bytes_per_doc': float
        }
    """
    # Build URL with optional index pattern
    if index_pattern:
        url = f"{endpoint.rstrip('/')}/{index_pattern}/_stats/store,docs"
    else:
        url = f"{endpoint.rstrip('/')}/_stats/store,docs"

    params = {"filter_path": "indices.*.primaries"}
    resp = requests.get(url, params=params, auth=auth, headers=headers, timeout=30, verify=False)
    resp.raise_for_status()

    result = {}
    indices_data = resp.json().get('indices', {})

    for index_name, index_stats in indices_data.items():
        primaries = index_stats.get('primaries', {})
        store_bytes = primaries.get('store', {}).get('size_in_bytes', 0)
        doc_count = primaries.get('docs', {}).get('count', 0)

        # Skip indices with no documents
        if doc_count == 0:
            continue

        avg_bytes_per_doc = store_bytes / doc_count if doc_count > 0 else 0

        result[index_name] = {
            'bytes': store_bytes,
            'docs': doc_count,
            'avg_bytes_per_doc': avg_bytes_per_doc
        }

    return result


def get_per_index_recent_docs(
    endpoint: str,
    days: int,
    index_names: List[str],
    auth: Optional[Tuple[str, str]] = None,
    headers: Optional[dict] = None,
    timestamp_field: str = "@timestamp",
    index_pattern: Optional[str] = None,
) -> Dict[str, int]:
    """
    Get recent document counts for each index using aggregation.

    Uses a single query with terms aggregation to get counts for all indices
    efficiently instead of querying each index individually.

    Returns:
        Dict mapping index_name -> recent_doc_count
    """
    if not index_names:
        return {}

    # Use pattern or _all to avoid URL length limits
    if index_pattern:
        url = f"{endpoint.rstrip('/')}/{index_pattern}/_search"
    else:
        url = f"{endpoint.rstrip('/')}/_all/_search"

    # Use terms aggregation on _index field to get per-index counts
    # Uses exact 24-hour multiples from current time (no day boundary rounding)
    query = {
        "size": 0,  # We don't need documents, just aggregation
        "query": {
            "range": {
                timestamp_field: {
                    "gte": f"now-{days}d",
                    "lte": "now",
                }
            }
        },
        "aggs": {
            "per_index": {
                "terms": {
                    "field": "_index",
                    "size": 10000  # Get up to 10k indices
                }
            }
        }
    }

    try:
        resp = requests.post(
            url, json=query,
            auth=auth, headers=headers, timeout=60, verify=False
        )
        resp.raise_for_status()

        result = {name: 0 for name in index_names}  # Initialize with 0s

        resp_json = resp.json()

        # Parse aggregation results
        agg_data = resp_json.get('aggregations', {}).get('per_index', {}).get('buckets', [])

        for bucket in agg_data:
            index_name = bucket['key']
            doc_count = bucket['doc_count']
            # Only store results for indices we care about
            if index_name in result:
                result[index_name] = doc_count

        return result

    except Exception as e:
        # If query fails entirely, return 0 for all indices
        print(f"ERROR: Aggregation query failed: {e}", file=sys.stderr)
        return {name: 0 for name in index_names}


# --------------------------------------------------------------------------- #
# Ingest Pipeline Byte Tracking                                               #
# --------------------------------------------------------------------------- #
INDEX_NAME = "ingest-tracking-snapshots"


def ensure_ingest_tracking_index(
    endpoint: str,
    auth: Optional[Tuple[str, str]] = None,
    headers: Optional[dict] = None,
) -> None:
    """Create ingest tracking index if it doesn't exist."""
    url = f"{endpoint.rstrip('/')}/{INDEX_NAME}"

    # Check if index exists
    resp = requests.head(url, auth=auth, headers=headers, verify=False, timeout=10)
    if resp.status_code == 200:
        return  # Index exists

    # Create index with mappings
    mappings = {
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "node_id": {"type": "keyword"},
                "node_name": {"type": "keyword"},
                "ingest_bytes": {"type": "long"},
                "pipeline_pattern": {"type": "keyword"}
            }
        }
    }

    resp = requests.put(url, json=mappings, auth=auth, headers=headers, verify=False, timeout=10)
    resp.raise_for_status()


def get_current_ingest_stats(
    endpoint: str,
    auth: Optional[Tuple[str, str]] = None,
    headers: Optional[dict] = None,
    pipeline_pattern: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Query current ingest byte counters from all nodes.

    Sums ingested_as_first_pipeline_in_bytes across matching pipelines per node.
    This represents bytes received by Elasticsearch before any processing.

    Args:
        endpoint: Elasticsearch endpoint URL
        auth: Optional basic auth tuple (user, pass)
        headers: Optional headers dict for API key auth
        pipeline_pattern: Optional wildcard pattern to filter pipelines (e.g., 'logs-*')
    """
    url = f"{endpoint.rstrip('/')}/_nodes/stats/ingest"
    resp = requests.get(url, auth=auth, headers=headers, verify=False, timeout=20)
    resp.raise_for_status()

    current_time = datetime.now(timezone.utc)
    stats = {}

    response_data = resp.json()

    if 'nodes' not in response_data:
        return stats

    for node_id, node_data in response_data['nodes'].items():
        pipelines = node_data.get('ingest', {}).get('pipelines', {})

        if not pipelines:
            continue

        # Sum ingested bytes across matching pipelines
        total_ingested_bytes = 0
        for pipeline_name, pipeline_data in pipelines.items():
            # Filter by pattern if specified
            if pipeline_pattern and not fnmatch.fnmatch(pipeline_name, pipeline_pattern):
                continue

            ingested_bytes = pipeline_data.get('ingested_as_first_pipeline_in_bytes', 0)
            total_ingested_bytes += ingested_bytes

        if total_ingested_bytes == 0:
            continue

        stats[node_id] = {
            "name": node_data['name'],
            "bytes": total_ingested_bytes,
            "timestamp": current_time
        }

    return stats


def store_ingest_snapshots(
    endpoint: str,
    auth: Optional[Tuple[str, str]] = None,
    headers: Optional[dict] = None,
    snapshots: Dict[str, Dict[str, Any]] = None,
    pipeline_pattern: Optional[str] = None,
) -> None:
    """Store current snapshots in tracking index."""
    if not snapshots:
        return

    # Use refresh=wait_for to make documents immediately searchable
    url = f"{endpoint.rstrip('/')}/{INDEX_NAME}/_bulk?refresh=wait_for"

    # Build bulk request
    bulk_lines = []
    for node_id, data in snapshots.items():
        action = {"index": {}}
        doc = {
            "timestamp": data['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
            "node_id": node_id,
            "node_name": data['name'],
            "ingest_bytes": data['bytes'],
            "pipeline_pattern": pipeline_pattern or "_all"
        }
        bulk_lines.append(requests.compat.json.dumps(action))
        bulk_lines.append(requests.compat.json.dumps(doc))

    bulk_body = "\n".join(bulk_lines) + "\n"

    resp = requests.post(
        url,
        data=bulk_body,
        headers={**({'Content-Type': 'application/x-ndjson'}), **(headers or {})},
        auth=auth,
        verify=False,
        timeout=20
    )

    # Check for errors in bulk response
    bulk_result = resp.json()
    if bulk_result.get('errors'):
        raise Exception(f"Bulk indexing failed: {bulk_result['items'][0]['index']['error']['reason']}")

    resp.raise_for_status()


def get_historical_snapshots(
    endpoint: str,
    auth: Optional[Tuple[str, str]] = None,
    headers: Optional[dict] = None,
    lookback_days: int = 0,
    pipeline_pattern: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Retrieve historical snapshots for comparison."""
    url = f"{endpoint.rstrip('/')}/{INDEX_NAME}/_search"

    # Query snapshots matching the same pipeline pattern
    # Use .keyword subfield for exact matching on pipeline_pattern
    pattern_value = pipeline_pattern or "_all"
    query = {
        "size": 10000,
        "sort": [{"timestamp": "desc"}],
        "query": {
            "term": {
                "pipeline_pattern.keyword": pattern_value
            }
        }
    }

    resp = requests.post(url, json=query, auth=auth, headers=headers, verify=False, timeout=20)
    resp.raise_for_status()

    hits = resp.json().get('hits', {}).get('hits', [])

    if not hits:
        return {}

    # Group snapshots by node
    snapshots_by_node: Dict[str, List[Dict]] = {}
    for hit in hits:
        source = hit['_source']
        node_id = source['node_id']
        timestamp = datetime.fromisoformat(source['timestamp'].replace('Z', '+00:00'))

        if node_id not in snapshots_by_node:
            snapshots_by_node[node_id] = []

        snapshots_by_node[node_id].append({
            "timestamp": timestamp,
            "bytes": source['ingest_bytes'],
            "name": source.get('node_name', node_id)
        })

    # Find appropriate snapshot per node
    result = {}
    now = datetime.now(timezone.utc)

    for node_id, snapshots in snapshots_by_node.items():
        # Sort by timestamp descending (newest first)
        snapshots.sort(key=lambda x: x['timestamp'], reverse=True)

        if lookback_days == 0:
            # Use most recent snapshot available
            if len(snapshots) > 0:
                chosen = snapshots[0]  # Most recent (already sorted descending)
            else:
                continue
        else:
            # Find snapshot closest to N days ago
            target_time = now - timedelta(days=lookback_days)

            min_diff = None
            chosen = None
            for snap in snapshots:
                diff = abs((snap['timestamp'] - target_time).total_seconds())
                if min_diff is None or diff < min_diff:
                    min_diff = diff
                    chosen = snap

            if chosen is None:
                continue

        result[node_id] = {
            "bytes": chosen['bytes'],
            "timestamp": chosen['timestamp'],
            "name": chosen['name']
        }

    return result


def calculate_daily_ingest_rates(
    current: Dict[str, Dict[str, Any]],
    historical: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """Calculate per-node daily ingest rates from snapshots."""
    results = {}

    for node_id, curr_data in current.items():
        if node_id not in historical:
            results[node_id] = {
                "name": curr_data['name'],
                "status": "no_historical_data",
                "daily_gb": 0.0,
                "period_hours": 0.0
            }
            continue

        hist_data = historical[node_id]

        delta_bytes = curr_data['bytes'] - hist_data['bytes']
        delta_seconds = (curr_data['timestamp'] - hist_data['timestamp']).total_seconds()

        # Handle node restart (counter reset)
        if delta_bytes < 0:
            results[node_id] = {
                "name": curr_data['name'],
                "status": "counter_reset",
                "daily_gb": 0.0,
                "period_hours": 0.0
            }
            continue

        # Handle zero time delta (shouldn't happen but defensive)
        if delta_seconds <= 0:
            results[node_id] = {
                "name": curr_data['name'],
                "status": "invalid_time_delta",
                "daily_gb": 0.0,
                "period_hours": 0.0
            }
            continue

        # Calculate daily rate by linear extrapolation
        bytes_per_second = delta_bytes / delta_seconds
        bytes_per_day = bytes_per_second * 86400
        daily_gb = bytes_per_day / (1024**3)

        results[node_id] = {
            "name": curr_data['name'],
            "status": "ok",
            "daily_gb": daily_gb,
            "period_hours": delta_seconds / 3600,
            "delta_bytes": delta_bytes
        }

    return results


def display_ingest_tracking_results(
    node_stats: Dict[str, Dict[str, Any]],
    pipeline_pattern: Optional[str] = None
) -> None:
    """Print formatted ingest tracking results."""
    print("3) Ingest pipeline byte tracking (network-level):")
    print()

    if pipeline_pattern:
        print(f"  Pipeline pattern: {pipeline_pattern}")
        print()

    # Filter successful nodes
    valid_nodes = {nid: data for nid, data in node_stats.items() if data['status'] == 'ok'}

    if not valid_nodes:
        print("  ⚠ No valid measurements available")

        # Show why each node failed
        for node_id, data in node_stats.items():
            if data['status'] == 'no_historical_data':
                print(f"  • {data['name']}: No historical snapshot (first run?)")
            elif data['status'] == 'counter_reset':
                print(f"  • {data['name']}: Node restarted (counter reset detected)")
            elif data['status'] == 'invalid_time_delta':
                print(f"  • {data['name']}: Invalid time delta")

        print()
        print("  ℹ Run script again later after snapshots have been collected.")
        print()
        return

    # Per-node results
    print("  Per-node results:")
    for node_id, data in valid_nodes.items():
        # Format period display
        period_hours = data['period_hours']
        if period_hours < 1:
            period_str = f"{period_hours * 60:.1f} minutes"
        else:
            period_str = f"{period_hours:.1f} hours"

        print(f"    • {data['name']}")
        print(f"        Measurement period:  {period_str}")
        print(f"        Bytes in period:     {data['delta_bytes'] / (1024**3):.2f} GiB")
        print(f"        Extrapolated daily:  {data['daily_gb']:.2f} GiB/day")

    print()

    # Average per node
    avg_daily_gb = sum(d['daily_gb'] for d in valid_nodes.values()) / len(valid_nodes)
    print(f"  Average per node:        {avg_daily_gb:.2f} GiB/day")

    # Total cluster
    total_daily_gb = sum(d['daily_gb'] for d in valid_nodes.values())
    print(f"  Total cluster ingest:    {total_daily_gb:.2f} GiB/day")
    print()

    # Warnings for failed nodes
    failed_nodes = {nid: data for nid, data in node_stats.items() if data['status'] != 'ok'}
    if failed_nodes:
        print("  ⚠ Warnings:")
        for node_id, data in failed_nodes.items():
            if data['status'] == 'no_historical_data':
                print(f"    • {data['name']}: No historical data available")
            elif data['status'] == 'counter_reset':
                print(f"    • {data['name']}: Counter reset detected (node restart)")
        print()


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

    # Ingest pipeline byte tracking configuration
    track_ingest = os.getenv('TRACK_INGEST_BYTES', '').lower() in ('true', '1', 'yes')
    ingest_lookback_str = os.getenv('INGEST_LOOKBACK_DAYS', '0')
    pipeline_pattern = os.getenv('PIPELINE_PATTERN')  # Optional: filter to specific pipelines

    # Display configuration
    show_per_index = os.getenv('SHOW_PER_INDEX_BREAKDOWN', '').lower() in ('true', '1', 'yes')

    # Normalize empty strings to None
    if index_pattern and not index_pattern.strip():
        index_pattern = None
    if pipeline_pattern and not pipeline_pattern.strip():
        pipeline_pattern = None

    # Validate and parse days
    try:
        days = int(days_str)
        if days < 1 or days > 365:
            print(f"ERROR: DAYS_TO_AVERAGE must be between 1 and 365 (got: {days})", file=sys.stderr)
            sys.exit(1)
    except ValueError:
        print(f"ERROR: DAYS_TO_AVERAGE must be a valid integer (got: {days_str})", file=sys.stderr)
        sys.exit(1)

    # Validate ingest lookback days
    if track_ingest:
        try:
            ingest_lookback_days = int(ingest_lookback_str)
            if ingest_lookback_days < 0 or ingest_lookback_days > 365:
                print(f"ERROR: INGEST_LOOKBACK_DAYS must be between 0 and 365 (got: {ingest_lookback_days})", file=sys.stderr)
                sys.exit(1)
        except ValueError:
            print(f"ERROR: INGEST_LOOKBACK_DAYS must be a valid integer (got: {ingest_lookback_str})", file=sys.stderr)
            sys.exit(1)
    else:
        ingest_lookback_days = 0

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

        # Per-index statistics (storage, docs, avg bytes per doc)
        per_index_stats = get_per_index_stats(endpoint, auth_tuple, auth_headers, index_pattern)

        # Per-index recent document counts
        index_names = list(per_index_stats.keys())
        per_index_recent_docs = get_per_index_recent_docs(
            endpoint, days, index_names, auth_tuple, auth_headers,
            index_pattern=index_pattern
        )

        # Calculate per-index daily ingest volumes
        per_index_daily_bytes = {}
        for index_name in index_names:
            recent_docs = per_index_recent_docs.get(index_name, 0)
            avg_bytes = per_index_stats[index_name]['avg_bytes_per_doc']
            daily_docs = recent_docs / days
            daily_bytes = daily_docs * avg_bytes
            per_index_daily_bytes[index_name] = {
                'recent_docs': recent_docs,
                'daily_docs': daily_docs,
                'daily_bytes': daily_bytes
            }

        # Aggregate totals for display
        total_bytes = sum(stats['bytes'] for stats in per_index_stats.values())
        total_docs = sum(stats['docs'] for stats in per_index_stats.values())
        avg_bytes_per_doc = total_bytes / total_docs if total_docs else 0

        docs_last_days = sum(per_index_recent_docs.values())
        avg_docs_per_day = docs_last_days / days
        avg_bytes_per_day = sum(data['daily_bytes'] for data in per_index_daily_bytes.values())

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

        # ─────────────── per-index breakdown (optional) ──────────────────────── #
        if show_per_index:
            print("Per-index breakdown:")
            print()

            # Sort indices by daily bytes (descending)
            sorted_indices = sorted(
                per_index_daily_bytes.items(),
                key=lambda x: x[1]['daily_bytes'],
                reverse=True
            )

            for index_name, daily_data in sorted_indices:
                index_stats = per_index_stats[index_name]
                print(f"  {index_name}")
                print(f"    Total storage           : {(index_stats['bytes'] / (1024**3)):,.2f} GiB")
                print(f"    Total documents         : {index_stats['docs']:,}")
                print(f"    Avg bytes per doc       : {index_stats['avg_bytes_per_doc']:,.2f} Bytes")
                print(f"    Recent docs ({days}d)      : {daily_data['recent_docs']:,}")
                print(f"    Avg docs per day        : {daily_data['daily_docs']:,.2f}")
                print(f"    Avg ingest per day      : {(daily_data['daily_bytes'] / (1024**3)):,.2f} GiB")
                print()

        # ─────────────────── ingest tracking (optional) ──────────────────────── #
        if track_ingest:
            try:
                # Ensure tracking index exists
                ensure_ingest_tracking_index(endpoint, auth_tuple, auth_headers)

                # Get current ingest stats
                current_stats = get_current_ingest_stats(
                    endpoint, auth_tuple, auth_headers, pipeline_pattern
                )

                # Check if any nodes have ingest data
                if not current_stats:
                    print("3) Ingest pipeline byte tracking (network-level):")
                    print()
                    if pipeline_pattern:
                        print(f"  Pipeline pattern: {pipeline_pattern}")
                        print()
                    print("  ⚠ No ingest data available")
                    print("  ℹ Either no matching pipelines exist, or no data has been processed yet.")
                    print("  ℹ Ingest counters start incrementing once data flows through pipelines.")
                    print()
                    raise ValueError("No ingest byte data available")

                # Get historical snapshots for comparison BEFORE storing current
                historical_stats = get_historical_snapshots(
                    endpoint, auth_tuple, auth_headers, ingest_lookback_days, pipeline_pattern
                )

                # Store current snapshot AFTER querying historical
                store_ingest_snapshots(endpoint, auth_tuple, auth_headers, current_stats, pipeline_pattern)

                # Calculate daily rates
                node_rates = calculate_daily_ingest_rates(current_stats, historical_stats)

                # Display results
                display_ingest_tracking_results(node_rates, pipeline_pattern)

            except ValueError as val_exc:
                # Already printed user-friendly message, just skip
                pass
            except Exception as ingest_exc:
                print(f"⚠ Ingest tracking failed: {ingest_exc}", file=sys.stderr)
                print("  (Continuing with disk-based statistics only)", file=sys.stderr)
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
