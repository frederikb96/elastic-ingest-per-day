#!/usr/bin/env python3
"""
Compute average primary-store bytes per document for an Elasticsearch cluster.

Usage:
    python elastic_ingest_per_day.py https://es.example.com:9200 myuser mypassword
"""

import argparse
import re
import sys
from typing import Tuple

import requests


def get_primary_store_bytes(endpoint: str, auth: Tuple[str, str]) -> int:
    """Sum the store size of all primary shards (bytes)."""
    url = f"{endpoint.rstrip('/')}/_cat/shards"
    params = {
        "bytes": "b",  # return raw bytes, no units
        "format": "json",  # JSON so we can parse reliably
        "h": "prirep,store",  # only the columns we need
    }
    resp = requests.get(url, params=params, auth=auth, timeout=20, verify=False)
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


def get_total_docs(endpoint: str, auth: Tuple[str, str]) -> int:
    """Return total document count from all primary shards."""
    url = f"{endpoint.rstrip('/')}/_stats"
    params = {"filter_path": "_all.primaries.docs.count"}
    resp = requests.get(url, params=params, auth=auth, timeout=20, verify=False)
    resp.raise_for_status()
    return resp.json()["_all"]["primaries"]["docs"]["count"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Calculate average bytes per document in an Elasticsearch cluster"
    )
    parser.add_argument("endpoint", help="Cluster endpoint, e.g. https://es.example.com:9200")
    parser.add_argument("username", help="Basic-auth username")
    parser.add_argument("password", help="Basic-auth password")
    args = parser.parse_args()

    auth = (args.username, args.password)

    try:
        total_bytes = get_primary_store_bytes(args.endpoint, auth)
        total_docs = get_total_docs(args.endpoint, auth)

        if total_docs == 0:
            print("Total document count is zero—cannot compute average.")
            sys.exit(1)

        avg_bytes = total_bytes / total_docs
        print(f"Primary-store size : {total_bytes:,} bytes")
        print(f"Document count     : {total_docs:,}")
        print(f"Average bytes/doc  : {avg_bytes:,.2f}")
    except requests.exceptions.RequestException as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        sys.exit(2)
    except KeyError:
        print("Unexpected response structure from Elasticsearch.", file=sys.stderr)
        sys.exit(3)


if __name__ == "__main__":
    main()
