#!/usr/bin/env python3
"""
Usage:
    URL=https://es.example.com:9200
    USER=myuser
    PASS=mypassword
    python elastic_ingest_per_day.py $URL $USER $PASS
"""

import argparse
import re
import sys
from typing import Tuple

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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


def get_docs_last_7d(
    endpoint: str,
    auth: Tuple[str, str],
    timestamp_field: str = "@timestamp",
    index_pattern: str = "_all",
) -> int:
    """
    Count documents with `timestamp_field` in the range:
        (now-7d, now) – *rounded to day boundaries*
    Equivalent Kibana KQL:  @timestamp >= now-7d/d and @timestamp <  now/d
    """
    url = f"{endpoint.rstrip('/')}/{index_pattern}/_count"
    query = {
        "query": {
            "range": {
                timestamp_field: {
                    "gte": "now-7d/d",
                    "lt": "now/d",
                }
            }
        }
    }
    resp = requests.post(
        url, json=query, params={"filter_path": "count"}, auth=auth, timeout=30, verify=False
    )
    resp.raise_for_status()
    return resp.json()["count"]


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Elasticsearch ingest statistics: bytes, docs, averages"
    )
    parser.add_argument("endpoint", help="Elasticsearch endpoint, e.g. https://es.example.com:9200")
    parser.add_argument("username", help="Basic-auth username")
    parser.add_argument("password", help="Basic-auth password")
    args = parser.parse_args()
    auth = (args.username, args.password)

    try:
        # cluster-wide store + doc metrics
        total_bytes = get_primary_store_bytes(args.endpoint, auth)
        total_docs = get_total_docs(args.endpoint, auth)
        avg_bytes_per_doc = total_bytes / total_docs if total_docs else 0

        # past 7 days metrics
        docs_7d = get_docs_last_7d(args.endpoint, auth)
        avg_docs_per_day_7d = docs_7d / 7
        avg_bytes_per_day_7d = avg_docs_per_day_7d * avg_bytes_per_doc

        # ────────────────────────────── output ───────────────────────────── #
        print()
        print("1) Overall cluster statistics:")
        print(f"Total primary storage     : {(total_bytes / (1024**3)):,.2f} GiB")
        print(f"Total document count      : {total_docs:,}")
        print(f"Average bytes per doc     : {avg_bytes_per_doc:,.2f} Bytes")
        print()
        print("2) Last 7 days statistics:")
        print(f"  Documents ingested      : {docs_7d:,}")
        print(f"  Avg docs per day        : {avg_docs_per_day_7d:,.2f}")
        print(f"  Avg ingest per day      : {(avg_bytes_per_day_7d / (1024**3)):,.2f} GiB")
        print()

    except requests.exceptions.RequestException as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        sys.exit(2)
    except KeyError:
        print("Unexpected response structure from Elasticsearch.", file=sys.stderr)
        sys.exit(3)


if __name__ == "__main__":
    main()
