<table>
<tr>
<td width="140">
<img src="docs/logo.png" alt="elastic-ingest-per-day" width="120">
</td>
<td>

# Elastic Ingest per Day

[![CI](https://github.com/frederikb96/elastic-ingest-per-day/actions/workflows/ci.yaml/badge.svg)](https://github.com/frederikb96/elastic-ingest-per-day/actions/workflows/ci.yaml)
[![Release](https://img.shields.io/github/v/release/frederikb96/elastic-ingest-per-day)](https://github.com/frederikb96/elastic-ingest-per-day/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

</td>
</tr>
</table>

Estimates daily ingest volume for Elasticsearch clusters. Uses the same formula as Elastic's **Streams UI** (GA in 9.2) — verified against the [Kibana source](https://github.com/elastic/kibana/blob/main/x-pack/platform/plugins/shared/streams_app/public/components/data_management/stream_detail_lifecycle/helpers/get_calculated_stats.ts).

## How It Works

The core formula is simple:

```
avg_doc_size  = index_storage_bytes / total_doc_count     (per index)
docs_per_day  = docs_in_time_window / days_in_window      (per index)
ingest/day    = avg_doc_size × docs_per_day               (per index, then summed)
```

- Queries **primary shard storage size** and **document count** per index via the `_stats` API
- Counts recent documents per index using a time-range aggregation (default window: 7 days)
- Computes per-index, then sums — so indices with 1KB docs and indices with 50KB docs each use their own average, not a cluster-wide one

### Compared to Elastic's Streams UI

The Streams UI uses the same `bytesPerDoc × docsPerDay` formula. This script differs in two ways that make it slightly more granular:

- **Per-index granularity** — Elastic computes per data stream. This script computes per backing index, which is more accurate when document sizes vary across backing indices of the same stream.
- **Time window flexibility** — The script allows any time window (e.g. 3 days, 12 hours).
- **Wildcard index selection** — The script can analyze a subset of indices matching a pattern and return a total for just those indices as well as a total overall.

### What This Measures (and What It Doesn't)

**This measures:** On-disk ingest volume per day (primary shards, excluding replicas). This is what matters for capacity planning — how much storage your data actually consumes.

**This does NOT measure:** Raw network ingest. Data goes through pipelines, field mappings, and compression before hitting disk, so the on-disk number is typically much smaller than what arrives over the wire. Raw network ingest can only be reliably measured at the network layer (firewall, load balancer, etc.) — Elasticsearch itself has no accurate way to report it.

**Accuracy edge case:** Since it uses an *average* document size, a sudden pipeline change that dramatically alters document size mid-window can skew results. In practice this rarely matters.

## Quick Start

Create a `.env` file (see configuration below), then pick one of:

### Option A: uv (recommended)

[uv](https://docs.astral.sh/uv/) is a single binary that replaces pip, venv, pyenv, and pipx. It reads the script's inline dependency metadata, creates an isolated environment behind the scenes, and runs — no manual setup.

**Run directly from a release** — no clone needed:

```bash
uv run https://raw.githubusercontent.com/frederikb96/elastic-ingest-per-day/v1.2.0/elastic_ingest_per_day.py
```

**Or locally:**

```bash
uv run elastic_ingest_per_day.py
```

### Option B: pip + venv

```bash
python3 -m venv .venv && source .venv/bin/activate
pip3 install -r requirements.txt
python3 elastic_ingest_per_day.py
```

## Configuration

All settings go in a `.env` file. Only `ES_URL` and one auth method are required.

```bash
# Elasticsearch URL (required)
ES_URL=https://es.example.com:9200

# Authentication — pick one method
ES_USER=myuser
ES_PASS=mypassword
# OR
ES_API_KEY=your_base64_encoded_api_key

# Time window for averaging (optional, default: 7d)
# Supports: Nd (days), Nh (hours), Nm (minutes) — exact time from now, not day boundaries
TIME_WINDOW=7d

# Only analyze matching indices (optional, default: all)
INDEX_PATTERN=logs-endpoint*

# Show per-index breakdown in output (optional, default: false)
SHOW_PER_INDEX_BREAKDOWN=true

# SSH jumphost — if your cluster isn't directly reachable
SSH_USER=sshuser
SSH_HOST=jumphost.example.com
SSH_PASS=sshpassword
# OR use key instead of password
SSH_KEY=/path/to/private/key
```

## Example Output

```
1) Overall cluster statistics:
Total primary storage     : 82.25 GiB
Total document count      : 186,617,606
Average bytes per doc     : 473.25 Bytes

2) Time window statistics (7d):
  Documents ingested      : 122,196,371
  Avg docs per day        : 17,456,624.43
  Avg ingest per day      : 7.69 GiB
```

## Requirements

- **uv path:** Dependencies are declared inline in the script (PEP 723) — `uv run` handles everything automatically
- **pip path:** See `requirements.txt`
- Self-signed SSL certificates are supported (certificate validation is disabled)

---

## Experimental: Ingest Pipeline Byte Tracking

**Limited usefulness.** This mode attempts to measure network-level ingest via pipeline byte counters (`ingested_as_first_pipeline_in_bytes`). However, because documents often pass through multiple chained pipelines, the counters can produce inflated or unreliable numbers. For accurate raw network ingest, measure at the network layer instead.

```bash
TRACK_INGEST_BYTES=true

# Compare against snapshot from N days ago (optional, default: 0 = most recent)
INGEST_LOOKBACK_DAYS=7

# Only count matching pipelines (optional, supports wildcards)
PIPELINE_PATTERN=logs-*
```

Requires at least two runs — the first run stores a baseline snapshot, subsequent runs calculate rates by comparing against historical snapshots.
