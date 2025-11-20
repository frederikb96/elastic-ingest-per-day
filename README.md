# Elastic Ingest per Day

A Python script to measure **daily ingest volume in Elasticsearch** using two approaches:

1. **Disk-based estimation** - Uses primary shard storage and document counts (fast, but affected by compression)
2. **Network-level tracking** (optional) - Uses ingest pipeline byte counters for accurate network traffic measurement

## Measurement Approaches

### Disk-Based Estimation (Default)

Queries:
- Total **primary store size** (in bytes)
- Total **document count**
- Documents ingested over **configurable time window** (default: 7 days)

Computes:
- Average document size (bytes/doc)
- Average number of documents per day
- Average ingest volume per day in GiB

**Limitation**: Disk storage is affected by compression (typically 36-99% reduction), deleted documents, and segment merging, making it an approximation of actual network ingested bytes.

### Ingest Pipeline Byte Tracking (Optional)

Tracks actual bytes flowing through Elasticsearch ingest pipelines:
- Queries `/_nodes/stats/ingest` for cumulative byte counters per node
- Stores snapshots in `ingest-tracking-snapshots` index
- Calculates daily ingest rate by linear extrapolation between snapshots
- Provides per-node breakdown and cluster total

**Requirements**:
- All data must flow through ingest pipelines (configure `pipeline` parameter in Beats, Logstash, or bulk API)
- Requires at least two snapshots for calculation (first run stores initial snapshot)
- Handles node restarts gracefully (detects counter resets)

## 🛠️ Usage

### Configuration

Create a `.env` file with your Elasticsearch configuration:

```bash
# Elasticsearch URL (required)
ES_URL=https://es.example.com:9200

# Authentication (choose one method)
# Method 1: Basic authentication
ES_USER=myuser
ES_PASS=mypassword

# Method 2: API key authentication
ES_API_KEY=your_base64_encoded_api_key

# Time window for averaging (optional, default: 7)
DAYS_TO_AVERAGE=7  # Valid range: 1-365
```

For clusters behind an SSH jumphost, add SSH configuration:

```bash
# SSH Jumphost (optional)
SSH_USER=sshuser
SSH_HOST=jumphost.example.com
SSH_PASS=sshpassword

# OR use SSH key instead of password
SSH_KEY=/path/to/private/key
```

To analyze only specific indices matching a pattern:

```bash
# Index Pattern Filter (optional)
INDEX_PATTERN=logs-endpoint*
# If not set or empty, analyzes all indices in the cluster
```

To enable **ingest pipeline byte tracking** (network-level measurement):

```bash
# Enable ingest pipeline byte tracking (optional)
TRACK_INGEST_BYTES=true
# Stores snapshots in 'ingest-tracking-snapshots' index
# Requires data flowing through ingest pipelines

# Lookback period for historical comparison (optional, default: 0)
INGEST_LOOKBACK_DAYS=7
# 0 = use most recent snapshot
# N = use snapshot closest to N days ago (valid range: 0-365)
```

**Important**: Ingest tracking requires at least **two snapshots** to calculate rates:
- **First run**: Stores initial snapshot, displays "No valid measurements" message
- **Second run** (and onwards): Calculates daily ingest rate by comparing snapshots

### Run the Script

```bash
python3 elastic_ingest_per_day.py
```

The script will automatically:
- Load configuration from `.env` file
- Establish SSH tunnel if SSH variables are set
- Query Elasticsearch for statistics
- Calculate and display daily ingest metrics

**Note:** Self-signed SSL certificates are supported — certificate validation is disabled by default.

## 📦 Requirements

* `requests` - HTTP client for Elasticsearch API
* `paramiko` - SSH protocol library
* `sshtunnel` - SSH tunneling support for jumphost connections
* `python-dotenv` - .env file loading

Install via:

```bash
pip3 install -r requirements.txt
```

Or using a virtual environment (recommended):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

## 📋 Example Output

### Disk-Based Statistics Only (Default)

```
1) Overall cluster statistics:
Total primary storage     : 82.25 GiB
Total document count      : 186,617,606
Average bytes per doc     : 473.25 Bytes

2) Last 7 days statistics:
  Documents ingested      : 122,196,371
  Avg docs per day        : 17,456,624.43
  Avg ingest per day      : 7.69 GiB
```
