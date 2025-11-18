# Elastic Ingest per Day

A simple Python script to estimate **daily ingest volume in Elasticsearch**, based on total cluster storage and document counts.

It queries:

- the total **primary store size** (in bytes),
- the **total document count**,
- and how many documents were ingested over a **configurable time window** (default: 7 days).

It then computes:

- the average document size (bytes/doc),
- the average number of documents per day,
- and the average ingest volume per day in GiB.

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

With default configuration (7 days):

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

With `DAYS_TO_AVERAGE=30`:

```
2) Last 30 days statistics:
  Documents ingested      : 523,698,450
  Avg docs per day        : 17,456,615.00
  Avg ingest per day      : 7.69 GiB
```
