# Elastic Ingest per Day

A simple Python script to estimate **daily ingest volume in Elasticsearch**, based on total cluster storage and document counts.

It queries:

* the total **primary store size** (in bytes),
* the **total document count**,
* and how many documents were ingested over the **last 7 full days**.

It then computes:

* the average document size (bytes/doc),
* the average number of documents per day,
* and the average ingest volume per day in GiB.

## 🛠️ Usage

Example:

```
URL=https://es.example.com:9200
USER=myuser
PASS=mypassword

python elastic_ingest_per_day.py $URL $USER $PASS
```

**Note:** Self-signed SSL certificates are supported — certificate validation is disabled by default.

## 📦 Requirements

* One dependency: `requests`

Install via:

```
pip install -r requirements.txt
```

## 📋 Example Output

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
