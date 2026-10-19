# tfl-streaming-pipeline

[![CI](https://github.com/Muhammad234871/tfl-streaming-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Muhammad234871/tfl-streaming-pipeline/actions/workflows/ci.yml)

A real-time data pipeline for London transport. It streams live train arrival predictions and
line disruptions for the Tube, DLR, Elizabeth line and Overground from the
[TfL Unified API](https://api.tfl.gov.uk) through **Kafka** into a partitioned **S3 data lake**,
with the AWS infrastructure provisioned by **Terraform**.

**Scale:** about **8,600 events per poll**, polled every minute, up to about **12 million events per day**.
Gzip compression reduces the raw JSON about **17×** on disk.

## Architecture

```mermaid
flowchart LR
    A[TfL Unified API] -->|poll every 60s| B[Python producer]
    B --> C[(Kafka<br/>tfl.arrivals.raw<br/>tfl.line-status.raw)]
    C --> D[Python consumer]
    D -->|gzipped JSON Lines| E[(S3 bronze lake<br/>eu-north-1)]
    T[Terraform] -.->|provisions| E
    E -.->|planned| F[Spark: silver and gold]
    F -.-> G[Athena]
    G -.-> H[Streamlit dashboard]
```

| Component | What it does |
|---|---|
| **Producer** (`producer.py`) | Polls TfL every 60 seconds, wraps each record in an envelope (`poll_id`, `ingested_at`, `source`, `data`), and publishes it to Kafka keyed by line |
| **Kafka** (`docker-compose.yml`) | Single-node broker in KRaft mode (no ZooKeeper), 3 partitions per topic, 72-hour retention, plus Kafka UI |
| **Consumer** (`consumer.py`) | Reads both topics, buffers messages, and writes gzipped JSON Lines files partitioned by ingestion hour |
| **Sinks** (`sinks.py`) | Write files to the lake: an S3 bucket or a local folder, chosen by one setting (`LAKE_URI`) without changing the consumer |
| **Infrastructure** (`infra/`) | Terraform for the S3 bucket: private, tagged, with a lifecycle rule that expires raw bronze files after 30 days |

Lake layout:
```
s3://<bucket>/bronze/arrivals/dt=2026-10-19/hour=12/part-20261019T124124Z-0473da9b.jsonl.gz
s3://<bucket>/bronze/line_status/dt=2026-10-19/hour=12/part-20261019T124124Z-50f40bb5.jsonl.gz
```

## Design decisions

- **At-least-once delivery.** The consumer commits Kafka offsets only *after* files are
  written. A crash or a failed S3 upload can replay messages (duplicates, removed later in
  silver) but never lose them.
- **Idempotent, safe producer.** `acks=all` and `enable.idempotence=true`, so network retries
  never create duplicates inside Kafka. A delivery callback counts failed messages, and a full
  local queue (Kafka down) pauses the producer instead of crashing it.
- **The raw payload is kept untouched.** Each record is wrapped in an envelope, so the bronze
  layer is a faithful copy of the source and any later transformation can be replayed.
- **Partitioned by ingestion time, in UTC.** Hive-style `dt=/hour=` folders let Spark and
  Athena skip irrelevant data, and a delayed flush still lands each event in the correct hour.
  UTC avoids broken hours when London changes between GMT and BST.
- **Kafka key = `lineId`.** All events for a line go to the same partition, in order.
- **Batched files, not one file per message.** Messages are flushed every 50,000 records or
  5 minutes, which avoids the small-files problem in Spark and Athena.
- **Pluggable storage.** `LocalSink` and `S3Sink` share the same `write()` method, so moving
  the lake from a laptop to S3 changed two lines in the consumer.
- **Infrastructure as code.** The bucket is created by Terraform, never by hand: public access
  is blocked, every resource is tagged, and a lifecycle rule deletes raw bronze files after
  30 days to control storage costs (silver and gold are kept).
- **No long-lived AWS keys.** Local development uses `aws login`, which issues temporary
  credentials (up to 12 hours); nothing secret is stored in the repo.
- **Fault isolation.** If one transport mode fails, the others are still collected. TfL
  rate limits and server errors are retried with exponential backoff.
- **Graceful shutdown.** On Ctrl+C or `docker stop` (SIGINT/SIGTERM), the producer flushes
  queued messages and the consumer writes and commits its remaining buffer before exiting.

## Run it locally

Prerequisites: Python 3.12, Docker.

```bash
# 1. Install
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 2. Start Kafka (Kafka UI at http://localhost:8080)
docker compose up -d

# 3. Run the producer and consumer in two terminals
python -m tfl_pipeline.producer
FLUSH_MAX_SECONDS=60 python -m tfl_pipeline.consumer
```

By default files are written to `data/lake/bronze/`. All settings have sensible defaults and
can be overridden with environment variables; see [`.env.example`](.env.example). A free
[TfL app key](https://api-portal.tfl.gov.uk) is optional and raises the API rate limit.

## Run against AWS

Prerequisites: an AWS account, [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
v2.32+ and [Terraform](https://developer.hashicorp.com/terraform/install) 1.16+.

```bash
# 1. Sign in (temporary credentials, no access keys)
aws login --remote

# 2. Create the S3 bucket
cd infra
terraform init
terraform plan     # review: 3 to add
terraform apply
terraform output lake_bucket_name
cd ..

# 3. Point the consumer at the bucket
LAKE_URI=s3://<bucket-name> FLUSH_MAX_SECONDS=60 python -m tfl_pipeline.consumer
```

`LAKE_URI` accepts `s3://bucket[/prefix]` for S3, or a local path (`./data/lake`, `file://...`).

## Testing and CI

```bash
ruff check .            # lint
ruff format --check .   # formatting
pytest -v               # unit tests
```

GitHub Actions runs all three on every pull request and on every push to `main`, and branch
protection blocks merging until they pass. Unit tests use fixed sample data, so they need
neither Kafka, the TfL API nor AWS.

## Project structure

```
src/tfl_pipeline/
  config.py       settings from environment variables, topic names
  tfl_client.py   TfL API client with retries and timeouts
  events.py       event envelope and lake partitioning
  producer.py     TfL -> Kafka
  consumer.py     Kafka -> bronze lake
  sinks.py        file writers: local folder or S3
infra/            Terraform: S3 data lake bucket
tests/            unit tests
docs/             data exploration notes
docker-compose.yml  local Kafka + Kafka UI
```

See [docs/data_exploration.md](docs/data_exploration.md) for what the TfL data looks like and
the quirks found while exploring it.

## Roadmap

| Phase | What | Status |
|---|---|---|
| 1 | Ingestion: TfL → Kafka → bronze lake | ✅ Done |
| – | CI with GitHub Actions (ruff + pytest), branch protection | ✅ Done |
| 2 | S3 data lake with Terraform; least-privilege IAM; Terraform checks in CI | ⏳ In progress (S3 done) |
| 3 | PySpark: bronze → silver (clean, deduplicate) → gold (KPIs) | Planned |
| 4 | Glue Data Catalog and Athena | Planned |
| 5 | Airflow orchestration and data quality checks | Planned |
| 6 | Streamlit dashboard (live line status, delays, busiest stations) | Planned |
| 7 | Docker images, 24/7 deployment, CD | Planned |
