# TraceSage

TraceSage is a local-first CLI for turning raw logs into semantic incident signals.

It ingests logs, generates embeddings with Hugging Face, clusters related failures, detects cluster growth, correlates incidents with deploy events, and exports incident-style reports.

## Current Scope

- Log ingestion from `JSON`, `JSONL`, `CSV`, or plaintext
- Deploy-event ingestion for release correlation
- Local storage in DuckDB
- Semantic embeddings with `sentence-transformers`
- Similar-log clustering
- Snapshot-based anomaly detection
- Cluster summarization with a deterministic template or Hugging Face provider
- Markdown incident export
- Local benchmarking command
- Live file watch mode for incremental local analysis
- Docker packaging

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Quick Start

Use the sample logs and deploy events included in the repo:

```bash
tracesage ingest examples/sample_logs.jsonl
tracesage deploys examples/deploy_events.jsonl
tracesage embed
tracesage cluster --eps 0.5 --min-samples 2
tracesage summarize --cluster 0
tracesage export --cluster 0 --format md
```

To simulate cluster growth and anomaly detection:

```bash
tracesage ingest examples/anomaly_spike.jsonl
tracesage embed
tracesage cluster --eps 0.5 --min-samples 2
tracesage anomaly
```

To measure a local pipeline run:

```bash
tracesage benchmark --path examples/sample_logs.jsonl --eps 0.5 --min-samples 2
```

To watch a log file as it grows:

```bash
tracesage watch --file ./logs/dev.log --poll-interval 2
```

To run a development command under live observation:

```bash
tracesage run -- python app.py
```

To review promoted incidents:

```bash
tracesage incidents
tracesage inspect --incident 1
tracesage explain --incident 1
tracesage ack --incident 1
tracesage resolve --incident 1
```

## Command Flow

Before using TraceSage, collect the logs you want to analyze into a local file such as `JSON`, `JSONL`, `CSV`, or plaintext. TraceSage works on exported log files, not live log streams.

1. `tracesage ingest <path>`
   Normalize logs and store them in DuckDB.
2. `tracesage deploys <path>`
   Store deploy events for later correlation.
3. `tracesage embed`
   Generate semantic vectors for logs that do not have embeddings yet.
4. `tracesage cluster`
   Group semantically similar logs into recurring issue patterns.
5. `tracesage anomaly`
   Compare clustering snapshots and flag novel or growing patterns.
6. `tracesage summarize --cluster <id>`
   Turn one cluster into an incident-style explanation with deploy correlation.
7. `tracesage export --cluster <id> --format md`
   Write a Markdown incident report.
8. `tracesage watch --file <path>`
   Tail a growing log file, process new lines incrementally, and alert on emerging issues.
9. `tracesage run -- <command>`
   Launch a command, capture stdout/stderr, and analyze failures live during the run.
10. `tracesage incidents`
    List incidents promoted from live anomalies.
11. `tracesage inspect --incident <id>`
    Inspect one incident and its evidence trail.
12. `tracesage explain --incident <id>`
    Explain one incident with representative logs, related sessions, and correlated context.
13. `tracesage ack --incident <id>`
    Mark an incident as acknowledged.
14. `tracesage resolve --incident <id>`
    Mark an incident as resolved.

## Notes

- The first `embed` run may need to download the Hugging Face embedding model.
- The Hugging Face summary provider also needs a one-time model download if you use `--provider huggingface`.
- Cluster and anomaly results improve as you rerun TraceSage with more data over time.
- Deploy correlation uses a configurable time window around the cluster timeline.
- The current clustering pipeline is local-first and tuned for MVP workflows rather than streaming scale.

## Useful Environment Variables

- `TRACESAGE_DB_PATH`
  Override the DuckDB file path.
- `TRACESAGE_HF_CACHE_DIR`
  Override the Hugging Face cache location.
- `TRACESAGE_SUMMARY_PROVIDER`
  Set the default summary provider: `template` or `huggingface`.
- `TRACESAGE_HF_SUMMARY_MODEL`
  Override the Hugging Face summary model.
- `TRACESAGE_EXPORT_DIR`
  Override the default Markdown export directory.
- `TRACESAGE_DEPLOY_WINDOW_MINUTES`
  Control how far around a cluster timeline deploy correlation should search.

## Docker

Build the image:

```bash
docker build -t tracesage .
```

Run the CLI inside Docker:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace tracesage ingest examples/sample_logs.jsonl
```

The DuckDB file and Hugging Face cache should be mounted from the host if you want data and models to persist across runs.

## Architecture

- `tracesage/ingest.py`
  Normalizes logs and deploy events.
- `tracesage/storage.py`
  Stores logs, embeddings, clusters, deploy events, summaries, and history in DuckDB.
- `tracesage/ml/embeddings.py`
  Generates semantic vectors from log messages.
- `tracesage/ml/clustering.py`
  Groups related logs into issue patterns.
- `tracesage/ml/summarization.py`
  Produces deterministic or Hugging Face summaries.
- `tracesage/pipeline.py`
  Orchestrates ingest, embed, cluster, anomaly, export, and benchmark flows.
- `tracesage/runtime/watch.py`
  Tails a local file, checkpoints read position, and triggers live processing plus alerts.
- `tracesage/runtime/run.py`
  Launches a subprocess, captures its output, and feeds it through the live analysis loop.
- `incidents` table in DuckDB
  Stores promoted incidents derived from live anomaly signals.

## Status

TraceSage currently includes the Phase 1, Phase 2, and baseline Phase 3 workflow:

- ingestion
- deploy correlation input
- embeddings
- clustering
- anomaly detection
- summarization
- markdown export
- benchmark command
- watch mode
- run mode
- incident review commands
- incident lifecycle commands
- incident explanation command
- docker packaging
