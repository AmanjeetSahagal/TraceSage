# TraceSage

TraceSage is a local-first CLI for turning raw logs into semantic incident signals.

It ingests logs, generates embeddings with Hugging Face, clusters related failures, detects cluster growth, and summarizes clusters into incident-style explanations.

## Current Scope

- Log ingestion from `JSON`, `JSONL`, `CSV`, or plaintext
- Local storage in DuckDB
- Semantic embeddings with `sentence-transformers`
- Similar-log clustering
- Snapshot-based anomaly detection
- Cluster summarization with a deterministic template or Hugging Face provider

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Quick Start

Use the sample logs included in the repo:

```bash
tracesage ingest examples/sample_logs.jsonl
tracesage embed
tracesage cluster --eps 0.5 --min-samples 2
tracesage summarize --cluster 0
```

To simulate cluster growth and anomaly detection:

```bash
tracesage ingest examples/anomaly_spike.jsonl
tracesage embed
tracesage cluster --eps 0.5 --min-samples 2
tracesage anomaly
```

## Command Flow

1. `tracesage ingest <path>`
   Normalize logs and store them in DuckDB.
2. `tracesage embed`
   Generate semantic vectors for logs that do not have embeddings yet.
3. `tracesage cluster`
   Group semantically similar logs into recurring issue patterns.
4. `tracesage anomaly`
   Compare clustering snapshots and flag novel or growing patterns.
5. `tracesage summarize --cluster <id>`
   Turn one cluster into an incident-style explanation.

## Notes

- The first `embed` run may need to download the Hugging Face embedding model.
- The Hugging Face summary provider also needs a one-time model download if you use `--provider huggingface`.
- Cluster and anomaly results improve as you rerun TraceSage with more data over time.

## Useful Environment Variables

- `TRACESAGE_DB_PATH`
  Override the DuckDB file path.
- `TRACESAGE_HF_CACHE_DIR`
  Override the Hugging Face cache location.
- `TRACESAGE_SUMMARY_PROVIDER`
  Set the default summary provider: `template` or `huggingface`.
- `TRACESAGE_HF_SUMMARY_MODEL`
  Override the Hugging Face summary model.
