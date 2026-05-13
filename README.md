# TraceSage

TraceSage is a local-first intelligent triage CLI that works alongside a developer while an application is running.

It can watch a growing log file, consume a piped log stream, or wrap a development command directly, then generate embeddings with Hugging Face, cluster related failures, detect growth and regression patterns, promote anomalies into incidents, correlate them with deploy events, and export incident-style reports.

## Current Scope

- Live developer workflow with `watch --file`, `watch --stdin`, and `run -- <command>`
- Incident promotion, explanation, acknowledgement, resolution, and Markdown export
- Log ingestion from `JSON`, `JSONL`, `CSV`, or plaintext for offline analysis
- Deploy-event ingestion for release correlation
- Deploy/git metadata enrichment with commit, branch, and changed-file evidence
- Deploy-bound regression detection for new clusters, frequency spikes, and reappearing failures
- GitHub Actions failure fixture ingestion and flaky-pattern summaries
- Local storage in DuckDB
- Semantic embeddings with `sentence-transformers`
- Similar-log clustering
- Snapshot-based anomaly detection
- Cluster summarization with a deterministic template or Hugging Face provider
- Local benchmarking command
- Optional JSONL alert hooks for live automation
- Automated tests for ingestion, clustering, incident promotion, and export
- Stable cross-run cluster matching and service inference for noisy live logs
- Benchmark throughput output in logs/sec
- Docker packaging

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Quick Start

The primary workflow is live developer-side triage. For example:

```bash
tracesage run --service api -- python app.py
tracesage incidents
tracesage explain --incident 1
tracesage export --incident 1 --format md
```

You can also watch existing logs while a service is running:

```bash
tracesage watch --file ./logs/dev.log --service api --poll-interval 2
docker compose logs -f api | tracesage watch --stdin --service api
```

For offline analysis, use the sample logs and deploy events included in the repo:

```bash
tracesage ingest examples/sample_logs.jsonl
tracesage deploys examples/deploy_events.jsonl
tracesage embed
tracesage cluster --eps 0.5 --min-samples 2
tracesage regressions --promote
tracesage summarize --cluster 0
tracesage export --cluster 0 --format md
```

To create an enriched deploy event from the current git checkout, optionally backed by GitHub commit metadata:

```bash
tracesage deploy-enrich --service api --environment prod --source both
```

GitHub enrichment uses `TRACESAGE_GITHUB_REPO` and, for private repositories or higher rate limits, `TRACESAGE_GITHUB_TOKEN`.

To ingest GitHub Deployment API records:

```bash
tracesage github-deployments-ingest --repo owner/name --service api --environment prod --limit 10
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

The benchmark output reports both elapsed time and throughput in logs/sec.

To review promoted incidents:

```bash
tracesage incidents
tracesage inspect --incident 1
tracesage explain --incident 1
tracesage summarize --incident 1
tracesage timeline --cluster 0
tracesage export --incident 1 --format md
tracesage ack --incident 1
tracesage resolve --incident 1
```

To ingest CI failure fixtures and identify recurring or flaky test patterns:

```bash
tracesage github-actions-ingest --path examples/ci_failures.jsonl
```

To fetch recent failed GitHub Actions jobs directly:

```bash
tracesage github-actions-ingest --repo owner/name --limit 10
```

To score a synthetic incident benchmark:

```bash
tracesage eval --benchmark examples/benchmark_incidents.json
```

## Command Flow

TraceSage is designed to work in two modes:

1. Live mode while you are developing or debugging
   Run your app through TraceSage, tail a file, or pipe logs into stdin so incidents are detected as failures emerge.
2. Batch mode for exported datasets
   Collect logs into a local file such as `JSON`, `JSONL`, `CSV`, or plaintext and run the offline pipeline.

### Live Mode

1. `tracesage watch --file <path>`
   Tail a growing log file, process new lines incrementally, and alert on emerging issues.
2. `tracesage watch --stdin`
   Read logs from stdin until EOF and process the stream as one live batch.
3. `tracesage run -- <command>`
   Launch a command, capture stdout/stderr, and analyze failures live during the run.
4. `tracesage incidents`
   List incidents promoted from live anomalies.
5. `tracesage inspect --incident <id>`
   Inspect one incident and its evidence trail.
6. `tracesage explain --incident <id>`
   Explain one incident with representative logs, related sessions, and correlated context.
7. `tracesage summarize --incident <id>`
   Render an incident-native summary from stored incident context.
8. `tracesage export --incident <id> --format md`
   Export one incident as a Markdown report.
9. `tracesage ack --incident <id>`
   Mark an incident as acknowledged.
10. `tracesage resolve --incident <id>`
   Mark an incident as resolved.

### Batch Mode

1. `tracesage ingest <path>`
   Normalize logs and store them in DuckDB.
2. `tracesage deploys <path>`
   Store deploy events for later correlation.
3. `tracesage embed`
   Generate semantic vectors for logs that do not have embeddings yet.
4. `tracesage cluster`
   Group semantically similar logs into recurring issue patterns.
5. `tracesage regressions --promote`
   Compare cluster frequency before and after deploy events, then promote likely deploy regressions into incidents with evidence.
6. `tracesage anomaly`
   Compare clustering snapshots and flag novel or growing patterns.
7. `tracesage summarize --cluster <id>`
   Turn one cluster into an incident-style explanation with deploy correlation.
8. `tracesage export --cluster <id> --format md`
   Write a Markdown incident report.

## Notes

- The first `embed` run may need to download the Hugging Face embedding model.
- The Hugging Face summary provider also needs a one-time model download if you use `--provider huggingface`.
- Cluster and anomaly results improve as you rerun TraceSage with more data over time.
- Deploy correlation uses a configurable time window around the cluster timeline.
- The current clustering pipeline is local-first and tuned for MVP workflows rather than streaming scale.
- For unstructured live logs, pass `--service` to `watch` or `run` so incidents can be attributed and correlated correctly.
- Live service inference also attempts to extract service names from patterns like `[api] ...`, `service=api`, and `api: ...`.
- `watch` and `run` both support `--alert-file <path>` to append anomaly alerts as JSON lines for simple automation hooks.
- Cross-run anomaly matching falls back to centroid and service similarity when exact cluster keys drift.

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
- `TRACESAGE_CLUSTER_MATCH_THRESHOLD`
  Tune how aggressively TraceSage matches clusters across runs when keys change.
- `TRACESAGE_REGRESSION_BEFORE_MINUTES`
  Control the pre-deploy comparison window for regression detection.
- `TRACESAGE_REGRESSION_AFTER_MINUTES`
  Control the post-deploy comparison window for regression detection.
- `TRACESAGE_REGRESSION_MIN_GROWTH`
  Minimum log-count increase before a deploy-bound spike can be flagged.
- `TRACESAGE_REGRESSION_SPIKE_PERCENT`
  Minimum percentage growth before an existing cluster is treated as a deploy regression.
- `TRACESAGE_GITHUB_REPO`
  Repository in `owner/name` form for GitHub commit enrichment.
- `TRACESAGE_GITHUB_TOKEN`
  Optional GitHub token for private repositories or higher rate limits.
- `TRACESAGE_GIT_BASE_REF`
  Optional base ref for changed-file collection.

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
  Stores logs, embeddings, clusters, incidents, deploy events, summaries, and history in DuckDB.
- `tracesage/ml/embeddings.py`
  Generates semantic vectors from log messages.
- `tracesage/ml/clustering.py`
  Groups related logs into issue patterns and derives stable cluster signatures.
- `tracesage/ml/summarization.py`
  Produces deterministic or Hugging Face summaries.
- `tracesage/pipeline.py`
  Orchestrates ingest, embed, cluster, anomaly, deploy regression, incident promotion, export, and benchmark flows.
- `tracesage/gitmeta.py`
  Collects local git deploy metadata and optional GitHub commit enrichment.
- `tracesage/ci.py`
  Normalizes GitHub Actions-style failure fixtures or live API results into CI log records and recurring failure patterns.
- `tracesage/evaluation.py`
  Scores synthetic incident benchmarks for root-cause and trigger attribution.
- `tracesage/runtime/watch.py`
  Tails a local file or stdin and triggers live processing plus alerts.
- `tracesage/runtime/run.py`
  Launches a subprocess, captures its output, and feeds it through the live analysis loop.
- `incidents` table in DuckDB
  Stores promoted incidents derived from live anomaly signals.
