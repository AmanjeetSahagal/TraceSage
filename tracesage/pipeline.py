from __future__ import annotations

import json
import time
from datetime import timedelta
from statistics import mean, pstdev
from pathlib import Path

from tracesage.domain import AnomalyRecord, BenchmarkResult, IncidentSummary
from tracesage.config import Settings
from tracesage.ingest import load_deploy_events, load_logs
from tracesage.ml.clustering import cluster_embeddings
from tracesage.storage import TraceSageDB


def ingest_logs(path: Path, settings: Settings) -> int:
    records = load_logs(path)
    db = TraceSageDB(settings.db_path)
    return db.upsert_logs(records)


def ingest_deploys(path: Path, settings: Settings) -> int:
    records = load_deploy_events(path)
    db = TraceSageDB(settings.db_path)
    return db.upsert_deploy_events(records)


def embed_logs(settings: Settings) -> int:
    db = TraceSageDB(settings.db_path)
    pending = db.fetch_logs_missing_embeddings()
    if not pending:
        return 0
    from tracesage.ml.embeddings import HFEmbeddingProvider

    provider = HFEmbeddingProvider(
        settings.embedding_model,
        batch_size=settings.embedding_batch_size,
        cache_dir=settings.hf_cache_dir,
    )
    embeddings_to_store: list[tuple[str, str, list[float]]] = []
    for index in range(0, len(pending), settings.embedding_batch_size):
        batch = pending[index : index + settings.embedding_batch_size]
        texts = [item[1] for item in batch]
        vectors = provider.embed(texts)
        for (log_id, _), vector in zip(batch, vectors, strict=True):
            embeddings_to_store.append((log_id, settings.embedding_model, vector))
    return db.store_embeddings(embeddings_to_store)


def cluster_logs(
    settings: Settings,
    eps: float,
    min_samples: int,
) -> tuple[bool, int, int, list[tuple[int, str, int, str | None, str | None, str]]]:
    db = TraceSageDB(settings.db_path)
    raw_rows = db.fetch_embedding_matrix()
    if not raw_rows:
        return False, 0, 0, []
    parsed_rows = [
        (log_id, json.loads(vector_json), timestamp, message, service)
        for log_id, vector_json, timestamp, message, service in raw_rows
    ]
    assignments, summaries = cluster_embeddings(parsed_rows, eps=eps, min_samples=min_samples)
    noise_count = sum(1 for cluster_id, _log_id in assignments if cluster_id == -1)
    run_id = db.replace_clusters(
        assignments,
        summaries,
        eps=eps,
        min_samples=min_samples,
        noise_count=noise_count,
    )
    return True, noise_count, run_id, db.fetch_cluster_summaries()


def detect_anomalies(settings: Settings, min_growth: int, z_threshold: float) -> list[AnomalyRecord]:
    db = TraceSageDB(settings.db_path)
    latest_run_id, previous_run_id = db.fetch_latest_run_ids()
    if latest_run_id is None:
        return []
    latest_rows = db.fetch_cluster_history_by_run(latest_run_id)
    previous_rows = db.fetch_cluster_history_by_run(previous_run_id) if previous_run_id is not None else []
    previous_by_key = {row[1]: row for row in previous_rows}
    full_history = db.fetch_cluster_history()
    history_by_key: dict[str, list[int]] = {}
    for _run_id, _cluster_id, cluster_key, log_count, _first_seen, _last_seen, _example_message in full_history:
        history_by_key.setdefault(cluster_key, []).append(int(log_count))

    anomalies: list[AnomalyRecord] = []
    for cluster_id, cluster_key, log_count, _first_seen, _last_seen, example_message in latest_rows:
        history = history_by_key.get(cluster_key, [])
        previous_size = int(previous_by_key.get(cluster_key, (None, None, 0, None, None, None))[2] or 0)
        delta = int(log_count) - previous_size
        anomaly_type: str | None = None
        severity = "medium"
        if previous_run_id is not None and cluster_key not in previous_by_key:
            anomaly_type = "novel_cluster"
            severity = "high"
            reason = "Cluster appears in the latest run but was absent in the previous snapshot."
            z_score = 0.0
        elif delta >= min_growth:
            anomaly_type = "growth_spike"
            reason = f"Cluster grew by {delta} logs between the last two clustering runs."
            z_score = 0.0
            if len(history) >= 2:
                baseline = history[:-1]
                sigma = pstdev(baseline) if len(baseline) > 1 else 0.0
                avg = mean(baseline)
                z_score = 0.0 if sigma == 0 else (int(log_count) - avg) / sigma
                if z_score >= z_threshold:
                    anomaly_type = "rare_spike"
                    severity = "high"
                    reason = (
                        f"Cluster size is {z_score:.2f} standard deviations above its historical baseline."
                    )
        else:
            continue
        anomalies.append(
            AnomalyRecord(
                anomaly_type=anomaly_type,
                cluster_key=cluster_key,
                cluster_id=int(cluster_id),
                current_size=int(log_count),
                previous_size=previous_size,
                delta=delta,
                z_score=z_score,
                severity=severity,
                reason=reason,
                example_message=str(example_message),
            )
        )
    anomalies.sort(key=lambda item: (item.severity == "high", item.delta, item.current_size), reverse=True)
    return anomalies


def summarize_cluster(settings: Settings, cluster_id: int, provider_name: str = "template") -> IncidentSummary:
    db = TraceSageDB(settings.db_path)
    cluster_detail = db.fetch_cluster_detail(cluster_id)
    if cluster_detail is None:
        raise ValueError(f"Cluster {cluster_id} does not exist in the latest clustering run.")
    logs = db.fetch_logs_for_cluster(cluster_id)
    if not logs:
        raise ValueError(f"Cluster {cluster_id} has no log records attached to it.")
    from tracesage.ml.summarization import build_summary_provider

    provider = build_summary_provider(
        provider_name=provider_name,
        settings=settings,
    )
    summary = provider.summarize(cluster_detail=cluster_detail, logs=logs)
    deploy_correlation = correlate_cluster_with_deploys(settings, cluster_id)
    summary.deploy_correlation = deploy_correlation
    db.store_incident_summary(summary)
    return summary


def correlate_cluster_with_deploys(settings: Settings, cluster_id: int) -> list[str]:
    db = TraceSageDB(settings.db_path)
    cluster_detail = db.fetch_cluster_detail(cluster_id)
    if cluster_detail is None:
        return []
    _cluster_id, _cluster_key, _log_count, first_seen, last_seen, _example, services_json, _log_ids = cluster_detail
    if first_seen is None or last_seen is None:
        return []
    services = json.loads(services_json)
    window_start = first_seen - timedelta(minutes=settings.deploy_correlation_window_minutes)
    window_end = last_seen + timedelta(minutes=settings.deploy_correlation_window_minutes)
    deploys = db.fetch_deploy_events_for_services(services, window_start, window_end)
    correlated: list[str] = []
    for deploy_id, service, version, environment, deployed_at in deploys[:5]:
        correlated.append(
            f"{service} deployed {version or 'unknown version'} to {environment or 'unknown env'} at {deployed_at} (event {deploy_id})"
        )
    return correlated


def export_cluster_report(
    settings: Settings,
    cluster_id: int,
    output_path: Path | None = None,
    provider_name: str | None = None,
) -> Path:
    summary = summarize_cluster(
        settings,
        cluster_id=cluster_id,
        provider_name=provider_name or settings.summary_provider,
    )
    report = render_markdown_report(summary)
    export_dir = settings.export_dir
    export_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_path or (export_dir / f"cluster-{cluster_id}.md")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    final_path.write_text(report, encoding="utf-8")
    return final_path


def render_markdown_report(summary: IncidentSummary) -> str:
    affected_services = ", ".join(summary.affected_services) or "unknown"
    timeline_lines = "\n".join(f"- {item}" for item in summary.timeline) or "- unavailable"
    log_lines = "\n".join(f"- `{item}`" for item in summary.representative_logs) or "- unavailable"
    deploy_lines = (
        "\n".join(f"- {item}" for item in summary.deploy_correlation)
        if summary.deploy_correlation
        else "- No correlated deploy events found in the configured time window."
    )
    return (
        f"# TraceSage Incident Report\n\n"
        f"## Cluster\n\n"
        f"- Cluster ID: {summary.cluster_id}\n"
        f"- Cluster Key: `{summary.cluster_key}`\n"
        f"- Affected Services: {affected_services}\n"
        f"- Confidence: {summary.confidence:.2f}\n\n"
        f"## Description\n\n"
        f"{summary.description}\n\n"
        f"## Timeline\n\n"
        f"{timeline_lines}\n\n"
        f"## Representative Logs\n\n"
        f"{log_lines}\n\n"
        f"## Suspected Root Cause\n\n"
        f"{summary.suspected_root_cause}\n\n"
        f"## Deploy Correlation\n\n"
        f"{deploy_lines}\n"
    )


def benchmark_pipeline(
    settings: Settings,
    log_path: Path,
    eps: float,
    min_samples: int,
) -> BenchmarkResult:
    start = time.perf_counter()
    ingest_start = start
    ingested_logs = ingest_logs(log_path, settings)
    ingest_seconds = time.perf_counter() - ingest_start

    embed_start = time.perf_counter()
    embedded_logs = embed_logs(settings)
    embed_seconds = time.perf_counter() - embed_start

    cluster_start = time.perf_counter()
    _has_embeddings, _noise_count, _run_id, summaries = cluster_logs(
        settings,
        eps=eps,
        min_samples=min_samples,
    )
    cluster_seconds = time.perf_counter() - cluster_start

    total_seconds = time.perf_counter() - start
    return BenchmarkResult(
        ingest_seconds=ingest_seconds,
        embed_seconds=embed_seconds,
        cluster_seconds=cluster_seconds,
        total_seconds=total_seconds,
        ingested_logs=ingested_logs,
        embedded_logs=embedded_logs,
        cluster_count=len(summaries),
    )
