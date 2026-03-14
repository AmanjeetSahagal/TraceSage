from __future__ import annotations

import json
import subprocess
import time
from datetime import timedelta
from statistics import mean, pstdev
from pathlib import Path
from math import sqrt

from tracesage.domain import (
    AnomalyRecord,
    BenchmarkResult,
    IncidentEvidence,
    IncidentExplanation,
    IncidentRecord,
    IncidentSummary,
    WatchResult,
)
from tracesage.config import Settings
from tracesage.ingest import load_deploy_events, load_logs, normalize_live_line
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
    return embed_logs_with_provider(settings)


def embed_logs_with_provider(settings: Settings, provider=None) -> int:
    db = TraceSageDB(settings.db_path)
    pending = db.fetch_logs_missing_embeddings()
    if not pending:
        return 0
    if provider is None:
        from tracesage.ml.embeddings import build_embedding_provider

        provider = build_embedding_provider(
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
    latest_rows = db.fetch_cluster_history_detail_by_run(latest_run_id)
    previous_rows = (
        db.fetch_cluster_history_detail_by_run(previous_run_id)
        if previous_run_id is not None
        else []
    )
    previous_by_key = {row[1]: row for row in previous_rows}
    full_history = db.fetch_cluster_history()
    history_by_key: dict[str, list[int]] = {}
    for _run_id, _cluster_id, cluster_key, log_count, _first_seen, _last_seen, _example_message in full_history:
        history_by_key.setdefault(cluster_key, []).append(int(log_count))

    anomalies: list[AnomalyRecord] = []
    for (
        cluster_id,
        cluster_key,
        log_count,
        _first_seen,
        _last_seen,
        example_message,
        services_json,
        centroid_json,
        _log_ids_json,
    ) in latest_rows:
        history = history_by_key.get(cluster_key, [])
        previous_row = previous_by_key.get(cluster_key)
        if previous_row is None and previous_rows:
            previous_row = _match_previous_cluster(
                cluster_key=cluster_key,
                services=json.loads(services_json),
                centroid=json.loads(centroid_json),
                previous_rows=previous_rows,
                threshold=settings.cluster_match_threshold,
            )
        previous_size = int(previous_row[2] if previous_row is not None else 0)
        delta = int(log_count) - previous_size
        anomaly_type: str | None = None
        severity = "medium"
        if previous_run_id is not None and previous_row is None:
            anomaly_type = "novel_cluster"
            severity = "high"
            reason = "Cluster appears in the latest run but was absent in the previous snapshot."
            z_score = 0.0
        elif previous_run_id is not None and delta >= min_growth:
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


def _match_previous_cluster(
    cluster_key: str,
    services: list[str],
    centroid: list[float],
    previous_rows: list[tuple[int, str, int, str | None, str | None, str, str, str, str]],
    threshold: float,
):
    best_row = None
    best_score = -1.0
    service_set = {item for item in services if item}
    for row in previous_rows:
        previous_cluster_key = row[1]
        previous_services = set(json.loads(row[6]))
        previous_centroid = json.loads(row[7])
        score = _cluster_similarity(
            cluster_key=cluster_key,
            services=service_set,
            centroid=centroid,
            previous_cluster_key=previous_cluster_key,
            previous_services=previous_services,
            previous_centroid=previous_centroid,
        )
        if score > best_score:
            best_score = score
            best_row = row
    if best_score >= threshold:
        return best_row
    return None


def _cluster_similarity(
    cluster_key: str,
    services: set[str],
    centroid: list[float],
    previous_cluster_key: str,
    previous_services: set[str],
    previous_centroid: list[float],
) -> float:
    key_score = 1.0 if cluster_key == previous_cluster_key else 0.0
    service_score = (
        len(services & previous_services) / len(services | previous_services)
        if services or previous_services
        else 1.0
    )
    centroid_score = _cosine_similarity(centroid, previous_centroid)
    return (0.6 * centroid_score) + (0.25 * service_score) + (0.15 * key_score)


def _cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    numerator = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = sqrt(sum(a * a for a in left))
    right_norm = sqrt(sum(b * b for b in right))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return numerator / (left_norm * right_norm)


def promote_anomalies_to_incidents(settings: Settings, anomalies: list[AnomalyRecord]) -> list[int]:
    if not anomalies:
        return []
    db = TraceSageDB(settings.db_path)
    latest_cluster_rows = {
        row[1]: row for row in db.fetch_cluster_history_by_run(db.fetch_latest_run_ids()[0] or 0)
    }
    incident_ids: list[int] = []
    for anomaly in anomalies:
        cluster_row = latest_cluster_rows.get(anomaly.cluster_key)
        first_seen = cluster_row[3] if cluster_row else None
        last_seen = cluster_row[4] if cluster_row else None
        cluster_detail = db.fetch_cluster_detail(anomaly.cluster_id)
        services: list[str] = []
        if cluster_detail is not None:
            services = json.loads(cluster_detail[6])
        deploy_correlation = correlate_cluster_with_deploys(settings, anomaly.cluster_id)
        title = f"{anomaly.anomaly_type} in cluster {anomaly.cluster_id}"
        service_text = f" affecting {', '.join(services)}" if services else ""
        summary = f"{anomaly.reason.rstrip('.')}{service_text}."
        confidence = min(0.95, 0.55 + (0.1 if anomaly.severity == "high" else 0.0) + min(anomaly.delta, 5) * 0.03)
        incident_id = db.upsert_incident(
            cluster_key=anomaly.cluster_key,
            cluster_id=anomaly.cluster_id,
            severity=anomaly.severity,
            title=title,
            summary=summary,
            first_seen=first_seen,
            last_seen=last_seen,
            current_size=anomaly.current_size,
            confidence=confidence,
        )
        incident_detail = db.fetch_incident_detail(incident_id)
        if incident_detail is not None and incident_detail[3] == "regressed":
            db.add_incident_evidence(
                incident_id=incident_id,
                evidence_type="status_change",
                details="Incident reopened as regressed because the same cluster key reappeared after resolution.",
            )
        db.add_incident_evidence(
            incident_id=incident_id,
            evidence_type="anomaly",
            details=(
                f"{anomaly.anomaly_type}: current={anomaly.current_size}, previous={anomaly.previous_size}, "
                f"delta={anomaly.delta}, z_score={anomaly.z_score:.2f}, reason={anomaly.reason}"
            ),
        )
        if services:
            db.add_incident_evidence(
                incident_id=incident_id,
                evidence_type="services",
                details=f"Affected services: {', '.join(services)}",
            )
        representative_logs = db.fetch_representative_logs_for_cluster(anomaly.cluster_id, limit=3)
        for message in representative_logs:
            db.add_incident_evidence(
                incident_id=incident_id,
                evidence_type="representative_log",
                details=message,
            )
        session_rows = db.fetch_sessions_for_cluster(anomaly.cluster_id)
        for session_id, command, git_sha, exit_code in session_rows:
            db.add_incident_evidence(
                incident_id=incident_id,
                evidence_type="session",
                details=(
                    f"session={session_id} command={command} git_sha={git_sha or 'unknown'} "
                    f"exit_code={exit_code if exit_code is not None else 'running'}"
                ),
            )
        for deploy in deploy_correlation:
            db.add_incident_evidence(
                incident_id=incident_id,
                evidence_type="deploy_correlation",
                details=deploy,
            )
        incident_ids.append(incident_id)
    return incident_ids


def list_incidents(settings: Settings) -> list[IncidentRecord]:
    db = TraceSageDB(settings.db_path)
    return [
        IncidentRecord(
            incident_id=row[0],
            cluster_key=row[1],
            cluster_id=row[2],
            status=row[3],
            severity=row[4],
            title=row[5],
            summary="",
            first_seen=row[6],
            last_seen=row[7],
            current_size=row[8],
            confidence=row[9],
        )
        for row in db.fetch_incidents()
    ]


def inspect_incident(
    settings: Settings,
    incident_id: int,
) -> tuple[IncidentRecord, list[IncidentEvidence]]:
    db = TraceSageDB(settings.db_path)
    detail = db.fetch_incident_detail(incident_id)
    if detail is None:
        raise ValueError(f"Incident {incident_id} does not exist.")
    evidence_rows = db.fetch_incident_evidence(incident_id)
    incident = IncidentRecord(
        incident_id=detail[0],
        cluster_key=detail[1],
        cluster_id=detail[2],
        status=detail[3],
        severity=detail[4],
        title=detail[5],
        summary=detail[6],
        first_seen=detail[7],
        last_seen=detail[8],
        current_size=detail[9],
        confidence=detail[10],
    )
    evidence = [
        IncidentEvidence(
            incident_id=row[0],
            evidence_type=row[1],
            details=row[2],
            created_at=row[3],
        )
        for row in evidence_rows
    ]
    return incident, evidence


def explain_incident(settings: Settings, incident_id: int) -> IncidentExplanation:
    db = TraceSageDB(settings.db_path)
    incident, evidence = inspect_incident(settings, incident_id)
    representative_logs = db.fetch_recent_unique_logs_for_cluster(incident.cluster_id, limit=5)
    session_rows = db.fetch_sessions_for_cluster(incident.cluster_id)
    related_sessions = [
        f"session={session_id} command={command} git_sha={git_sha or 'unknown'} exit_code={exit_code if exit_code is not None else 'running'}"
        for session_id, command, git_sha, exit_code in session_rows
    ]
    deploy_correlation = [item.details for item in evidence if item.evidence_type == "deploy_correlation"]
    return IncidentExplanation(
        incident=incident,
        evidence=evidence,
        representative_logs=representative_logs,
        related_sessions=related_sessions,
        deploy_correlation=deploy_correlation,
    )


def summarize_incident(settings: Settings, incident_id: int) -> str:
    explanation = explain_incident(settings, incident_id)
    deploy_text = (
        f" Correlated deploys: {'; '.join(explanation.deploy_correlation)}."
        if explanation.deploy_correlation
        else ""
    )
    session_text = (
        f" Recent sessions: {'; '.join(explanation.related_sessions[:2])}."
        if explanation.related_sessions
        else ""
    )
    return (
        f"Incident {explanation.incident.incident_id} is {explanation.incident.status} with "
        f"{explanation.incident.severity} severity. {explanation.incident.summary}"
        f"{deploy_text}{session_text}"
    )


def set_incident_status(settings: Settings, incident_id: int, status: str) -> IncidentRecord:
    db = TraceSageDB(settings.db_path)
    detail = db.fetch_incident_detail(incident_id)
    if detail is None:
        raise ValueError(f"Incident {incident_id} does not exist.")
    db.update_incident_status(incident_id, status)
    updated = db.fetch_incident_detail(incident_id)
    assert updated is not None
    return IncidentRecord(
        incident_id=updated[0],
        cluster_key=updated[1],
        cluster_id=updated[2],
        status=updated[3],
        severity=updated[4],
        title=updated[5],
        summary=updated[6],
        first_seen=updated[7],
        last_seen=updated[8],
        current_size=updated[9],
        confidence=updated[10],
    )


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


def export_incident_report(
    settings: Settings,
    incident_id: int,
    output_path: Path | None = None,
) -> Path:
    explanation = explain_incident(settings, incident_id=incident_id)
    report = render_incident_markdown_report(explanation)
    export_dir = settings.export_dir
    export_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_path or (export_dir / f"incident-{incident_id}.md")
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


def render_incident_markdown_report(explanation: IncidentExplanation) -> str:
    evidence_lines = (
        "\n".join(f"- [{item.evidence_type}] {item.details}" for item in explanation.evidence)
        if explanation.evidence
        else "- No incident evidence stored."
    )
    log_lines = (
        "\n".join(f"- `{item}`" for item in explanation.representative_logs)
        if explanation.representative_logs
        else "- No representative logs found."
    )
    session_lines = (
        "\n".join(f"- {item}" for item in explanation.related_sessions)
        if explanation.related_sessions
        else "- No related sessions captured."
    )
    deploy_lines = (
        "\n".join(f"- {item}" for item in explanation.deploy_correlation)
        if explanation.deploy_correlation
        else "- No correlated deploy events found."
    )
    incident = explanation.incident
    return (
        f"# TraceSage Incident Report\n\n"
        f"## Incident\n\n"
        f"- Incident ID: {incident.incident_id}\n"
        f"- Status: {incident.status}\n"
        f"- Severity: {incident.severity}\n"
        f"- Cluster ID: {incident.cluster_id}\n"
        f"- Cluster Key: `{incident.cluster_key}`\n"
        f"- Confidence: {incident.confidence:.2f}\n"
        f"- First Seen: {incident.first_seen or '-'}\n"
        f"- Last Seen: {incident.last_seen or '-'}\n"
        f"- Current Size: {incident.current_size}\n\n"
        f"## Title\n\n"
        f"{incident.title}\n\n"
        f"## Summary\n\n"
        f"{incident.summary}\n\n"
        f"## Representative Logs\n\n"
        f"{log_lines}\n\n"
        f"## Related Sessions\n\n"
        f"{session_lines}\n\n"
        f"## Deploy Correlation\n\n"
        f"{deploy_lines}\n\n"
        f"## Evidence\n\n"
        f"{evidence_lines}\n"
    )


def benchmark_pipeline(
    settings: Settings,
    log_path: Path,
    eps: float,
    min_samples: int,
    provider=None,
) -> BenchmarkResult:
    start = time.perf_counter()
    ingest_start = start
    ingested_logs = ingest_logs(log_path, settings)
    ingest_seconds = time.perf_counter() - ingest_start

    embed_start = time.perf_counter()
    embedded_logs = embed_logs_with_provider(settings, provider=provider)
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
        ingest_logs_per_second=_rate(ingested_logs, ingest_seconds),
        embed_logs_per_second=_rate(embedded_logs, embed_seconds),
        cluster_logs_per_second=_rate(ingested_logs, cluster_seconds),
    )


def ingest_watched_lines(settings: Settings, source: str, lines: list[tuple[int, str]]) -> int:
    return ingest_live_lines(settings, source=source, lines=lines)


def ingest_live_lines(
    settings: Settings,
    source: str,
    lines: list[tuple[int, str]],
    session_id: int | None = None,
    service: str | None = None,
) -> int:
    records = [
        record
        for offset, line in lines
        if (
            record := normalize_live_line(
                line,
                source=source,
                offset=offset,
                session_id=session_id,
                service=service,
            )
        )
        is not None
    ]
    if not records:
        return 0
    db = TraceSageDB(settings.db_path)
    return db.upsert_logs(records)


def process_watch_iteration(
    settings: Settings,
    source: str,
    lines: list[tuple[int, str]],
    eps: float,
    min_samples: int,
    min_growth: int,
    z_threshold: float,
    session_id: int | None = None,
    service: str | None = None,
) -> tuple[WatchResult, list[AnomalyRecord]]:
    return process_live_iteration(
        settings=settings,
        source=source,
        lines=lines,
        eps=eps,
        min_samples=min_samples,
        min_growth=min_growth,
        z_threshold=z_threshold,
        session_id=session_id,
        service=service,
    )


def process_live_iteration(
    settings: Settings,
    source: str,
    lines: list[tuple[int, str]],
    eps: float,
    min_samples: int,
    min_growth: int,
    z_threshold: float,
    session_id: int | None = None,
    service: str | None = None,
    provider=None,
) -> tuple[WatchResult, list[AnomalyRecord]]:
    ingested_logs = ingest_live_lines(
        settings,
        source=source,
        lines=lines,
        session_id=session_id,
        service=service,
    )
    if ingested_logs == 0:
        return WatchResult(0, 0, None, 0), []
    embedded_logs = embed_logs_with_provider(settings, provider=provider)
    has_embeddings, _noise_count, run_id, _summaries = cluster_logs(
        settings,
        eps=eps,
        min_samples=min_samples,
    )
    anomalies = detect_anomalies(settings, min_growth=min_growth, z_threshold=z_threshold) if has_embeddings else []
    promote_anomalies_to_incidents(settings, anomalies)
    return WatchResult(
        ingested_logs=ingested_logs,
        embedded_logs=embedded_logs,
        cluster_run_id=run_id if has_embeddings else None,
        anomaly_count=len(anomalies),
    ), anomalies


def create_run_session(settings: Settings, command: list[str]) -> int:
    db = TraceSageDB(settings.db_path)
    git_sha = _resolve_git_sha()
    return db.create_run_session(" ".join(command), git_sha=git_sha)


def complete_run_session(settings: Settings, session_id: int, exit_code: int) -> None:
    db = TraceSageDB(settings.db_path)
    db.complete_run_session(session_id, exit_code)


def _resolve_git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    value = result.stdout.strip()
    return value or None


def _rate(count: int, seconds: float) -> float:
    if seconds <= 0:
        return float(count)
    return count / seconds
