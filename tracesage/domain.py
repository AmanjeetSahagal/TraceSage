from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class LogRecord:
    id: str
    timestamp: datetime | None
    service: str | None
    level: str | None
    message: str
    raw: dict[str, Any]


@dataclass
class ClusterSummary:
    cluster_id: int
    cluster_key: str
    size: int
    first_seen: datetime | None
    last_seen: datetime | None
    example_message: str
    services: list[str]
    centroid: list[float]


@dataclass
class ClusterSnapshot:
    cluster_id: int
    cluster_key: str
    size: int
    first_seen: datetime | None
    last_seen: datetime | None
    example_message: str
    services: list[str]
    centroid: list[float]
    log_ids: list[str]


@dataclass
class AnomalyRecord:
    anomaly_type: str
    cluster_key: str
    cluster_id: int
    current_size: int
    previous_size: int
    delta: int
    z_score: float
    severity: str
    reason: str
    example_message: str


@dataclass
class IncidentSummary:
    cluster_id: int
    cluster_key: str
    description: str
    affected_services: list[str]
    timeline: list[str]
    representative_logs: list[str]
    suspected_root_cause: str
    confidence: float
    deploy_correlation: list[str]


@dataclass
class DeployEvent:
    id: str
    deployed_at: datetime
    service: str
    version: str | None
    environment: str | None
    raw: dict[str, Any]


@dataclass
class BenchmarkResult:
    ingest_seconds: float
    embed_seconds: float
    cluster_seconds: float
    total_seconds: float
    ingested_logs: int
    embedded_logs: int
    cluster_count: int


@dataclass
class WatchResult:
    ingested_logs: int
    embedded_logs: int
    cluster_run_id: int | None
    anomaly_count: int


@dataclass
class RunSession:
    session_id: int
    command: str
    started_at: datetime
    ended_at: datetime | None
    exit_code: int | None
    git_sha: str | None


@dataclass
class IncidentRecord:
    incident_id: int
    cluster_key: str
    cluster_id: int
    status: str
    severity: str
    title: str
    summary: str
    first_seen: datetime | None
    last_seen: datetime | None
    current_size: int
    confidence: float


@dataclass
class IncidentEvidence:
    incident_id: int
    evidence_type: str
    details: str
    created_at: datetime | None


@dataclass
class IncidentExplanation:
    incident: IncidentRecord
    evidence: list[IncidentEvidence]
    representative_logs: list[str]
    related_sessions: list[str]
    deploy_correlation: list[str]
