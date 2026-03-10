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
