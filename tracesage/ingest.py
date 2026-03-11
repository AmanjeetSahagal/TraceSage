from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from tracesage.domain import DeployEvent, LogRecord


def load_logs(path: Path) -> list[LogRecord]:
    suffix = path.suffix.lower()
    if suffix in {".json", ".jsonl"}:
        payloads = list(_load_json(path))
    elif suffix == ".csv":
        payloads = list(_load_csv(path))
    else:
        payloads = list(_load_plaintext(path))
    return [_normalize_log(payload) for payload in payloads]


def load_deploy_events(path: Path) -> list[DeployEvent]:
    suffix = path.suffix.lower()
    if suffix in {".json", ".jsonl"}:
        payloads = list(_load_json(path))
    elif suffix == ".csv":
        payloads = list(_load_csv(path))
    else:
        raise ValueError("Deploy events must be provided as JSON, JSONL, or CSV.")
    return [_normalize_deploy_event(payload) for payload in payloads]


def normalize_live_line(line: str, source: str, offset: int) -> LogRecord | None:
    stripped = line.strip()
    if not stripped:
        return None
    timestamp = datetime.now(UTC)
    payload: dict[str, Any]
    if stripped.startswith("{"):
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, dict):
                payload = parsed
            else:
                payload = {"message": stripped}
        except json.JSONDecodeError:
            payload = {"message": stripped}
    else:
        payload = {"message": stripped}
    payload.setdefault("timestamp", timestamp.isoformat())
    payload.setdefault("source", source)
    payload["watch_offset"] = offset
    return _normalize_log(payload)


def _load_json(path: Path) -> Iterable[dict[str, Any]]:
    raw_text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".jsonl":
        for line in raw_text.splitlines():
            if line.strip():
                yield json.loads(line)
        return
    parsed = json.loads(raw_text)
    if isinstance(parsed, list):
        yield from parsed
    elif isinstance(parsed, dict):
        yield parsed
    else:
        raise ValueError("JSON log input must be an object, array, or JSONL lines.")


def _load_csv(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        yield from reader


def _load_plaintext(path: Path) -> Iterable[dict[str, Any]]:
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if line.strip():
            yield {"message": line, "line_number": line_number}


def _normalize_log(payload: dict[str, Any]) -> LogRecord:
    message = (
        payload.get("message")
        or payload.get("msg")
        or payload.get("log")
        or payload.get("event")
    )
    if not message:
        raise ValueError(f"Unable to find a message field in log record: {payload}")
    timestamp = _coerce_timestamp(
        payload.get("timestamp")
        or payload.get("ts")
        or payload.get("time")
        or payload.get("@timestamp")
    )
    service = payload.get("service") or payload.get("service_name") or payload.get("app")
    level = payload.get("level") or payload.get("severity")
    stable_id = _build_log_id(payload, str(message))
    return LogRecord(
        id=stable_id,
        timestamp=timestamp,
        service=service,
        level=str(level) if level is not None else None,
        message=str(message),
        raw=payload,
    )


def _normalize_deploy_event(payload: dict[str, Any]) -> DeployEvent:
    deployed_at = _coerce_timestamp(
        payload.get("deployed_at")
        or payload.get("timestamp")
        or payload.get("time")
        or payload.get("@timestamp")
    )
    if deployed_at is None:
        raise ValueError(f"Deploy event is missing a valid timestamp: {payload}")
    service = payload.get("service") or payload.get("service_name") or payload.get("app")
    if not service:
        raise ValueError(f"Deploy event is missing a service field: {payload}")
    version = payload.get("version") or payload.get("release") or payload.get("build")
    environment = payload.get("environment") or payload.get("env")
    deployment_id = payload.get("id") or payload.get("deployment_id") or payload.get("sha")
    if not deployment_id:
        seed = json.dumps(payload, sort_keys=True, default=str)
        deployment_id = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    return DeployEvent(
        id=str(deployment_id),
        deployed_at=deployed_at,
        service=str(service),
        version=str(version) if version is not None else None,
        environment=str(environment) if environment is not None else None,
        raw=payload,
    )


def _coerce_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _build_log_id(payload: dict[str, Any], message: str) -> str:
    seed = json.dumps(payload, sort_keys=True, default=str)
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    message_digest = hashlib.sha256(message.encode("utf-8")).hexdigest()
    return f"{digest[:12]}-{message_digest[:4]}"
