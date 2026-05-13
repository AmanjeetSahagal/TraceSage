from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from tracesage.domain import CIFailurePattern, LogRecord


def load_ci_failures(path: Path) -> list[LogRecord]:
    return [_normalize_ci_failure(payload) for payload in _load_json_records(path)]


def fetch_github_actions_failures(
    repo: str,
    token: str | None,
    limit: int,
    api_base: str = "https://api.github.com",
) -> list[LogRecord]:
    runs_url = (
        f"{api_base.rstrip('/')}/repos/{repo}/actions/runs?"
        + urllib.parse.urlencode({"status": "completed", "per_page": str(limit)})
    )
    runs_payload = _github_json(runs_url, token)
    records: list[LogRecord] = []
    for run in runs_payload.get("workflow_runs", [])[:limit]:
        if str(run.get("conclusion")).lower() not in {"failure", "timed_out", "cancelled", "action_required"}:
            continue
        jobs_url = run.get("jobs_url")
        jobs_payload = _github_json(str(jobs_url), token) if jobs_url else {"jobs": []}
        failed_jobs = [
            job
            for job in jobs_payload.get("jobs", [])
            if str(job.get("conclusion")).lower() in {"failure", "timed_out", "cancelled", "action_required"}
        ]
        if not failed_jobs:
            records.append(_normalize_ci_failure(_run_payload_to_failure(run, None)))
            continue
        for job in failed_jobs:
            records.append(_normalize_ci_failure(_run_payload_to_failure(run, job)))
    return records


def summarize_ci_patterns(records: Iterable[LogRecord]) -> list[CIFailurePattern]:
    grouped: dict[tuple[str, str | None, str | None], list[dict[str, Any]]] = {}
    for record in records:
        raw = record.raw
        signature = raw.get("failure_signature")
        if not signature and raw.get("test_name"):
            signature = raw.get("test_name")
        key = (
            str(signature or record.message).lower(),
            raw.get("test_name"),
            raw.get("workflow"),
        )
        grouped.setdefault(key, []).append(raw)
    patterns: list[CIFailurePattern] = []
    for (signature, test_name, workflow), rows in grouped.items():
        statuses = [str(row.get("status") or "failed").lower() for row in rows]
        failed_runs = sum(1 for status in statuses if status in {"failed", "failure", "timed_out", "cancelled"})
        total_runs = len(rows)
        flaky = failed_runs > 0 and failed_runs < total_runs
        commit_sha = next((row.get("commit_sha") or row.get("sha") for row in rows if row.get("commit_sha") or row.get("sha")), None)
        reason = (
            f"Test {test_name or 'unknown'} failed in {failed_runs}/{total_runs} recent CI runs"
            f"{' and appears flaky' if flaky else ''}."
        )
        patterns.append(
            CIFailurePattern(
                pattern=signature,
                test_name=test_name,
                workflow=workflow,
                failed_runs=failed_runs,
                total_runs=total_runs,
                flaky=flaky,
                commit_sha=str(commit_sha) if commit_sha else None,
                reason=reason,
            )
        )
    patterns.sort(key=lambda item: (item.flaky, item.failed_runs, item.total_runs), reverse=True)
    return patterns


def _load_json_records(path: Path) -> Iterable[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".jsonl":
        for line in text.splitlines():
            if line.strip():
                yield json.loads(line)
        return
    parsed = json.loads(text)
    if isinstance(parsed, list):
        yield from parsed
    elif isinstance(parsed, dict):
        runs = parsed.get("runs") or parsed.get("failures")
        if isinstance(runs, list):
            yield from runs
        else:
            yield parsed
    else:
        raise ValueError("CI input must be a JSON object, array, or JSONL records.")


def _normalize_ci_failure(payload: dict[str, Any]) -> LogRecord:
    message = (
        payload.get("message")
        or payload.get("failure_message")
        or payload.get("log")
        or payload.get("error")
        or payload.get("conclusion")
        or "CI failure"
    )
    timestamp = _coerce_timestamp(
        payload.get("timestamp")
        or payload.get("started_at")
        or payload.get("completed_at")
        or payload.get("created_at")
    )
    raw = dict(payload)
    raw["source_type"] = "ci"
    raw.setdefault("service", "ci")
    raw.setdefault("level", "ERROR")
    raw.setdefault("message", str(message))
    raw.setdefault("failure_signature", _failure_signature(raw, str(message)))
    seed = json.dumps(raw, sort_keys=True, default=str)
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return LogRecord(
        id=f"ci-{digest[:16]}",
        timestamp=timestamp,
        service=str(raw.get("service") or "ci"),
        level=str(raw.get("level") or "ERROR"),
        message=str(message),
        raw=raw,
    )


def _coerce_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(UTC)


def _run_payload_to_failure(run: dict[str, Any], job: dict[str, Any] | None) -> dict[str, Any]:
    head_sha = run.get("head_sha")
    workflow = run.get("name") or run.get("display_title")
    job_name = job.get("name") if job else None
    conclusion = (job or run).get("conclusion")
    message = f"{workflow or 'workflow'} failed"
    if job_name:
        message = f"{job_name} failed in {workflow or 'workflow'}"
    return {
        "workflow": workflow,
        "test_name": job_name,
        "status": conclusion or "failed",
        "message": message,
        "failure_message": message,
        "failure_signature": job_name or workflow or message,
        "commit_sha": head_sha,
        "branch": run.get("head_branch"),
        "run_id": run.get("id"),
        "run_number": run.get("run_number"),
        "job_id": job.get("id") if job else None,
        "html_url": (job or run).get("html_url"),
        "logs_url": job.get("logs_url") if job else run.get("logs_url"),
        "started_at": (job or run).get("started_at") or run.get("created_at"),
        "completed_at": (job or run).get("completed_at") or run.get("updated_at"),
        "source_type": "ci",
        "provider": "github_actions",
    }


def _failure_signature(raw: dict[str, Any], message: str) -> str:
    test_name = raw.get("test_name")
    if test_name:
        return str(test_name)
    workflow = raw.get("workflow")
    if workflow and "timeout" in message.lower():
        return f"{workflow}:timeout"
    return " ".join(message.lower().split()[:8])


def _github_json(url: str, token: str | None) -> dict[str, Any]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "TraceSage",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"GitHub API request failed with HTTP {exc.code}: {url}") from exc
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"GitHub API request failed: {url}") from exc
