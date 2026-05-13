from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tracesage.domain import DeployEvent


def build_local_deploy_event(
    repo_path: Path,
    service: str,
    environment: str | None,
    deploy_id: str | None = None,
    commit_sha: str | None = None,
    base_ref: str | None = None,
) -> DeployEvent:
    commit = commit_sha or _git(repo_path, ["rev-parse", "HEAD"])
    short_commit = _git(repo_path, ["rev-parse", "--short", commit]) if commit else None
    branch = _git(repo_path, ["branch", "--show-current"])
    deployed_at = _commit_timestamp(repo_path, commit) if commit else datetime.now(UTC)
    changed_files = _changed_files(repo_path, commit=commit, base_ref=base_ref)
    raw = {
        "provider": "local_git",
        "repo_path": str(repo_path),
        "commit_sha": commit,
        "branch": branch,
        "changed_files": changed_files,
    }
    return DeployEvent(
        id=deploy_id or f"local-{short_commit or 'unknown'}",
        deployed_at=deployed_at,
        service=service,
        version=short_commit,
        environment=environment,
        commit_sha=commit,
        branch=branch,
        changed_files=changed_files,
        repo_url=None,
        provider="local_git",
        raw=raw,
    )


def enrich_deploy_event_from_github(
    event: DeployEvent,
    repo: str,
    token: str | None,
) -> DeployEvent:
    if not event.commit_sha:
        return event
    payload = _github_json(f"https://api.github.com/repos/{repo}/commits/{event.commit_sha}", token)
    if payload is None:
        return event
    pulls_payload = _github_json(
        f"https://api.github.com/repos/{repo}/commits/{event.commit_sha}/pulls",
        token,
    )
    files = [item.get("filename") for item in payload.get("files", []) if item.get("filename")]
    raw = dict(event.raw)
    raw["github_commit"] = payload
    if pulls_payload is not None:
        raw["github_pull_requests"] = pulls_payload
    branch = event.branch
    if not branch and isinstance(pulls_payload, list) and pulls_payload:
        head = pulls_payload[0].get("head") or {}
        branch = head.get("ref")
    return DeployEvent(
        id=event.id,
        deployed_at=event.deployed_at,
        service=event.service,
        version=event.version,
        environment=event.environment,
        commit_sha=payload.get("sha") or event.commit_sha,
        branch=branch,
        changed_files=files or event.changed_files,
        repo_url=payload.get("html_url") or event.repo_url,
        provider="github" if event.provider in (None, "local_git") else event.provider,
        raw=raw,
    )


def fetch_github_deployments(
    repo: str,
    token: str | None,
    service: str,
    limit: int,
    environment: str | None = None,
    api_base: str = "https://api.github.com",
) -> list[DeployEvent]:
    query: dict[str, str] = {"per_page": str(limit)}
    if environment:
        query["environment"] = environment
    import urllib.parse

    url = f"{api_base.rstrip('/')}/repos/{repo}/deployments?{urllib.parse.urlencode(query)}"
    payload = _github_json(url, token)
    if not isinstance(payload, list):
        return []
    events: list[DeployEvent] = []
    for item in payload[:limit]:
        sha = item.get("sha")
        ref = item.get("ref")
        deployed_at = _parse_timestamp(item.get("updated_at") or item.get("created_at"))
        raw = {"provider": "github_deployments", "github_deployment": item}
        event = DeployEvent(
            id=f"github-deployment-{item.get('id')}",
            deployed_at=deployed_at,
            service=service,
            version=str(sha)[:12] if sha else None,
            environment=item.get("environment") or environment,
            commit_sha=sha,
            branch=ref,
            changed_files=[],
            repo_url=item.get("url"),
            provider="github_deployments",
            raw=raw,
        )
        events.append(enrich_deploy_event_from_github(event, repo=repo, token=token))
    return events


def _git(repo_path: Path, args: list[str]) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=repo_path,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    value = result.stdout.strip()
    return value or None


def _commit_timestamp(repo_path: Path, commit: str) -> datetime:
    value = _git(repo_path, ["show", "-s", "--format=%cI", commit])
    if not value:
        return datetime.now(UTC)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(UTC)


def _changed_files(repo_path: Path, commit: str | None, base_ref: str | None) -> list[str]:
    if not commit:
        return []
    if base_ref:
        value = _git(repo_path, ["diff", "--name-only", f"{base_ref}..{commit}"])
    else:
        parent = _git(repo_path, ["rev-parse", f"{commit}^"])
        value = _git(repo_path, ["diff", "--name-only", f"{parent}..{commit}"]) if parent else None
    if not value:
        return []
    return [line.strip() for line in value.splitlines() if line.strip()]


def _github_json(url: str, token: str | None) -> dict[str, Any] | None:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "TraceSage",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return None


def _parse_timestamp(value: Any) -> datetime:
    if not value:
        return datetime.now(UTC)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(UTC)
