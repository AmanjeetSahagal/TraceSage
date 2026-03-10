from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Protocol

from tracesage.config import Settings
from tracesage.domain import IncidentSummary


class SummaryProvider(Protocol):
    def summarize(
        self,
        cluster_detail: tuple[int, str, int, str | None, str | None, str, str, str],
        logs: list[tuple[str | None, str | None, str | None, str]],
    ) -> IncidentSummary: ...


class TemplateSummaryProvider:
    def summarize(
        self,
        cluster_detail: tuple[int, str, int, str | None, str | None, str, str, str],
        logs: list[tuple[str | None, str | None, str | None, str]],
    ) -> IncidentSummary:
        cluster_id, cluster_key, _size, first_seen, last_seen, example_message, services_json, _log_ids_json = (
            cluster_detail
        )
        services = json.loads(services_json)
        levels = [str(level) for _timestamp, _service, level, _message in logs if level]
        level_summary = Counter(levels).most_common(1)
        dominant_level = level_summary[0][0] if level_summary else "UNKNOWN"
        representative_logs = [message for _ts, _svc, _lvl, message in logs[:5]]
        timeline = []
        if first_seen:
            timeline.append(f"First seen: {first_seen}")
        if last_seen:
            timeline.append(f"Last seen: {last_seen}")
        timeline.append(f"Observed {len(logs)} related logs in the latest clustering run.")
        description = (
            f"Cluster {cluster_id} groups repeated {dominant_level.lower()} log events with the pattern "
            f"'{example_message}'."
        )
        suspected_root_cause = (
            "Likely a recurring issue in the affected service path. Inspect the representative logs and "
            "recent deploys around the first occurrence."
        )
        confidence = min(0.95, 0.45 + (0.1 * min(len(logs), 5)))
        return IncidentSummary(
            cluster_id=cluster_id,
            cluster_key=cluster_key,
            description=description,
            affected_services=services,
            timeline=timeline,
            representative_logs=representative_logs,
            suspected_root_cause=suspected_root_cause,
            confidence=confidence,
        )


class HFSummaryProvider:
    def __init__(self, model_name: str, cache_dir: Path) -> None:
        self.model_name = model_name
        cache_dir.mkdir(parents=True, exist_ok=True)
        from transformers import pipeline

        try:
            self.pipeline = pipeline(
                "text2text-generation",
                model=model_name,
                tokenizer=model_name,
                cache_dir=str(cache_dir),
                local_files_only=True,
            )
        except Exception:
            try:
                self.pipeline = pipeline(
                    "text2text-generation",
                    model=model_name,
                    tokenizer=model_name,
                    cache_dir=str(cache_dir),
                )
            except Exception as exc:
                raise RuntimeError(
                    "Unable to load the Hugging Face summary model. "
                    "If this is the first run, the model must be downloaded or pre-cached."
                ) from exc

    def summarize(
        self,
        cluster_detail: tuple[int, str, int, str | None, str | None, str, str, str],
        logs: list[tuple[str | None, str | None, str | None, str]],
    ) -> IncidentSummary:
        cluster_id, cluster_key, size, first_seen, last_seen, example_message, services_json, _log_ids_json = cluster_detail
        services = json.loads(services_json)
        prompt = _build_incident_prompt(
            cluster_id=cluster_id,
            size=size,
            first_seen=first_seen,
            last_seen=last_seen,
            services=services,
            logs=logs,
        )
        result = self.pipeline(
            prompt,
            max_new_tokens=220,
            do_sample=False,
            truncation=True,
        )
        generated = result[0].get("generated_text", "").strip() if result else ""
        parsed = _parse_structured_generation(generated)
        timeline = parsed["timeline"] or _fallback_timeline(first_seen, last_seen, len(logs))
        confidence = _parse_confidence(parsed["confidence"])
        return IncidentSummary(
            cluster_id=cluster_id,
            cluster_key=cluster_key,
            description=parsed["description"] or f"Cluster {cluster_id} groups repeated events matching '{example_message}'.",
            affected_services=parsed["affected_services"] or services,
            timeline=timeline,
            representative_logs=[message for _ts, _svc, _lvl, message in logs[:5]],
            suspected_root_cause=(
                parsed["suspected_root_cause"]
                or "Model-generated summary; validate against representative logs and deploy history."
            ),
            confidence=confidence,
        )


def build_summary_provider(provider_name: str, settings: Settings) -> SummaryProvider:
    if provider_name == "huggingface":
        return HFSummaryProvider(
            model_name=settings.hf_summary_model,
            cache_dir=settings.hf_cache_dir / "summaries",
        )
    return TemplateSummaryProvider()


def _build_incident_prompt(
    cluster_id: int,
    size: int,
    first_seen: str | None,
    last_seen: str | None,
    services: list[str],
    logs: list[tuple[str | None, str | None, str | None, str]],
) -> str:
    lines = [
        "You are an SRE assistant analyzing a cluster of production logs.",
        "Return exactly these fields on separate lines:",
        "Description:",
        "Affected Services:",
        "Timeline:",
        "Suspected Root Cause:",
        "Confidence:",
        "",
        "Facts:",
        f"- Cluster ID: {cluster_id}",
        f"- Log Count: {size}",
        f"- First Seen: {first_seen or 'unknown'}",
        f"- Last Seen: {last_seen or 'unknown'}",
        f"- Services: {', '.join(services) if services else 'unknown'}",
        "",
        "Representative logs:",
    ]
    for timestamp, service, level, message in logs[:8]:
        lines.append(
            f"- [{timestamp or 'unknown'}] service={service or 'unknown'} level={level or 'unknown'} message={message}"
        )
    lines.append("")
    lines.append("Be concise, operational, and avoid speculation beyond the logs.")
    return "\n".join(lines)


def _parse_structured_generation(generated: str) -> dict[str, str | list[str]]:
    parsed: dict[str, str | list[str]] = {
        "description": "",
        "affected_services": [],
        "timeline": [],
        "suspected_root_cause": "",
        "confidence": "",
    }
    current_field: str | None = None
    for raw_line in generated.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        lowered = line.lower()
        if lowered.startswith("description:"):
            parsed["description"] = line.split(":", 1)[1].strip()
            current_field = "description"
            continue
        if lowered.startswith("affected services:"):
            services_text = line.split(":", 1)[1].strip()
            parsed["affected_services"] = [item.strip() for item in services_text.split(",") if item.strip()]
            current_field = "affected_services"
            continue
        if lowered.startswith("timeline:"):
            timeline_text = line.split(":", 1)[1].strip()
            parsed["timeline"] = [timeline_text] if timeline_text else []
            current_field = "timeline"
            continue
        if lowered.startswith("suspected root cause:"):
            parsed["suspected_root_cause"] = line.split(":", 1)[1].strip()
            current_field = "suspected_root_cause"
            continue
        if lowered.startswith("confidence:"):
            parsed["confidence"] = line.split(":", 1)[1].strip()
            current_field = "confidence"
            continue
        if current_field == "timeline":
            timeline = parsed["timeline"]
            assert isinstance(timeline, list)
            timeline.append(line.lstrip("- ").strip())
        elif current_field == "description" and not parsed["description"]:
            parsed["description"] = line
        elif current_field == "suspected_root_cause" and not parsed["suspected_root_cause"]:
            parsed["suspected_root_cause"] = line
    return parsed


def _fallback_timeline(first_seen: str | None, last_seen: str | None, log_count: int) -> list[str]:
    timeline: list[str] = []
    if first_seen:
        timeline.append(f"First seen: {first_seen}")
    if last_seen:
        timeline.append(f"Last seen: {last_seen}")
    timeline.append(f"Observed {log_count} related logs in the latest clustering run.")
    return timeline


def _parse_confidence(value: str | list[str]) -> float:
    if isinstance(value, list):
        value = value[0] if value else ""
    text = str(value).strip().replace("%", "")
    try:
        numeric = float(text)
    except ValueError:
        return 0.7
    if numeric > 1:
        numeric /= 100
    return max(0.0, min(1.0, numeric))
