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
        cache_dir.mkdir(parents=True, exist_ok=True)
        from transformers import pipeline

        try:
            self.pipeline = pipeline(
                "summarization",
                model=model_name,
                tokenizer=model_name,
                model_kwargs={"local_files_only": True},
            )
        except Exception:
            self.pipeline = pipeline(
                "summarization",
                model=model_name,
                tokenizer=model_name,
                cache_dir=str(cache_dir),
            )

    def summarize(
        self,
        cluster_detail: tuple[int, str, int, str | None, str | None, str, str, str],
        logs: list[tuple[str | None, str | None, str | None, str]],
    ) -> IncidentSummary:
        cluster_id, cluster_key, _size, first_seen, last_seen, example_message, services_json, _log_ids_json = (
            cluster_detail
        )
        services = json.loads(services_json)
        source_text = "\n".join(message for _ts, _svc, _lvl, message in logs[:10])
        result = self.pipeline(source_text, max_length=120, min_length=30, do_sample=False)
        generated = result[0]["summary_text"] if result else example_message
        timeline = []
        if first_seen:
            timeline.append(f"First seen: {first_seen}")
        if last_seen:
            timeline.append(f"Last seen: {last_seen}")
        return IncidentSummary(
            cluster_id=cluster_id,
            cluster_key=cluster_key,
            description=generated,
            affected_services=services,
            timeline=timeline,
            representative_logs=[message for _ts, _svc, _lvl, message in logs[:5]],
            suspected_root_cause="Model-generated summary; validate against representative logs.",
            confidence=0.65,
        )


def build_summary_provider(provider_name: str, settings: Settings) -> SummaryProvider:
    if provider_name == "huggingface":
        return HFSummaryProvider(
            model_name="sshleifer/distilbart-cnn-12-6",
            cache_dir=settings.hf_cache_dir / "summaries",
        )
    return TemplateSummaryProvider()
