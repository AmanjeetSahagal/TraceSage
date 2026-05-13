from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tracesage.domain import EvaluationResult


def evaluate_benchmark(path: Path) -> EvaluationResult:
    cases = _load_cases(path)
    if not cases:
        return EvaluationResult(0, 0.0, 0.0, 0.0, 0.0, 0.0)
    root_cause_hits = 0
    root_cause_top3_hits = 0
    trigger_hits = 0
    baseline_hits = 0
    baseline_steps = 0
    tracesage_steps = 0
    for case in cases:
        expected_root = str(case.get("root_cause") or "").lower()
        expected_trigger = str(case.get("trigger") or "").lower()
        report = str(case.get("report") or case.get("predicted_root_cause") or "").lower()
        candidates = _prediction_candidates(case, report)
        predicted_trigger = str(case.get("predicted_trigger") or report).lower()
        logs = " ".join(str(item) for item in case.get("logs", [])).lower()
        if expected_root and expected_root in report:
            root_cause_hits += 1
        if expected_root and any(expected_root in candidate for candidate in candidates[:3]):
            root_cause_top3_hits += 1
        if expected_trigger and expected_trigger in predicted_trigger:
            trigger_hits += 1
        if expected_root and expected_root in logs:
            baseline_hits += 1
        baseline_steps += int(case.get("baseline_triage_steps") or len(case.get("logs", [])) or 1)
        tracesage_steps += int(case.get("tracesage_triage_steps") or 1)
    total = len(cases)
    triage_reduction = 0.0
    if baseline_steps > 0:
        triage_reduction = max(0.0, (baseline_steps - tracesage_steps) / baseline_steps)
    return EvaluationResult(
        incident_count=total,
        root_cause_top1=root_cause_hits / total,
        root_cause_top3=root_cause_top3_hits / total,
        trigger_attribution=trigger_hits / total,
        baseline_keyword_accuracy=baseline_hits / total,
        triage_reduction=triage_reduction,
    )


def _prediction_candidates(case: dict[str, Any], report: str) -> list[str]:
    raw_candidates = case.get("root_cause_candidates") or case.get("predictions")
    if isinstance(raw_candidates, list):
        return [str(item).lower() for item in raw_candidates]
    return [report]


def _load_cases(path: Path) -> list[dict[str, Any]]:
    if path.is_dir():
        cases: list[dict[str, Any]] = []
        for child in sorted(path.glob("*.json")):
            loaded = _load_cases(child)
            cases.extend(loaded)
        return cases
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        cases = parsed.get("cases")
        if isinstance(cases, list):
            return [item for item in cases if isinstance(item, dict)]
        return [parsed]
    return []
