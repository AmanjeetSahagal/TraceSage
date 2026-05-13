from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tracesage.config import Settings
from tracesage.pipeline import (
    cluster_logs,
    detect_regressions,
    embed_logs_with_provider,
    evaluate_incident_benchmark,
    ingest_ci_failures,
    ingest_deploys,
    ingest_logs,
    inspect_incident,
)
from tracesage import ci
from tracesage import gitmeta


class FakeEmbeddingProvider:
    def embed(self, texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.0] if "timeout" in text.lower() else [0.0, 1.0] for text in texts]


class RegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.settings = Settings(
            db_path=root / "tracesage.duckdb",
            hf_cache_dir=root / "hf-cache",
            export_dir=root / "reports",
            regression_before_minutes=30,
            regression_after_minutes=30,
            regression_min_growth=2,
            regression_spike_percent=200.0,
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_detects_and_promotes_deploy_bound_regression(self) -> None:
        root = Path(self.temp_dir.name)
        logs = root / "logs.jsonl"
        logs.write_text(
            "\n".join(
                [
                    '{"timestamp":"2026-03-14T09:50:00+00:00","service":"api","level":"ERROR","message":"database timeout while creating order"}',
                    '{"timestamp":"2026-03-14T10:02:00+00:00","service":"api","level":"ERROR","message":"database timeout while creating order"}',
                    '{"timestamp":"2026-03-14T10:03:00+00:00","service":"api","level":"ERROR","message":"database timeout while creating order"}',
                    '{"timestamp":"2026-03-14T10:04:00+00:00","service":"api","level":"ERROR","message":"database timeout while creating order"}',
                ]
            ),
            encoding="utf-8",
        )
        deploys = root / "deploys.jsonl"
        deploys.write_text(
            '{"deployed_at":"2026-03-14T10:00:00+00:00","service":"api","version":"v2","environment":"prod","deployment_id":"deploy-api-9","commit_sha":"abc123","branch":"main","changed_files":["api/orders.py"]}',
            encoding="utf-8",
        )

        ingest_logs(logs, self.settings)
        ingest_deploys(deploys, self.settings)
        embed_logs_with_provider(self.settings, provider=FakeEmbeddingProvider())
        cluster_logs(self.settings, eps=0.1, min_samples=2)

        regressions = detect_regressions(self.settings, promote=True)

        self.assertEqual(len(regressions), 1)
        self.assertEqual(regressions[0].regression_type, "frequency_spike_after_deploy")
        self.assertEqual(regressions[0].before_count, 1)
        self.assertEqual(regressions[0].after_count, 3)
        incident, evidence = inspect_incident(self.settings, incident_id=1)
        self.assertIn("deploy-api-9", incident.summary)
        self.assertTrue(any(item.evidence_type == "changed_files" for item in evidence))

    def test_ingests_ci_failures_and_marks_flaky_pattern(self) -> None:
        path = Path(self.temp_dir.name) / "ci.jsonl"
        path.write_text(
            "\n".join(
                [
                    '{"workflow":"tests","test_name":"auth_flow_test","status":"failed","message":"auth_flow_test timeout","commit_sha":"aaa"}',
                    '{"workflow":"tests","test_name":"auth_flow_test","status":"passed","message":"auth_flow_test passed","commit_sha":"aab"}',
                    '{"workflow":"tests","test_name":"auth_flow_test","status":"failed","message":"auth_flow_test timeout","commit_sha":"aac"}',
                ]
            ),
            encoding="utf-8",
        )

        count, patterns = ingest_ci_failures(path, self.settings)

        self.assertEqual(count, 3)
        self.assertTrue(any(pattern.flaky for pattern in patterns))

    def test_fetches_failed_github_actions_jobs(self) -> None:
        original = ci._github_json

        def fake_github_json(url: str, token: str | None):
            if url.endswith("/jobs"):
                return {
                    "jobs": [
                        {
                            "id": 20,
                            "name": "auth_flow_test",
                            "conclusion": "failure",
                            "html_url": "https://github.test/job/20",
                            "logs_url": "https://github.test/job/20/logs",
                            "started_at": "2026-03-14T10:01:00Z",
                            "completed_at": "2026-03-14T10:02:00Z",
                        }
                    ]
                }
            return {
                "workflow_runs": [
                    {
                        "id": 10,
                        "name": "tests",
                        "conclusion": "failure",
                        "head_sha": "abc123",
                        "head_branch": "main",
                        "jobs_url": "https://api.github.test/jobs",
                        "html_url": "https://github.test/run/10",
                        "created_at": "2026-03-14T10:00:00Z",
                    },
                    {"id": 11, "name": "tests", "conclusion": "success"},
                ]
            }

        try:
            ci._github_json = fake_github_json
            records = ci.fetch_github_actions_failures("owner/repo", token=None, limit=5, api_base="https://api.github.test")
        finally:
            ci._github_json = original

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].raw["workflow"], "tests")
        self.assertEqual(records[0].raw["test_name"], "auth_flow_test")
        self.assertEqual(records[0].raw["commit_sha"], "abc123")

    def test_fetches_github_deployments_as_deploy_events(self) -> None:
        original = gitmeta._github_json

        def fake_github_json(url: str, token: str | None):
            if url.endswith("/pulls"):
                return []
            if "/commits/" in url:
                return {
                    "sha": "abc123",
                    "html_url": "https://github.test/commit/abc123",
                    "files": [{"filename": "api/orders.py"}],
                }
            return [
                {
                    "id": 44,
                    "sha": "abc123",
                    "ref": "main",
                    "environment": "prod",
                    "updated_at": "2026-03-14T10:00:00Z",
                    "url": "https://api.github.test/deployments/44",
                }
            ]

        try:
            gitmeta._github_json = fake_github_json
            events = gitmeta.fetch_github_deployments(
                repo="owner/repo",
                token=None,
                service="api",
                limit=1,
                environment="prod",
                api_base="https://api.github.test",
            )
        finally:
            gitmeta._github_json = original

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, "github-deployment-44")
        self.assertEqual(events[0].commit_sha, "abc123")
        self.assertEqual(events[0].changed_files, ["api/orders.py"])

    def test_evaluates_synthetic_benchmark_fixture(self) -> None:
        path = Path(self.temp_dir.name) / "benchmark.json"
        path.write_text(
            '{"cases":[{"root_cause":"orders.py timeout","trigger":"deploy-api-9","report":"orders.py timeout after deploy-api-9","predicted_trigger":"deploy-api-9","logs":["generic timeout"]}]}',
            encoding="utf-8",
        )

        result = evaluate_incident_benchmark(path)

        self.assertEqual(result.incident_count, 1)
        self.assertEqual(result.root_cause_top1, 1.0)
        self.assertEqual(result.root_cause_top3, 1.0)
        self.assertEqual(result.trigger_attribution, 1.0)


if __name__ == "__main__":
    unittest.main()
