from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tracesage.config import Settings
from tracesage.pipeline import (
    benchmark_pipeline,
    export_incident_report,
    inspect_incident,
    list_incidents,
    process_live_iteration,
    set_incident_status,
)


class FakeEmbeddingProvider:
    def embed(self, texts: list[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for text in texts:
            lowered = text.lower()
            if "timeout" in lowered:
                vectors.append([1.0, 0.0])
            elif "redis" in lowered:
                vectors.append([0.0, 1.0])
            else:
                vectors.append([0.5, 0.5])
        return vectors


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.settings = Settings(
            db_path=root / "tracesage.duckdb",
            hf_cache_dir=root / "hf-cache",
            export_dir=root / "reports",
        )
        self.provider = FakeEmbeddingProvider()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_live_pipeline_promotes_and_reopens_incident(self) -> None:
        first = process_live_iteration(
            settings=self.settings,
            source="stdin",
            lines=[
                (1, "[api] database timeout while creating order"),
                (2, "[api] database timeout while creating order"),
            ],
            eps=0.1,
            min_samples=2,
            min_growth=2,
            z_threshold=2.0,
            provider=self.provider,
        )
        self.assertEqual(first[0].anomaly_count, 0)

        second = process_live_iteration(
            settings=self.settings,
            source="stdin",
            lines=[
                (3, "[api] database timeout while creating order"),
                (4, "[api] database timeout while creating order"),
            ],
            eps=0.1,
            min_samples=2,
            min_growth=2,
            z_threshold=2.0,
            provider=self.provider,
        )
        self.assertGreaterEqual(second[0].anomaly_count, 1)

        incidents = list_incidents(self.settings)
        self.assertEqual(len(incidents), 1)
        self.assertEqual(incidents[0].status, "open")

        resolved = set_incident_status(self.settings, incident_id=1, status="resolved")
        self.assertEqual(resolved.status, "resolved")

        process_live_iteration(
            settings=self.settings,
            source="stdin",
            lines=[
                (5, "[api] database timeout while creating order"),
                (6, "[api] database timeout while creating order"),
            ],
            eps=0.1,
            min_samples=2,
            min_growth=2,
            z_threshold=2.0,
            provider=self.provider,
        )

        reopened, evidence = inspect_incident(self.settings, incident_id=1)
        self.assertEqual(reopened.status, "regressed")
        self.assertTrue(any(item.evidence_type == "status_change" for item in evidence))

    def test_export_incident_report_writes_markdown(self) -> None:
        process_live_iteration(
            settings=self.settings,
            source="stdin",
            lines=[
                (1, "service=api database timeout while creating order"),
                (2, "service=api database timeout while creating order"),
            ],
            eps=0.1,
            min_samples=2,
            min_growth=2,
            z_threshold=2.0,
            provider=self.provider,
        )
        process_live_iteration(
            settings=self.settings,
            source="stdin",
            lines=[
                (3, "service=api database timeout while creating order"),
                (4, "service=api database timeout while creating order"),
            ],
            eps=0.1,
            min_samples=2,
            min_growth=2,
            z_threshold=2.0,
            provider=self.provider,
        )

        output_path = export_incident_report(self.settings, incident_id=1)

        self.assertTrue(output_path.exists())
        report = output_path.read_text(encoding="utf-8")
        self.assertIn("# TraceSage Incident Report", report)
        self.assertIn("Incident ID: 1", report)

    def test_benchmark_pipeline_reports_throughput(self) -> None:
        log_path = Path(self.temp_dir.name) / "logs.jsonl"
        log_path.write_text(
            "\n".join(
                [
                    '{"timestamp":"2026-03-14T10:00:00+00:00","service":"api","message":"database timeout while creating order"}',
                    '{"timestamp":"2026-03-14T10:00:01+00:00","service":"api","message":"database timeout while creating order"}',
                    '{"timestamp":"2026-03-14T10:00:02+00:00","service":"worker","message":"redis reset while dispatching job"}',
                ]
            ),
            encoding="utf-8",
        )

        result = benchmark_pipeline(
            settings=self.settings,
            log_path=log_path,
            eps=0.1,
            min_samples=2,
            provider=self.provider,
        )

        self.assertEqual(result.ingested_logs, 3)
        self.assertGreater(result.ingest_logs_per_second, 0.0)
        self.assertGreater(result.embed_logs_per_second, 0.0)
        self.assertGreater(result.cluster_logs_per_second, 0.0)


if __name__ == "__main__":
    unittest.main()
