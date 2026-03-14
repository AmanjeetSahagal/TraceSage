from __future__ import annotations

import unittest

from tracesage.ingest import normalize_live_line


class NormalizeLiveLineTests(unittest.TestCase):
    def test_infers_service_from_message_prefix(self) -> None:
        record = normalize_live_line(
            line="[api] database connection timeout while creating order",
            source="stdin",
            offset=1,
        )

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.service, "api")

    def test_explicit_service_wins_over_inference(self) -> None:
        record = normalize_live_line(
            line="service=worker retry exhausted",
            source="stdin",
            offset=2,
            service="api",
        )

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.service, "api")


if __name__ == "__main__":
    unittest.main()
