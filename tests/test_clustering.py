from __future__ import annotations

import unittest
from datetime import UTC, datetime

from tracesage.ml.clustering import cluster_embeddings


class ClusterTrackingTests(unittest.TestCase):
    def test_cluster_key_stays_stable_for_numeric_message_variants(self) -> None:
        timestamp = datetime.now(UTC)
        first_rows = [
            ("a1", [1.0, 0.0], timestamp, "api timeout on shard 1", "api"),
            ("a2", [1.0, 0.0], timestamp, "api timeout on shard 2", "api"),
        ]
        second_rows = [
            ("b1", [1.0, 0.0], timestamp, "api timeout on shard 3", "api"),
            ("b2", [1.0, 0.0], timestamp, "api timeout on shard 4", "api"),
        ]

        _assignments, first_summaries = cluster_embeddings(first_rows, eps=0.1, min_samples=2)
        _assignments, second_summaries = cluster_embeddings(second_rows, eps=0.1, min_samples=2)

        self.assertEqual(len(first_summaries), 1)
        self.assertEqual(len(second_summaries), 1)
        self.assertEqual(first_summaries[0].cluster_key, second_summaries[0].cluster_key)


if __name__ == "__main__":
    unittest.main()
