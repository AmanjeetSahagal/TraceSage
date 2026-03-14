from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime
from typing import Any

import numpy as np
from sklearn.cluster import DBSCAN

from tracesage.domain import ClusterSnapshot

def cluster_embeddings(
    rows: list[tuple[str, list[float], datetime | None, str, str | None]],
    eps: float,
    min_samples: int,
) -> tuple[list[tuple[int, str]], list[ClusterSnapshot]]:
    if not rows:
        return [], []
    log_ids = [row[0] for row in rows]
    matrix = np.array([row[1] for row in rows], dtype=float)
    timestamps = [row[2] for row in rows]
    messages = [row[3] for row in rows]
    services = [row[4] for row in rows]
    labels = DBSCAN(eps=eps, min_samples=min_samples, metric="cosine").fit_predict(matrix)

    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    assignments: list[tuple[int, str]] = []
    for index, label in enumerate(labels):
        cluster_id = int(label)
        assignments.append((cluster_id, log_ids[index]))
        if cluster_id >= 0:
            grouped[cluster_id].append(
                {
                    "timestamp": timestamps[index],
                    "message": messages[index],
                    "service": services[index],
                    "vector": matrix[index].tolist(),
                    "log_id": log_ids[index],
                }
            )

    summaries: list[ClusterSnapshot] = []
    for cluster_id, items in sorted(grouped.items()):
        ordered_timestamps = sorted(item["timestamp"] for item in items if item["timestamp"] is not None)
        first_seen = ordered_timestamps[0] if ordered_timestamps else None
        last_seen = ordered_timestamps[-1] if ordered_timestamps else None
        example = items[0]["message"]
        service_names = sorted(
            {
                str(item["service"])
                for item in items
                if item["service"] not in (None, "")
            }
        )
        centroid = np.mean(np.array([item["vector"] for item in items], dtype=float), axis=0)
        cluster_key = _build_cluster_key(items)
        summaries.append(
            ClusterSnapshot(
                cluster_id=cluster_id,
                cluster_key=cluster_key,
                size=len(items),
                first_seen=first_seen,
                last_seen=last_seen,
                example_message=example,
                services=service_names,
                centroid=centroid.tolist(),
                log_ids=[str(item["log_id"]) for item in items],
            )
        )
    return assignments, summaries


def _build_cluster_key(items: list[dict[str, Any]]) -> str:
    service_names = sorted(
        {
            str(item["service"]).strip().lower()
            for item in items
            if item["service"] not in (None, "")
        }
    )
    message_signatures = sorted({_normalize_message_signature(str(item["message"])) for item in items})[:5]
    payload = json.dumps(
        {
            "services": service_names,
            "messages": message_signatures,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _normalize_message_signature(message: str) -> str:
    normalized = message.strip().lower()
    normalized = re.sub(r"\b\d+\b", "<num>", normalized)
    normalized = re.sub(r"0x[0-9a-f]+", "<hex>", normalized)
    normalized = re.sub(r"\b[0-9a-f]{8,}\b", "<id>", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized[:160]
