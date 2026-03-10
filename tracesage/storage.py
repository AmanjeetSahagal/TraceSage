from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable

import duckdb

from tracesage.domain import ClusterSnapshot, DeployEvent, IncidentSummary, LogRecord


class TraceSageDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(str(self.db_path))
        self._init_schema(conn)
        return conn

    def _init_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id TEXT PRIMARY KEY,
                timestamp TIMESTAMP,
                service TEXT,
                level TEXT,
                message TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                cluster_id INTEGER,
                embedding_status TEXT DEFAULT 'pending'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS embeddings (
                log_id TEXT PRIMARY KEY,
                model_name TEXT NOT NULL,
                vector_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS clusters (
                cluster_id INTEGER PRIMARY KEY,
                cluster_key TEXT NOT NULL,
                log_count INTEGER NOT NULL,
                first_seen TIMESTAMP,
                last_seen TIMESTAMP,
                example_message TEXT NOT NULL,
                services_json TEXT NOT NULL DEFAULT '[]',
                centroid_json TEXT NOT NULL DEFAULT '[]',
                log_ids_json TEXT NOT NULL DEFAULT '[]',
                latest_run_id BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cluster_runs (
                run_id BIGINT PRIMARY KEY,
                eps DOUBLE NOT NULL,
                min_samples INTEGER NOT NULL,
                total_logs INTEGER NOT NULL,
                noise_count INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cluster_history (
                run_id BIGINT NOT NULL,
                cluster_id INTEGER NOT NULL,
                cluster_key TEXT NOT NULL,
                log_count INTEGER NOT NULL,
                first_seen TIMESTAMP,
                last_seen TIMESTAMP,
                example_message TEXT NOT NULL,
                services_json TEXT NOT NULL,
                centroid_json TEXT NOT NULL,
                log_ids_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS incident_summaries (
                cluster_key TEXT PRIMARY KEY,
                cluster_id INTEGER NOT NULL,
                summary_json TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS deploy_events (
                id TEXT PRIMARY KEY,
                deployed_at TIMESTAMP NOT NULL,
                service TEXT NOT NULL,
                version TEXT,
                environment TEXT,
                raw_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute("ALTER TABLE clusters ADD COLUMN IF NOT EXISTS cluster_key TEXT")
        conn.execute("ALTER TABLE clusters ADD COLUMN IF NOT EXISTS services_json TEXT DEFAULT '[]'")
        conn.execute("ALTER TABLE clusters ADD COLUMN IF NOT EXISTS centroid_json TEXT DEFAULT '[]'")
        conn.execute("ALTER TABLE clusters ADD COLUMN IF NOT EXISTS log_ids_json TEXT DEFAULT '[]'")
        conn.execute("ALTER TABLE clusters ADD COLUMN IF NOT EXISTS latest_run_id BIGINT")

    def upsert_logs(self, records: Iterable[LogRecord]) -> int:
        conn = self.connect()
        payload = [
            [
                record.id,
                record.timestamp,
                record.service,
                record.level,
                record.message,
                json.dumps(record.raw),
            ]
            for record in records
        ]
        if payload:
            conn.executemany(
                """
                INSERT INTO logs (id, timestamp, service, level, message, raw_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    timestamp = excluded.timestamp,
                    service = excluded.service,
                    level = excluded.level,
                    message = excluded.message,
                    raw_json = excluded.raw_json
                """,
                payload,
            )
        conn.close()
        return len(payload)

    def upsert_deploy_events(self, records: Iterable[DeployEvent]) -> int:
        conn = self.connect()
        payload = [
            [
                record.id,
                record.deployed_at,
                record.service,
                record.version,
                record.environment,
                json.dumps(record.raw),
            ]
            for record in records
        ]
        if payload:
            conn.executemany(
                """
                INSERT INTO deploy_events (id, deployed_at, service, version, environment, raw_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    deployed_at = excluded.deployed_at,
                    service = excluded.service,
                    version = excluded.version,
                    environment = excluded.environment,
                    raw_json = excluded.raw_json
                """,
                payload,
            )
        conn.close()
        return len(payload)

    def fetch_logs_missing_embeddings(self) -> list[tuple[str, str]]:
        conn = self.connect()
        rows = conn.execute(
            """
            SELECT id, message
            FROM logs
            WHERE embedding_status = 'pending'
            ORDER BY timestamp NULLS LAST, id
            """
        ).fetchall()
        conn.close()
        return [(row[0], row[1]) for row in rows]

    def store_embeddings(
        self, embeddings: list[tuple[str, str, list[float]]]
    ) -> int:
        conn = self.connect()
        embedding_payload = [
            [log_id, model_name, json.dumps(vector)]
            for log_id, model_name, vector in embeddings
        ]
        if embedding_payload:
            conn.executemany(
                """
                INSERT INTO embeddings (log_id, model_name, vector_json)
                VALUES (?, ?, ?)
                ON CONFLICT(log_id) DO UPDATE SET
                    model_name = excluded.model_name,
                    vector_json = excluded.vector_json
                """,
                embedding_payload,
            )
            conn.executemany(
                "UPDATE logs SET embedding_status = 'complete' WHERE id = ?",
                [[log_id] for log_id, _model_name, _vector in embeddings],
            )
        conn.close()
        return len(embeddings)

    def fetch_embedding_matrix(self) -> list[tuple[str, str, str | None, str | None, str | None]]:
        conn = self.connect()
        rows = conn.execute(
            """
            SELECT logs.id, embeddings.vector_json, logs.timestamp, logs.message, logs.service
            FROM logs
            INNER JOIN embeddings ON logs.id = embeddings.log_id
            ORDER BY logs.timestamp NULLS LAST, logs.id
            """
        ).fetchall()
        conn.close()
        return [(row[0], row[1], row[2], row[3], row[4]) for row in rows]

    def replace_clusters(
        self,
        assignments: list[tuple[int, str]],
        summaries: list[ClusterSnapshot],
        eps: float,
        min_samples: int,
        noise_count: int,
    ) -> int:
        conn = self.connect()
        next_run_id = (
            conn.execute("SELECT COALESCE(MAX(run_id), 0) + 1 FROM cluster_runs").fetchone()[0]
        )
        conn.execute(
            """
            INSERT INTO cluster_runs (run_id, eps, min_samples, total_logs, noise_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            [next_run_id, eps, min_samples, len(assignments), noise_count],
        )
        conn.execute("UPDATE logs SET cluster_id = NULL")
        conn.execute("DELETE FROM clusters")
        for cluster_id, log_id in assignments:
            conn.execute("UPDATE logs SET cluster_id = ? WHERE id = ?", [cluster_id, log_id])
        for snapshot in summaries:
            conn.execute(
                """
                INSERT INTO clusters (
                    cluster_id,
                    cluster_key,
                    log_count,
                    first_seen,
                    last_seen,
                    example_message,
                    services_json,
                    centroid_json,
                    log_ids_json,
                    latest_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    snapshot.cluster_id,
                    snapshot.cluster_key,
                    snapshot.size,
                    snapshot.first_seen,
                    snapshot.last_seen,
                    snapshot.example_message,
                    json.dumps(snapshot.services),
                    json.dumps(snapshot.centroid),
                    json.dumps(snapshot.log_ids),
                    next_run_id,
                ],
            )
            conn.execute(
                """
                INSERT INTO cluster_history (
                    run_id,
                    cluster_id,
                    cluster_key,
                    log_count,
                    first_seen,
                    last_seen,
                    example_message,
                    services_json,
                    centroid_json,
                    log_ids_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    next_run_id,
                    snapshot.cluster_id,
                    snapshot.cluster_key,
                    snapshot.size,
                    snapshot.first_seen,
                    snapshot.last_seen,
                    snapshot.example_message,
                    json.dumps(snapshot.services),
                    json.dumps(snapshot.centroid),
                    json.dumps(snapshot.log_ids),
                ],
            )
        conn.close()
        return int(next_run_id)

    def fetch_cluster_summaries(self) -> list[tuple[int, str, int, str | None, str | None, str]]:
        conn = self.connect()
        rows = conn.execute(
            """
            SELECT cluster_id, cluster_key, log_count, first_seen, last_seen, example_message
            FROM clusters
            ORDER BY log_count DESC, cluster_id ASC
            """
        ).fetchall()
        conn.close()
        return rows

    def fetch_cluster_history(self) -> list[tuple[int, int, str, int, str | None, str | None, str]]:
        conn = self.connect()
        rows = conn.execute(
            """
            SELECT run_id, cluster_id, cluster_key, log_count, first_seen, last_seen, example_message
            FROM cluster_history
            ORDER BY run_id ASC, cluster_id ASC
            """
        ).fetchall()
        conn.close()
        return rows

    def fetch_cluster_history_by_run(
        self,
        run_id: int,
    ) -> list[tuple[int, str, int, str | None, str | None, str]]:
        conn = self.connect()
        rows = conn.execute(
            """
            SELECT cluster_id, cluster_key, log_count, first_seen, last_seen, example_message
            FROM cluster_history
            WHERE run_id = ?
            ORDER BY log_count DESC, cluster_id ASC
            """,
            [run_id],
        ).fetchall()
        conn.close()
        return rows

    def fetch_latest_run_ids(self) -> tuple[int | None, int | None]:
        conn = self.connect()
        latest = conn.execute(
            "SELECT run_id FROM cluster_runs ORDER BY run_id DESC LIMIT 2"
        ).fetchall()
        conn.close()
        latest_run = latest[0][0] if len(latest) >= 1 else None
        previous_run = latest[1][0] if len(latest) >= 2 else None
        return latest_run, previous_run

    def fetch_cluster_detail(
        self,
        cluster_id: int,
    ) -> tuple[int, str, int, str | None, str | None, str, str, str] | None:
        conn = self.connect()
        row = conn.execute(
            """
            SELECT
                cluster_id,
                cluster_key,
                log_count,
                first_seen,
                last_seen,
                example_message,
                services_json,
                log_ids_json
            FROM clusters
            WHERE cluster_id = ?
            """,
            [cluster_id],
        ).fetchone()
        conn.close()
        return row

    def fetch_logs_for_cluster(self, cluster_id: int) -> list[tuple[str | None, str | None, str | None, str]]:
        conn = self.connect()
        rows = conn.execute(
            """
            SELECT timestamp, service, level, message
            FROM logs
            WHERE cluster_id = ?
            ORDER BY timestamp NULLS LAST, id
            """,
            [cluster_id],
        ).fetchall()
        conn.close()
        return rows

    def fetch_deploy_events_for_services(
        self,
        services: list[str],
        window_start: datetime | None,
        window_end: datetime | None,
    ) -> list[tuple[str, str, str | None, str | None, str]]:
        if not services or window_start is None or window_end is None:
            return []
        conn = self.connect()
        placeholders = ", ".join(["?"] * len(services))
        rows = conn.execute(
            f"""
            SELECT id, service, version, environment, deployed_at
            FROM deploy_events
            WHERE service IN ({placeholders})
              AND deployed_at BETWEEN ? AND ?
            ORDER BY deployed_at ASC
            """,
            [*services, window_start, window_end],
        ).fetchall()
        conn.close()
        return rows

    def store_incident_summary(self, summary: IncidentSummary) -> None:
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO incident_summaries (cluster_key, cluster_id, summary_json, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(cluster_key) DO UPDATE SET
                cluster_id = excluded.cluster_id,
                summary_json = excluded.summary_json
            """,
            [
                summary.cluster_key,
                summary.cluster_id,
                json.dumps(summary.__dict__),
            ],
        )
        conn.close()

    def fetch_incident_summary(self, cluster_id: int) -> str | None:
        conn = self.connect()
        row = conn.execute(
            """
            SELECT incident_summaries.summary_json
            FROM incident_summaries
            INNER JOIN clusters ON incident_summaries.cluster_key = clusters.cluster_key
            WHERE clusters.cluster_id = ?
            """,
            [cluster_id],
        ).fetchone()
        conn.close()
        return row[0] if row else None
