from __future__ import annotations

import sys
import time
from pathlib import Path

from tracesage.config import Settings
from tracesage.domain import AnomalyRecord, WatchResult
from tracesage.runtime.live import LiveProcessor
from tracesage.storage import TraceSageDB


def watch_file(
    settings: Settings,
    path: Path,
    service: str | None,
    poll_interval: float,
    eps: float,
    min_samples: int,
    min_growth: int,
    z_threshold: float,
    on_iteration: callable,
    on_anomaly: callable,
    max_cycles: int | None = None,
) -> None:
    db = TraceSageDB(settings.db_path)
    source = str(path.resolve())
    processor = LiveProcessor(
        settings=settings,
        eps=eps,
        min_samples=min_samples,
        min_growth=min_growth,
        z_threshold=z_threshold,
    )
    cycles = 0
    while True:
        lines, next_offset = _read_new_lines(path, db.fetch_watch_checkpoint(source))
        if lines:
            result, anomalies = processor.process(
                source=source,
                lines=lines,
                session_id=None,
                service=service,
            )
            db.store_watch_checkpoint(source, next_offset)
            on_iteration(result)
            for anomaly in anomalies:
                on_anomaly(anomaly)
        cycles += 1
        if max_cycles is not None and cycles >= max_cycles:
            return
        time.sleep(poll_interval)


def watch_stdin(
    settings: Settings,
    service: str | None,
    eps: float,
    min_samples: int,
    min_growth: int,
    z_threshold: float,
    on_iteration: callable,
    on_anomaly: callable,
) -> None:
    processor = LiveProcessor(
        settings=settings,
        eps=eps,
        min_samples=min_samples,
        min_growth=min_growth,
        z_threshold=z_threshold,
    )
    lines = [(index, line) for index, line in enumerate(sys.stdin, start=1) if line.strip()]
    if not lines:
        return
    result, anomalies = processor.process(
        source="stdin",
        lines=lines,
        session_id=None,
        service=service,
    )
    on_iteration(result)
    for anomaly in anomalies:
        on_anomaly(anomaly)


def _read_new_lines(path: Path, offset: int) -> tuple[list[tuple[int, str]], int]:
    if not path.exists():
        raise FileNotFoundError(f"Watch source does not exist: {path}")
    current_size = path.stat().st_size
    if offset > current_size:
        offset = 0
    with path.open("r", encoding="utf-8") as handle:
        handle.seek(offset)
        lines: list[tuple[int, str]] = []
        while True:
            position = handle.tell()
            line = handle.readline()
            if not line:
                break
            lines.append((position, line))
        next_offset = handle.tell()
    return lines, next_offset
