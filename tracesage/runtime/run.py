from __future__ import annotations

import selectors
import subprocess
import time

from tracesage.config import Settings
from tracesage.runtime.live import LiveProcessor
from tracesage.pipeline import (
    complete_run_session,
    create_run_session,
)


def run_command(
    settings: Settings,
    command: list[str],
    eps: float,
    min_samples: int,
    min_growth: int,
    z_threshold: float,
    flush_interval: float,
    max_batch_lines: int,
    on_iteration: callable,
    on_anomaly: callable,
) -> int:
    if not command:
        raise ValueError("A command is required after `tracesage run --`.")
    session_id = create_run_session(settings, command)
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    selector = selectors.DefaultSelector()
    assert process.stdout is not None
    assert process.stderr is not None
    selector.register(process.stdout, selectors.EVENT_READ, data="stdout")
    selector.register(process.stderr, selectors.EVENT_READ, data="stderr")
    processor = LiveProcessor(
        settings=settings,
        eps=eps,
        min_samples=min_samples,
        min_growth=min_growth,
        z_threshold=z_threshold,
    )

    sequence = 0
    pending_lines: list[tuple[str, int, str]] = []
    last_flush = time.monotonic()
    while selector.get_map():
        events = selector.select(timeout=flush_interval)
        for key, _mask in events:
            line = key.fileobj.readline()
            if not line:
                selector.unregister(key.fileobj)
                continue
            sequence += 1
            pending_lines.append((key.data, sequence, line))
        should_flush = (
            pending_lines
            and (
                len(pending_lines) >= max_batch_lines
                or (time.monotonic() - last_flush) >= flush_interval
                or not selector.get_map()
            )
        )
        if should_flush:
            _flush_pending(
                processor=processor,
                session_id=session_id,
                pending_lines=pending_lines,
                on_iteration=on_iteration,
                on_anomaly=on_anomaly,
            )
            pending_lines = []
            last_flush = time.monotonic()

    if pending_lines:
        _flush_pending(
            processor=processor,
            session_id=session_id,
            pending_lines=pending_lines,
            on_iteration=on_iteration,
            on_anomaly=on_anomaly,
        )

    exit_code = process.wait()
    complete_run_session(settings, session_id=session_id, exit_code=exit_code)
    return exit_code


def _flush_pending(
    processor: LiveProcessor,
    session_id: int,
    pending_lines: list[tuple[str, int, str]],
    on_iteration: callable,
    on_anomaly: callable,
) -> None:
    grouped: dict[str, list[tuple[int, str]]] = {}
    for stream_name, sequence, line in pending_lines:
        source = f"session:{session_id}:{stream_name}"
        grouped.setdefault(source, []).append((sequence, line))
    for source, lines in grouped.items():
        result, anomalies = processor.process(
            source=source,
            lines=lines,
            session_id=session_id,
        )
        if result.ingested_logs:
            on_iteration(result)
        for anomaly in anomalies:
            on_anomaly(anomaly)
