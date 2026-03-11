from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from tracesage.config import get_settings
from tracesage.domain import AnomalyRecord, WatchResult
from tracesage.pipeline import (
    benchmark_pipeline,
    cluster_logs,
    detect_anomalies,
    embed_logs,
    explain_incident,
    export_cluster_report,
    ingest_deploys,
    ingest_logs,
    inspect_incident,
    list_incidents,
    set_incident_status,
    summarize_cluster,
)
from tracesage.runtime.run import run_command
from tracesage.runtime.watch import watch_file

app = typer.Typer(
    help=(
        "TraceSage turns raw logs into semantic incident signals.\n\n"
        "Typical workflow:\n"
        "1. ingest a log file into DuckDB\n"
        "2. embed messages with a Hugging Face model\n"
        "3. cluster related failures into issue patterns\n"
        "4. detect unusual cluster growth or novel clusters\n"
        "5. summarize or export one cluster into an incident-style report\n"
        "6. watch a live log file and react as new errors appear\n"
        "7. run a development command under live observation\n"
        "8. review, explain, and manage promoted incidents"
    ),
    no_args_is_help=True,
    rich_markup_mode="rich",
)
console = Console()


@app.command()
def ingest(path: Path) -> None:
    """Ingest a log file and normalize records into the local TraceSage database."""
    settings = get_settings()
    try:
        count = ingest_logs(path, settings)
    except Exception as exc:
        console.print(f"[red]Ingest failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    console.print(f"Ingested [bold]{count}[/bold] logs into {settings.db_path}")


@app.command()
def embed() -> None:
    """Generate semantic embeddings for logs that do not have vectors yet."""
    settings = get_settings()
    try:
        count = embed_logs(settings)
    except Exception as exc:
        console.print(f"[red]Embedding failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    if count == 0:
        console.print("No pending logs to embed.")
        return
    console.print(
        f"Generated [bold]{count}[/bold] embeddings with {settings.embedding_model}"
    )


@app.command()
def deploys(path: Path) -> None:
    """Ingest deploy events so clusters can be correlated with recent releases."""
    settings = get_settings()
    try:
        count = ingest_deploys(path, settings)
    except Exception as exc:
        console.print(f"[red]Deploy ingestion failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    console.print(f"Ingested [bold]{count}[/bold] deploy events into {settings.db_path}")


@app.command()
def watch(
    file: Path = typer.Option(..., "--file", help="Log file to tail continuously."),
    poll_interval: float = typer.Option(2.0, help="Seconds between reads of the watched file."),
    eps: float = typer.Option(0.5, help="Clustering distance threshold for live processing."),
    min_samples: int = typer.Option(2, help="Minimum samples required to form a cluster."),
    min_growth: int = typer.Option(2, help="Minimum cluster growth before alerting."),
    z_threshold: float = typer.Option(2.0, help="Z-score threshold for rare spike alerts."),
    max_cycles: int | None = typer.Option(
        None,
        help="Optional limit for polling cycles. Useful for testing watch mode.",
    ),
) -> None:
    """Tail a log file, ingest new lines, and alert on new or growing issue patterns."""
    settings = get_settings()

    def _show_iteration(result: WatchResult) -> None:
        console.print(
            f"Processed {result.ingested_logs} new logs, embedded {result.embedded_logs}, "
            f"cluster run {result.cluster_run_id or '-'}."
        )

    def _show_anomaly(anomaly: AnomalyRecord) -> None:
        console.print(
            Panel(
                (
                    f"{anomaly.anomaly_type} in cluster {anomaly.cluster_id}\n"
                    f"Current: {anomaly.current_size} | Previous: {anomaly.previous_size} | Delta: {anomaly.delta}\n"
                    f"{anomaly.reason}\n"
                    f"Example: {anomaly.example_message}"
                ),
                title=f"Live Alert [{anomaly.severity}]",
            )
        )

    console.print(f"Watching [bold]{file}[/bold] for new logs. Press Ctrl+C to stop.")
    try:
        watch_file(
            settings=settings,
            path=file,
            poll_interval=poll_interval,
            eps=eps,
            min_samples=min_samples,
            min_growth=min_growth,
            z_threshold=z_threshold,
            on_iteration=_show_iteration,
            on_anomaly=_show_anomaly,
            max_cycles=max_cycles,
        )
    except KeyboardInterrupt:
        console.print("Stopped watch mode.")
    except Exception as exc:
        console.print(f"[red]Watch failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc


@app.command(
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True,
    }
)
def run(
    ctx: typer.Context,
    eps: float = typer.Option(0.5, help="Clustering distance threshold for live processing."),
    min_samples: int = typer.Option(2, help="Minimum samples required to form a cluster."),
    min_growth: int = typer.Option(2, help="Minimum cluster growth before alerting."),
    z_threshold: float = typer.Option(2.0, help="Z-score threshold for rare spike alerts."),
    flush_interval: float = typer.Option(
        1.0,
        help="Seconds between live processing flushes while the command is running.",
    ),
    max_batch_lines: int = typer.Option(
        20,
        help="Maximum buffered lines before forcing a live processing flush.",
    ),
) -> None:
    """Run a command under TraceSage observation and analyze stdout/stderr live."""
    settings = get_settings()
    command = list(ctx.args)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        console.print("[red]Run failed:[/red] provide a command after `tracesage run --`.")
        raise typer.Exit(code=1)

    def _show_iteration(result: WatchResult) -> None:
        console.print(
            f"Processed {result.ingested_logs} new logs, embedded {result.embedded_logs}, "
            f"cluster run {result.cluster_run_id or '-'}."
        )

    def _show_anomaly(anomaly: AnomalyRecord) -> None:
        console.print(
            Panel(
                (
                    f"{anomaly.anomaly_type} in cluster {anomaly.cluster_id}\n"
                    f"Current: {anomaly.current_size} | Previous: {anomaly.previous_size} | Delta: {anomaly.delta}\n"
                    f"{anomaly.reason}\n"
                    f"Example: {anomaly.example_message}"
                ),
                title=f"Live Alert [{anomaly.severity}]",
            )
        )

    console.print(f"Running under TraceSage observation: [bold]{' '.join(command)}[/bold]")
    try:
        exit_code = run_command(
            settings=settings,
            command=command,
            eps=eps,
            min_samples=min_samples,
            min_growth=min_growth,
            z_threshold=z_threshold,
            flush_interval=flush_interval,
            max_batch_lines=max_batch_lines,
            on_iteration=_show_iteration,
            on_anomaly=_show_anomaly,
        )
    except Exception as exc:
        console.print(f"[red]Run failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    raise typer.Exit(code=exit_code)


@app.command()
def cluster(
    eps: float = typer.Option(
        0.3,
        help="Cosine distance threshold. Larger values create broader clusters.",
    ),
    min_samples: int = typer.Option(
        3,
        help="Minimum nearby logs required before a group becomes a cluster.",
    ),
) -> None:
    """Group semantically similar logs into recurring issue patterns."""
    settings = get_settings()
    try:
        has_embeddings, noise_count, run_id, summaries = cluster_logs(
            settings,
            eps=eps,
            min_samples=min_samples,
        )
    except Exception as exc:
        console.print(f"[red]Clustering failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    if not has_embeddings:
        console.print("No embeddings available. Run `tracesage embed` first.")
        return
    if not summaries:
        console.print(
            f"No dense clusters found with eps={eps} and min_samples={min_samples}. "
            f"{noise_count} logs were labeled as noise."
        )
        return
    table = Table(title="Cluster Summary")
    table.add_column("Cluster ID")
    table.add_column("Cluster Key")
    table.add_column("Logs")
    table.add_column("First Seen")
    table.add_column("Last Seen")
    table.add_column("Example")
    for cluster_id, cluster_key, size, first_seen, last_seen, example in summaries:
        table.add_row(
            str(cluster_id),
            cluster_key,
            str(size),
            str(first_seen or "-"),
            str(last_seen or "-"),
            example[:80],
        )
    console.print(table)
    console.print(f"Cluster run: [bold]{run_id}[/bold]")
    if noise_count:
        console.print(f"Noise logs: [bold]{noise_count}[/bold]")


@app.command()
def anomaly(
    min_growth: int = typer.Option(
        2,
        help="Minimum increase in cluster size between runs before flagging growth.",
    ),
    z_threshold: float = typer.Option(
        2.0,
        help="Statistical spike threshold compared with that cluster's prior history.",
    ),
) -> None:
    """Detect novel clusters and unusual growth across clustering snapshots."""
    settings = get_settings()
    try:
        anomalies = detect_anomalies(settings, min_growth=min_growth, z_threshold=z_threshold)
    except Exception as exc:
        console.print(f"[red]Anomaly detection failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    if not anomalies:
        console.print("No anomalies detected. Run clustering at least twice with changing data.")
        return
    table = Table(title="Anomaly Summary")
    table.add_column("Type")
    table.add_column("Severity")
    table.add_column("Cluster")
    table.add_column("Current")
    table.add_column("Previous")
    table.add_column("Delta")
    table.add_column("Z-Score")
    table.add_column("Reason")
    for item in anomalies:
        table.add_row(
            item.anomaly_type,
            item.severity,
            f"{item.cluster_id} / {item.cluster_key}",
            str(item.current_size),
            str(item.previous_size),
            str(item.delta),
            f"{item.z_score:.2f}",
            item.reason,
        )
    console.print(table)


@app.command()
def incidents() -> None:
    """List incidents promoted from live anomalies."""
    settings = get_settings()
    try:
        rows = list_incidents(settings)
    except Exception as exc:
        console.print(f"[red]Incident listing failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    if not rows:
        console.print("No incidents have been promoted yet.")
        return
    table = Table(title="Incidents")
    table.add_column("Incident")
    table.add_column("Severity")
    table.add_column("Status")
    table.add_column("Cluster")
    table.add_column("Current Size")
    table.add_column("Last Seen")
    table.add_column("Title")
    for row in rows:
        table.add_row(
            str(row.incident_id),
            row.severity,
            row.status,
            str(row.cluster_id),
            str(row.current_size),
            str(row.last_seen or "-"),
            row.title,
        )
    console.print(table)


@app.command()
def inspect(
    incident: int = typer.Option(..., "--incident", help="Incident ID to inspect."),
) -> None:
    """Inspect one incident with its evidence trail."""
    settings = get_settings()
    try:
        record, evidence = inspect_incident(settings, incident_id=incident)
    except Exception as exc:
        console.print(f"[red]Incident inspection failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    lines = [
        f"Title: {record.title}",
        f"Severity: {record.severity}",
        f"Status: {record.status}",
        f"Cluster ID: {record.cluster_id}",
        f"Cluster Key: {record.cluster_key}",
        f"Current Size: {record.current_size}",
        f"Confidence: {record.confidence:.2f}",
        f"First Seen: {record.first_seen or '-'}",
        f"Last Seen: {record.last_seen or '-'}",
        "",
        "Summary:",
        record.summary,
        "",
        "Evidence:",
        *(f"- [{item.evidence_type}] {item.details}" for item in evidence),
    ]
    console.print(Panel("\n".join(lines), title=f"Incident {record.incident_id}"))


@app.command()
def explain(
    incident: int = typer.Option(..., "--incident", help="Incident ID to explain."),
) -> None:
    """Explain an incident with representative logs, related sessions, and correlated deploys."""
    settings = get_settings()
    try:
        result = explain_incident(settings, incident_id=incident)
    except Exception as exc:
        console.print(f"[red]Incident explain failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    lines = [
        f"Title: {result.incident.title}",
        f"Severity: {result.incident.severity}",
        f"Status: {result.incident.status}",
        f"Summary: {result.incident.summary}",
        f"Confidence: {result.incident.confidence:.2f}",
        "",
        "Representative Logs:",
        *(f"- {item}" for item in result.representative_logs),
        "",
        "Related Sessions:",
        *(f"- {item}" for item in (result.related_sessions or ["No related sessions captured."])),
        "",
        "Deploy Correlation:",
        *(f"- {item}" for item in (result.deploy_correlation or ["No correlated deploy events found."])),
    ]
    console.print(Panel("\n".join(lines), title=f"Incident {result.incident.incident_id} Explanation"))


@app.command()
def ack(
    incident: int = typer.Option(..., "--incident", help="Incident ID to acknowledge."),
) -> None:
    """Acknowledge an incident without resolving it."""
    settings = get_settings()
    try:
        record = set_incident_status(settings, incident_id=incident, status="acknowledged")
    except Exception as exc:
        console.print(f"[red]Incident acknowledge failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    console.print(f"Incident {record.incident_id} marked as {record.status}.")


@app.command()
def resolve(
    incident: int = typer.Option(..., "--incident", help="Incident ID to resolve."),
) -> None:
    """Resolve an incident and remove it from future active promotion."""
    settings = get_settings()
    try:
        record = set_incident_status(settings, incident_id=incident, status="resolved")
    except Exception as exc:
        console.print(f"[red]Incident resolve failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    console.print(f"Incident {record.incident_id} marked as {record.status}.")


@app.command()
def summarize(
    cluster: int = typer.Option(
        ...,
        "--cluster",
        help="Cluster ID from the latest `tracesage cluster` run.",
    ),
    provider: str = typer.Option(
        None,
        help=(
            "Summary provider to use. `template` is deterministic and local. "
            "`huggingface` uses an instruction-tuned model for a richer explanation."
        ),
    ),
) -> None:
    """Explain one cluster as an incident-style summary with timeline and likely cause."""
    settings = get_settings()
    try:
        summary = summarize_cluster(
            settings,
            cluster_id=cluster,
            provider_name=provider or settings.summary_provider,
        )
    except Exception as exc:
        console.print(f"[red]Summarization failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    console.print(
        Panel(
            "\n".join(
                [
                    summary.description,
                    "",
                    f"Affected services: {', '.join(summary.affected_services) or 'unknown'}",
                    f"Confidence: {summary.confidence:.2f}",
                    "",
                    "Timeline:",
                    *summary.timeline,
                    "",
                    "Representative logs:",
                    *summary.representative_logs,
                    "",
                    f"Suspected root cause: {summary.suspected_root_cause}",
                    "",
                    "Deploy correlation:",
                    *(summary.deploy_correlation or ["No correlated deploy events found."]),
                ]
            ),
            title=f"Cluster {summary.cluster_id} Summary",
        )
    )


@app.command()
def export(
    cluster: int = typer.Option(
        ...,
        "--cluster",
        help="Cluster ID from the latest `tracesage cluster` run.",
    ),
    format: str = typer.Option(
        "md",
        "--format",
        help="Export format. Phase 3 currently supports `md`.",
    ),
    output: Path | None = typer.Option(
        None,
        "--output",
        help="Optional output path. Defaults to reports/cluster-<id>.md.",
    ),
    provider: str = typer.Option(
        None,
        help="Summary provider to use before export: template or huggingface.",
    ),
) -> None:
    """Export a cluster summary as a Markdown incident report."""
    if format != "md":
        console.print("[red]Export failed:[/red] Only `md` is supported right now.")
        raise typer.Exit(code=1)
    settings = get_settings()
    try:
        path = export_cluster_report(
            settings,
            cluster_id=cluster,
            output_path=output,
            provider_name=provider,
        )
    except Exception as exc:
        console.print(f"[red]Export failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    console.print(f"Exported report to [bold]{path}[/bold]")


@app.command()
def benchmark(
    path: Path = typer.Option(..., "--path", help="Log file to benchmark."),
    eps: float = typer.Option(0.5, help="Clustering distance threshold for the benchmark run."),
    min_samples: int = typer.Option(2, help="Minimum samples for the benchmark clustering run."),
) -> None:
    """Benchmark ingest, embed, and cluster timings for a local dataset."""
    settings = get_settings()
    try:
        result = benchmark_pipeline(settings, log_path=path, eps=eps, min_samples=min_samples)
    except Exception as exc:
        console.print(f"[red]Benchmark failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    table = Table(title="Benchmark")
    table.add_column("Stage")
    table.add_column("Seconds")
    table.add_row("ingest", f"{result.ingest_seconds:.3f}")
    table.add_row("embed", f"{result.embed_seconds:.3f}")
    table.add_row("cluster", f"{result.cluster_seconds:.3f}")
    table.add_row("total", f"{result.total_seconds:.3f}")
    console.print(table)
    console.print(
        f"Ingested {result.ingested_logs} logs, embedded {result.embedded_logs}, produced {result.cluster_count} clusters."
    )


if __name__ == "__main__":
    app()
