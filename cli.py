from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from tracesage.config import get_settings
from tracesage.pipeline import (
    cluster_logs,
    detect_anomalies,
    embed_logs,
    ingest_logs,
    summarize_cluster,
)

app = typer.Typer(help="TraceSage semantic log intelligence CLI")
console = Console()


@app.command()
def ingest(path: Path) -> None:
    """Load JSON, CSV, or plaintext logs into DuckDB."""
    settings = get_settings()
    try:
        count = ingest_logs(path, settings)
    except Exception as exc:
        console.print(f"[red]Ingest failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    console.print(f"Ingested [bold]{count}[/bold] logs into {settings.db_path}")


@app.command()
def embed() -> None:
    """Generate embeddings for pending logs."""
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
def cluster(
    eps: float = typer.Option(0.3, help="Cosine distance threshold for clustering."),
    min_samples: int = typer.Option(3, help="Minimum samples per cluster."),
) -> None:
    """Group similar log messages into clusters."""
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
    min_growth: int = typer.Option(2, help="Minimum log-count increase to flag growth."),
    z_threshold: float = typer.Option(2.0, help="Z-score threshold for rare spikes."),
) -> None:
    """Detect novel or rapidly growing clusters across clustering runs."""
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
def summarize(
    cluster: int = typer.Option(..., "--cluster", help="Cluster ID to summarize."),
    provider: str = typer.Option(None, help="Summary provider: template or huggingface."),
) -> None:
    """Generate a structured incident summary for a cluster."""
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
                ]
            ),
            title=f"Cluster {summary.cluster_id} Summary",
        )
    )


if __name__ == "__main__":
    app()
