from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    db_path: Path = Path(".tracesage/tracesage.duckdb")
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    embedding_batch_size: int = 64
    hf_cache_dir: Path = Path(".tracesage/hf-cache")
    summary_provider: str = "template"
    hf_summary_model: str = "google/flan-t5-base"
    export_dir: Path = Path("reports")
    deploy_correlation_window_minutes: int = 90
    cluster_match_threshold: float = 0.92


def get_settings() -> Settings:
    return Settings(
        db_path=Path(os.getenv("TRACESAGE_DB_PATH", ".tracesage/tracesage.duckdb")),
        embedding_model=os.getenv(
            "TRACESAGE_EMBEDDING_MODEL",
            "sentence-transformers/all-MiniLM-L6-v2",
        ),
        embedding_batch_size=int(os.getenv("TRACESAGE_EMBEDDING_BATCH_SIZE", "64")),
        hf_cache_dir=Path(os.getenv("TRACESAGE_HF_CACHE_DIR", ".tracesage/hf-cache")),
        summary_provider=os.getenv("TRACESAGE_SUMMARY_PROVIDER", "template"),
        hf_summary_model=os.getenv("TRACESAGE_HF_SUMMARY_MODEL", "google/flan-t5-base"),
        export_dir=Path(os.getenv("TRACESAGE_EXPORT_DIR", "reports")),
        deploy_correlation_window_minutes=int(
            os.getenv("TRACESAGE_DEPLOY_WINDOW_MINUTES", "90")
        ),
        cluster_match_threshold=float(
            os.getenv("TRACESAGE_CLUSTER_MATCH_THRESHOLD", "0.92")
        ),
    )
