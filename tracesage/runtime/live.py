from __future__ import annotations

from tracesage.config import Settings
from tracesage.domain import AnomalyRecord, WatchResult
from tracesage.ml.embeddings import build_embedding_provider
from tracesage.pipeline import process_live_iteration


class LiveProcessor:
    def __init__(
        self,
        settings: Settings,
        eps: float,
        min_samples: int,
        min_growth: int,
        z_threshold: float,
    ) -> None:
        self.settings = settings
        self.eps = eps
        self.min_samples = min_samples
        self.min_growth = min_growth
        self.z_threshold = z_threshold
        self.provider = build_embedding_provider(
            settings.embedding_model,
            batch_size=settings.embedding_batch_size,
            cache_dir=settings.hf_cache_dir,
        )

    def process(
        self,
        source: str,
        lines: list[tuple[int, str]],
        session_id: int | None = None,
    ) -> tuple[WatchResult, list[AnomalyRecord]]:
        return process_live_iteration(
            settings=self.settings,
            source=source,
            lines=lines,
            eps=self.eps,
            min_samples=self.min_samples,
            min_growth=self.min_growth,
            z_threshold=self.z_threshold,
            session_id=session_id,
            provider=self.provider,
        )
