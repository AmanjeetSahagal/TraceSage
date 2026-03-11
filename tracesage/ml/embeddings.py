from __future__ import annotations

from pathlib import Path
from typing import Protocol

from sentence_transformers import SentenceTransformer


class EmbeddingProvider(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]: ...


class HFEmbeddingProvider:
    def __init__(self, model_name: str, batch_size: int, cache_dir: Path) -> None:
        self.model_name = model_name
        self.batch_size = batch_size
        cache_dir.mkdir(parents=True, exist_ok=True)
        try:
            self.model = SentenceTransformer(
                model_name,
                cache_folder=str(cache_dir),
                local_files_only=True,
            )
        except Exception:
            try:
                self.model = SentenceTransformer(
                    model_name,
                    cache_folder=str(cache_dir),
                )
            except Exception as exc:
                raise RuntimeError(
                    "Unable to load the Hugging Face embedding model. "
                    "If this is the first run, the model must be downloaded or pre-cached."
                ) from exc

    def embed(self, texts: list[str]) -> list[list[float]]:
        vectors = self.model.encode(
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        return vectors.tolist()


def build_embedding_provider(model_name: str, batch_size: int, cache_dir: Path) -> HFEmbeddingProvider:
    return HFEmbeddingProvider(
        model_name=model_name,
        batch_size=batch_size,
        cache_dir=cache_dir,
    )
