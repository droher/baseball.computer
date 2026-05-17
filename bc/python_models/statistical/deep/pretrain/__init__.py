"""Shared entity-embedding pretraining over event-universe pretext heads.

Pretraining writes an artifact carrying per-high-card-column embedding
weights + the matching ``entity_id`` vocabularies. Per-target deep
models warm-start their Embedding layers from this artifact via
``model_factory.set_pretrained_embeddings`` so high-cardinality
features (batter_id, pitcher_id, park_id, scorer, ...) carry signal
sourced from all 18M events rather than the per-target subset alone.
"""

from __future__ import annotations

from python_models.statistical.deep.pretrain.spec import (
    HeadSpec,
    PretrainSpec,
)
from python_models.statistical.deep.pretrain.training import run_pretrain

__all__ = ("HeadSpec", "PretrainSpec", "run_pretrain")
