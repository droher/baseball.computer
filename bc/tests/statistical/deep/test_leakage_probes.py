"""Held-out source_family probe contract: signal vs no-signal AUC."""

from __future__ import annotations

import numpy as np
import pytest

from python_models.statistical.deep.leakage_probes import (
    ProbeResult,
    classify_publication_tier,
    source_probe_held_out,
)

SEED = 20260514


def _build_dataset_with_signal(
    *,
    n_rows: int,
    n_dim: int,
    held_out_family: str,
    other_families: tuple[str, ...],
    signal_strength: float,
    rng: np.random.Generator,
) -> tuple[np.ndarray, list[str]]:
    """held_out rows shift in feature space by `signal_strength * e_0`."""
    n_held = n_rows // 2
    n_other = n_rows - n_held
    embeddings = rng.normal(size=(n_rows, n_dim)).astype(np.float64)
    embeddings[:n_held, 0] += signal_strength
    labels = [held_out_family] * n_held + [
        other_families[i % len(other_families)] for i in range(n_other)
    ]
    return embeddings, labels


def test_strong_signal_yields_high_auc() -> None:
    rng = np.random.default_rng(SEED)
    embeddings, labels = _build_dataset_with_signal(
        n_rows=400,
        n_dim=8,
        held_out_family="RETROSHEET",
        other_families=("STATCAST", "BASEBALL_REFERENCE"),
        signal_strength=4.0,
        rng=rng,
    )
    result = source_probe_held_out(embeddings, labels, "RETROSHEET")
    assert isinstance(result, ProbeResult)
    assert result.held_out_family == "RETROSHEET"
    assert result.auc > 0.9, f"expected AUC > 0.9 with strong signal, got {result.auc}"
    assert result.publication_tier == "diagnostic_only"


def test_no_signal_yields_random_auc() -> None:
    rng = np.random.default_rng(SEED)
    embeddings, labels = _build_dataset_with_signal(
        n_rows=400,
        n_dim=8,
        held_out_family="RETROSHEET",
        other_families=("STATCAST", "BASEBALL_REFERENCE"),
        signal_strength=0.0,
        rng=rng,
    )
    result = source_probe_held_out(embeddings, labels, "RETROSHEET")
    assert 0.35 <= result.auc <= 0.65, (
        f"expected near-random AUC with no signal, got {result.auc}"
    )


def test_classify_publication_tier_boundaries() -> None:
    assert classify_publication_tier(0.50) == "full"
    assert classify_publication_tier(0.6499) == "full"
    assert classify_publication_tier(0.65) == "manual_review"
    assert classify_publication_tier(0.7499) == "manual_review"
    assert classify_publication_tier(0.75) == "diagnostic_only"
    assert classify_publication_tier(0.99) == "diagnostic_only"


def test_raises_when_held_out_absent() -> None:
    rng = np.random.default_rng(SEED)
    embeddings = rng.normal(size=(20, 4)).astype(np.float64)
    labels = ["A"] * 10 + ["B"] * 10
    with pytest.raises(ValueError, match="not present"):
        _ = source_probe_held_out(embeddings, labels, "MISSING")


def test_raises_when_held_out_is_only_family() -> None:
    rng = np.random.default_rng(SEED)
    embeddings = rng.normal(size=(20, 4)).astype(np.float64)
    labels = ["ONLY"] * 20
    with pytest.raises(ValueError, match="only family"):
        _ = source_probe_held_out(embeddings, labels, "ONLY")


def test_raises_on_too_few_rows() -> None:
    rng = np.random.default_rng(SEED)
    embeddings = rng.normal(size=(3, 4)).astype(np.float64)
    labels = ["A", "A", "B"]
    with pytest.raises(ValueError, match="at least 4"):
        _ = source_probe_held_out(embeddings, labels, "B")


def test_raises_on_non_2d_embeddings() -> None:
    rng = np.random.default_rng(SEED)
    embeddings = rng.normal(size=(20,)).astype(np.float64)
    labels = ["A", "B"] * 10
    with pytest.raises(ValueError, match="must be 2-d"):
        _ = source_probe_held_out(embeddings, labels, "B")
