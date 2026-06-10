"""Held-out source_family probe contract: signal vs no-signal AUC."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from python_models.statistical.deep.leakage_probes import (
    OofIntegrityResult,
    ProbeResult,
    classify_publication_tier,
    oof_integrity_probe,
    source_probe_held_out,
)
from python_models.statistical.splits import game_hash_fold

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


OOF_FOLD_COUNT = 3


def _oof_fixture(
    *, n_games: int = 12, rows_per_game: int = 4
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Probabilities export + grain->game_id mapping with honest fold ids."""
    records: list[dict[str, object]] = []
    mapping: list[dict[str, object]] = []
    event_key = 0
    for g_idx in range(n_games):
        game_id = f"GAME{g_idx:04d}"
        train_game = g_idx < n_games - 2
        fold = game_hash_fold(game_id, fold_count=OOF_FOLD_COUNT)
        for _ in range(rows_per_game):
            partition = "OOF" if train_game else (
                "VALIDATE" if g_idx == n_games - 2 else "TEST"
            )
            records.append(
                {
                    "event_key": event_key,
                    "partition": partition,
                    "fold_id": fold if partition == "OOF" else None,
                    "dl_p_class": [0.5, 0.5],
                }
            )
            mapping.append({"event_key": event_key, "game_id": game_id})
            event_key += 1
    probabilities = pl.DataFrame(
        records,
        schema_overrides={"fold_id": pl.Int32, "dl_p_class": pl.List(pl.Float64)},
    )
    game_ids = pl.DataFrame(mapping)
    assert (
        probabilities.filter(pl.col("partition") == "OOF")["fold_id"].n_unique()
        >= 2
    )
    return probabilities, game_ids


def test_oof_integrity_probe_passes_on_honest_export() -> None:
    probabilities, game_ids = _oof_fixture()
    result = oof_integrity_probe(
        probabilities, game_ids, fold_count=OOF_FOLD_COUNT
    )
    assert isinstance(result, OofIntegrityResult)
    assert result.status == "pass"
    assert result.n_fold_mismatches == 0
    assert result.n_cross_partition_grains == 0
    assert result.n_distinct_folds >= 2
    assert result.n_oof_rows == int(
        probabilities.filter(pl.col("partition") == "OOF").height
    )


def test_oof_integrity_probe_fails_on_wrong_fold() -> None:
    probabilities, game_ids = _oof_fixture()
    corrupted = probabilities.with_columns(
        pl.when(pl.col("event_key") == 0)
        .then((pl.col("fold_id") + 1) % OOF_FOLD_COUNT)
        .otherwise(pl.col("fold_id"))
        .cast(pl.Int32)
        .alias("fold_id")
    )
    result = oof_integrity_probe(corrupted, game_ids, fold_count=OOF_FOLD_COUNT)
    assert result.status == "fail"
    assert result.n_fold_mismatches == 1
    assert "does not match" in result.detail


def test_oof_integrity_probe_fails_on_cross_partition_duplicate() -> None:
    probabilities, game_ids = _oof_fixture()
    oof_key = int(
        probabilities.filter(pl.col("partition") == "OOF")["event_key"][0]
    )
    duplicate = probabilities.filter(pl.col("event_key") == oof_key).with_columns(
        pl.lit("VALIDATE").alias("partition"),
        pl.lit(None, dtype=pl.Int32).alias("fold_id"),
    )
    result = oof_integrity_probe(
        pl.concat([probabilities, duplicate]), game_ids, fold_count=OOF_FOLD_COUNT
    )
    assert result.status == "fail"
    assert result.n_cross_partition_grains == 1
    assert "multiple partitions" in result.detail


def test_oof_integrity_probe_fails_on_single_fold() -> None:
    probabilities, game_ids = _oof_fixture()
    collapsed = probabilities.with_columns(
        pl.when(pl.col("partition") == "OOF")
        .then(pl.lit(0, dtype=pl.Int32))
        .otherwise(pl.lit(None, dtype=pl.Int32))
        .alias("fold_id")
    )
    result = oof_integrity_probe(collapsed, game_ids, fold_count=OOF_FOLD_COUNT)
    assert result.status == "fail"
    assert result.n_distinct_folds == 1
    assert "distinct fold" in result.detail


def test_oof_integrity_probe_unverifiable_without_fold_id_column() -> None:
    probabilities, game_ids = _oof_fixture()
    legacy = probabilities.drop("fold_id")
    result = oof_integrity_probe(legacy, game_ids, fold_count=OOF_FOLD_COUNT)
    assert result.status == "unverifiable"
    assert "predates" in result.detail
