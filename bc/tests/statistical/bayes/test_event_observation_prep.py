"""Synthetic-fixture tests for ``prepare_event_observation_inputs``."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.models._event_data import (
    CONTINUOUS_COLUMNS,
    FIXED_EFFECT_COLUMNS,
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    UNKNOWN_LEVEL,
    prepare_event_observation_inputs,
)
from python_models.statistical.splits import game_hash_fold


def _game_id_pool(*, n_holdout: int, n_train: int) -> list[str]:
    holdout: list[str] = []
    train: list[str] = []
    i = 0
    while len(holdout) < n_holdout or len(train) < n_train:
        gid = f"GAME{i:04d}"
        if game_hash_fold(gid, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID:
            if len(holdout) < n_holdout:
                holdout.append(gid)
        elif len(train) < n_train:
            train.append(gid)
        i += 1
    return holdout + train


def _expected_split_event_keys(parquet_path: Path) -> tuple[set[int], set[int]]:
    df = pl.read_parquet(parquet_path)
    held_games = {
        g
        for g in df.get_column("game_id").unique().to_list()
        if game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
    }
    held_keys = set(
        df.filter(pl.col("game_id").is_in(sorted(held_games)))
        .get_column("event_key")
        .to_list()
    )
    train_keys = set(df.get_column("event_key").to_list()) - held_keys
    return train_keys, held_keys


def _synthetic_dataset(tmp_path: Path, *, dimension: str = "trajectory") -> Path:
    rng = np.random.default_rng(20260519)
    n = 200
    game_pool = _game_id_pool(n_holdout=4, n_train=16)
    game_ids = [game_pool[i % len(game_pool)] for i in range(n)]
    seasons = rng.choice(["1995", "2005", "2015"], size=n)
    scorers = rng.choice(["A", "B", "C", "D"], size=n)
    parks = rng.choice(["PRK1", "PRK2", "PRK3"], size=n)
    sources = rng.choice(["play_by_play", "box_score"], size=n)
    game_types = rng.choice(["RegularSeason", "Postseason"], size=n)
    frame_start = rng.choice(["Top", "Bottom"], size=n)
    exposure = rng.choice(["complete", "shortened"], size=n)
    alignment = rng.choice(
        ["pre_shift_era", "shift_growth_era", "full_shift_era", "post_restriction"],
        size=n,
    )
    league = rng.choice(["AL", "NL"], size=n)
    result_family = rng.choice(
        ["hit", "out_in_play", "strikeout", "walk", "hbp"], size=n
    )
    leverage_bucket = rng.choice(["low", "medium", "high"], size=n)
    batter_hand = rng.choice(["L", "R"], size=n)
    pitcher_hand = rng.choice(["L", "R"], size=n)
    personnel_conf = rng.choice(["high", "medium", "low"], size=n)
    context_conf = rng.choice(["high", "medium", "low"], size=n)

    outs_start = rng.integers(0, 3, size=n)
    inning_start = rng.integers(1, 10, size=n)
    score_margin = rng.integers(-5, 5, size=n)
    leverage_index = rng.uniform(0.1, 3.0, size=n).astype(np.float64)
    runs_on_play = rng.integers(0, 4, size=n)

    leverage_index[::17] = np.nan

    is_observed = rng.integers(0, 2, size=n).astype(bool)
    df = pl.DataFrame(
        {
            "event_key": np.arange(n, dtype=np.int64),
            "dimension": [dimension] * n,
            "game_id": pl.Series("game_id", game_ids, dtype=pl.Utf8),
            "is_observed": is_observed,
            "season": pl.Series("season", [str(s) for s in seasons.tolist()], dtype=pl.Utf8),
            "scorer": pl.Series(
                "scorer",
                [None if i % 23 == 0 else str(v) for i, v in enumerate(scorers.tolist())],
                dtype=pl.Utf8,
            ),
            "park_id": pl.Series("park_id", [str(s) for s in parks.tolist()], dtype=pl.Utf8),
            "source_family": pl.Series("source_family", [str(s) for s in sources.tolist()], dtype=pl.Utf8),
            "game_type": pl.Series("game_type", [str(s) for s in game_types.tolist()], dtype=pl.Utf8),
            "frame_start": pl.Series("frame_start", [str(s) for s in frame_start.tolist()], dtype=pl.Utf8),
            "exposure_status": pl.Series(
                "exposure_status",
                [None if i % 29 == 0 else str(v) for i, v in enumerate(exposure.tolist())],
                dtype=pl.Utf8,
            ),
            "alignment_regime": pl.Series("alignment_regime", [str(s) for s in alignment.tolist()], dtype=pl.Utf8),
            "league": pl.Series("league", [str(s) for s in league.tolist()], dtype=pl.Utf8),
            "result_family": pl.Series("result_family", [str(s) for s in result_family.tolist()], dtype=pl.Utf8),
            "hit_or_out": is_observed,
            "leverage_bucket": pl.Series("leverage_bucket", [str(s) for s in leverage_bucket.tolist()], dtype=pl.Utf8),
            "batter_hand": pl.Series("batter_hand", [str(s) for s in batter_hand.tolist()], dtype=pl.Utf8),
            "pitcher_hand": pl.Series("pitcher_hand", [str(s) for s in pitcher_hand.tolist()], dtype=pl.Utf8),
            "personnel_confidence": pl.Series("personnel_confidence", [str(s) for s in personnel_conf.tolist()], dtype=pl.Utf8),
            "context_confidence": pl.Series("context_confidence", [str(s) for s in context_conf.tolist()], dtype=pl.Utf8),
            "outs_start": outs_start.astype(np.int64),
            "inning_start": inning_start.astype(np.int64),
            "score_margin": score_margin.astype(np.int64),
            "leverage_index": leverage_index,
            "runs_on_play": runs_on_play.astype(np.int64),
            "training_weight": np.full(n, 1.0),
        }
    )
    parquet_path = tmp_path / "dataset.parquet"
    df.write_parquet(parquet_path)
    return parquet_path


def test_prepare_indexer_contiguity_and_shapes(tmp_path: Path) -> None:
    parquet_path = _synthetic_dataset(tmp_path)
    train_keys, held_keys = _expected_split_event_keys(parquet_path)
    assert train_keys and held_keys
    inputs = prepare_event_observation_inputs(parquet_path, dimension="trajectory")
    n_train = len(train_keys)
    assert inputs.dimension == "trajectory"
    assert inputs.y.shape == (n_train,)
    assert inputs.event_keys.shape == (n_train,)
    for name, idx, key in (
        ("season", inputs.season_idx, "season"),
        ("scorer", inputs.scorer_idx, "scorer"),
        ("park", inputs.park_idx, "park"),
        ("source", inputs.source_idx, "source"),
    ):
        assert idx.shape == (n_train,), name
        assert set(np.unique(idx).tolist()) == set(range(len(inputs.coords[key])))


def test_unknown_level_inserted_for_null_categoricals(tmp_path: Path) -> None:
    parquet_path = _synthetic_dataset(tmp_path)
    inputs = prepare_event_observation_inputs(parquet_path, dimension="trajectory")
    assert UNKNOWN_LEVEL in inputs.coords["scorer"]
    assert UNKNOWN_LEVEL in inputs.fixed_effects["exposure_status"].levels


def test_continuous_standardization_zero_mean_unit_std(tmp_path: Path) -> None:
    parquet_path = _synthetic_dataset(tmp_path)
    inputs = prepare_event_observation_inputs(parquet_path, dimension="trajectory")
    for column in CONTINUOUS_COLUMNS:
        feat = inputs.continuous[column]
        finite_mask = feat.is_missing == 0
        assert finite_mask.any()
        finite_vals = feat.values[finite_mask]
        assert finite_vals.mean() == pytest.approx(0.0, abs=1e-9)
        assert finite_vals.std(ddof=0) == pytest.approx(1.0, abs=1e-9)


def test_continuous_missing_indicator_emits_for_nulls(tmp_path: Path) -> None:
    parquet_path = _synthetic_dataset(tmp_path)
    inputs = prepare_event_observation_inputs(parquet_path, dimension="trajectory")
    li = inputs.continuous["leverage_index"]
    assert int(li.is_missing.sum()) > 0
    assert np.all(li.values[li.is_missing == 1] == 0.0)


def test_saturated_seasons_are_dropped(tmp_path: Path) -> None:
    parquet_path = _synthetic_dataset(tmp_path)
    df = pl.read_parquet(parquet_path)
    saturated = df.with_columns(
        is_observed=pl.when(pl.col("season") == "2015").then(True).otherwise(pl.col("is_observed"))
    )
    saturated.write_parquet(parquet_path)
    inputs = prepare_event_observation_inputs(parquet_path, dimension="trajectory")
    assert "2015" not in inputs.coords["season"]


def test_holdout_is_game_disjoint_and_deterministic(tmp_path: Path) -> None:
    parquet_path = _synthetic_dataset(tmp_path)
    train_keys, held_keys = _expected_split_event_keys(parquet_path)
    assert train_keys and held_keys

    first = prepare_event_observation_inputs(parquet_path, dimension="trajectory")
    second = prepare_event_observation_inputs(parquet_path, dimension="trajectory")
    assert first.held_out is not None and second.held_out is not None
    assert np.array_equal(first.event_keys, second.event_keys)
    assert np.array_equal(first.held_out.event_keys, second.held_out.event_keys)
    assert np.array_equal(first.held_out.y, second.held_out.y)

    assert set(first.event_keys.tolist()) == train_keys
    assert set(first.held_out.event_keys.tolist()) == held_keys
    assert set(first.event_keys.tolist()).isdisjoint(
        first.held_out.event_keys.tolist()
    )

    key_to_game = dict(
        pl.read_parquet(parquet_path)
        .select(["event_key", "game_id"])
        .iter_rows()
    )
    train_games = {key_to_game[k] for k in first.event_keys.tolist()}
    held_games = {key_to_game[k] for k in first.held_out.event_keys.tolist()}
    assert train_games.isdisjoint(held_games)


def test_holdout_unaffected_by_row_subsample(tmp_path: Path) -> None:
    parquet_path = _synthetic_dataset(tmp_path)
    full = prepare_event_observation_inputs(parquet_path, dimension="trajectory")
    limited = prepare_event_observation_inputs(
        parquet_path, dimension="trajectory", smoke_limit=50
    )
    assert full.held_out is not None and limited.held_out is not None
    assert limited.n_events <= 50
    assert np.array_equal(limited.held_out.event_keys, full.held_out.event_keys)


def test_unseen_levels_in_held_out_encode_to_minus_one(tmp_path: Path) -> None:
    parquet_path = _synthetic_dataset(tmp_path)
    df = pl.read_parquet(parquet_path)
    held_games = sorted(
        g
        for g in df.get_column("game_id").unique().to_list()
        if game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
    )
    assert held_games
    target_game = held_games[0]
    is_target = pl.col("game_id") == target_game
    df = df.with_columns(
        pl.when(is_target).then(pl.lit("ZZZZ")).otherwise(pl.col("park_id")).alias("park_id"),
        pl.when(is_target).then(pl.lit("Exhibition")).otherwise(pl.col("game_type")).alias("game_type"),
    )
    df.write_parquet(parquet_path)
    target_keys = set(
        df.filter(is_target).get_column("event_key").to_list()
    )

    inputs = prepare_event_observation_inputs(parquet_path, dimension="trajectory")
    assert "ZZZZ" not in inputs.coords["park"]
    assert "Exhibition" not in inputs.fixed_effects["game_type"].levels
    held = inputs.held_out
    assert held is not None
    mask = np.isin(held.event_keys, np.array(sorted(target_keys), dtype=np.int64))
    assert mask.any()
    assert np.all(held.park_idx[mask] == -1)
    assert np.all(held.fixed_effects["game_type"].codes[mask] == -1)
    assert np.all(held.park_idx[~mask] >= 0)


def test_held_out_continuous_standardized_with_training_stats(tmp_path: Path) -> None:
    parquet_path = _synthetic_dataset(tmp_path)
    inputs = prepare_event_observation_inputs(parquet_path, dimension="trajectory")
    held = inputs.held_out
    assert held is not None and held.n_events > 0
    raw_by_key = dict(
        pl.read_parquet(parquet_path)
        .select(["event_key", "outs_start"])
        .iter_rows()
    )
    train_feat = inputs.continuous["outs_start"]
    held_feat = held.continuous["outs_start"]
    assert held_feat.raw_mean == train_feat.raw_mean
    assert held_feat.raw_std == train_feat.raw_std
    expected = (
        np.array([float(raw_by_key[k]) for k in held.event_keys.tolist()])
        - train_feat.raw_mean
    ) / train_feat.raw_std
    np.testing.assert_allclose(held_feat.values, expected)


def test_fixed_effect_design_shape_and_levels(tmp_path: Path) -> None:
    parquet_path = _synthetic_dataset(tmp_path)
    inputs = prepare_event_observation_inputs(parquet_path, dimension="trajectory")
    assert inputs.fixed_effects, "expected at least one FE column to be designed"
    for column, design in inputs.fixed_effects.items():
        assert column in FIXED_EFFECT_COLUMNS, (
            f"unexpected FE column {column!r} not in FIXED_EFFECT_COLUMNS"
        )
        assert design.codes.shape == (inputs.n_events,)
        assert design.levels == tuple(sorted(design.levels))
        assert set(np.unique(design.codes).tolist()).issubset(
            set(range(len(design.levels)))
        )
