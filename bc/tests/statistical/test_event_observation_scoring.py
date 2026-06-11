"""Synthetic-fixture tests for ``build_observation_scoring_frame``."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.models._event_data import (
    CONTINUOUS_COLUMNS,
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    EventObservationInputs,
    ObservationHeldOutSet,
    build_observation_scoring_frame,
    prepare_event_observation_inputs,
)
from python_models.statistical.splits import game_hash_fold

DIMENSION = "trajectory"
OTHER_DIMENSION = "location_side"
KEPT_SEASONS = ("1995", "2005")
SATURATED_SEASON = "2015"
UNSEEN_PARK = "ZZZZ"
UNSEEN_GAME_TYPE = "Exhibition"
EXTREME_SCORE_MARGIN = 50


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


def _random_columns(rng: np.random.Generator, n: int) -> dict[str, object]:
    return {
        "scorer": pl.Series(
            "scorer",
            [
                None if i % 19 == 3 else str(v)
                for i, v in enumerate(rng.choice(["A", "B", "C"], size=n).tolist())
            ],
            dtype=pl.Utf8,
        ),
        "park_id": pl.Series(
            "park_id",
            [str(v) for v in rng.choice(["PRK1", "PRK2", "PRK3"], size=n).tolist()],
            dtype=pl.Utf8,
        ),
        "source_family": pl.Series(
            "source_family",
            [
                str(v)
                for v in rng.choice(["play_by_play", "box_score"], size=n).tolist()
            ],
            dtype=pl.Utf8,
        ),
        "game_type": pl.Series(
            "game_type",
            [
                str(v)
                for v in rng.choice(["RegularSeason", "Postseason"], size=n).tolist()
            ],
            dtype=pl.Utf8,
        ),
        "frame_start": pl.Series(
            "frame_start",
            [str(v) for v in rng.choice(["Top", "Bottom"], size=n).tolist()],
            dtype=pl.Utf8,
        ),
        "exposure_status": pl.Series(
            "exposure_status",
            [str(v) for v in rng.choice(["complete", "shortened"], size=n).tolist()],
            dtype=pl.Utf8,
        ),
        "league": pl.Series(
            "league",
            [str(v) for v in rng.choice(["AL", "NL"], size=n).tolist()],
            dtype=pl.Utf8,
        ),
        "result_family": pl.Series(
            "result_family",
            [
                str(v)
                for v in rng.choice(
                    ["hit", "out_in_play", "strikeout"], size=n
                ).tolist()
            ],
            dtype=pl.Utf8,
        ),
        "leverage_bucket": pl.Series(
            "leverage_bucket",
            [str(v) for v in rng.choice(["low", "medium", "high"], size=n).tolist()],
            dtype=pl.Utf8,
        ),
        "batter_hand": pl.Series(
            "batter_hand",
            [str(v) for v in rng.choice(["L", "R"], size=n).tolist()],
            dtype=pl.Utf8,
        ),
        "pitcher_hand": pl.Series(
            "pitcher_hand",
            [str(v) for v in rng.choice(["L", "R"], size=n).tolist()],
            dtype=pl.Utf8,
        ),
        "personnel_confidence": pl.Series(
            "personnel_confidence",
            [str(v) for v in rng.choice(["high", "medium", "low"], size=n).tolist()],
            dtype=pl.Utf8,
        ),
        "context_confidence": pl.Series(
            "context_confidence",
            [str(v) for v in rng.choice(["high", "medium", "low"], size=n).tolist()],
            dtype=pl.Utf8,
        ),
        "outs_start": rng.integers(0, 3, size=n).astype(np.int64),
        "inning_start": rng.integers(1, 10, size=n).astype(np.int64),
        "score_margin": rng.integers(-5, 5, size=n).astype(np.int64),
        "leverage_index": rng.uniform(0.1, 3.0, size=n).astype(np.float64),
        "runs_on_play": rng.integers(0, 4, size=n).astype(np.int64),
    }


def _block(
    rng: np.random.Generator,
    *,
    start_key: int,
    n: int,
    dimension: str,
    seasons: list[str],
    game_pool: list[str],
    training_weight: float,
    is_observed: np.ndarray,
    overrides: dict[str, object] | None = None,
) -> pl.DataFrame:
    columns = _random_columns(rng, n)
    if overrides:
        columns.update(overrides)
    return pl.DataFrame(
        {
            "event_key": np.arange(start_key, start_key + n, dtype=np.int64),
            "dimension": pl.Series("dimension", [dimension] * n, dtype=pl.Utf8),
            "game_id": pl.Series(
                "game_id",
                [game_pool[i % len(game_pool)] for i in range(n)],
                dtype=pl.Utf8,
            ),
            "is_observed": is_observed.astype(bool),
            "training_weight": np.full(n, training_weight, dtype=np.float64),
            "season": pl.Series("season", seasons, dtype=pl.Utf8),
            **columns,
        }
    )


def _write_dataset(tmp_path: Path) -> Path:
    rng = np.random.default_rng(20260610)
    game_pool = _game_id_pool(n_holdout=4, n_train=16)
    frames: list[pl.DataFrame] = []
    key = 0

    for season in KEPT_SEASONS:
        n = 320
        frames.append(
            _block(
                rng,
                start_key=key,
                n=n,
                dimension=DIMENSION,
                seasons=[season] * n,
                game_pool=game_pool,
                training_weight=1.0,
                is_observed=(np.arange(n) % 2 == 0),
            )
        )
        key += n

    n_saturated = 120
    frames.append(
        _block(
            rng,
            start_key=key,
            n=n_saturated,
            dimension=DIMENSION,
            seasons=[SATURATED_SEASON] * n_saturated,
            game_pool=game_pool,
            training_weight=1.0,
            is_observed=np.ones(n_saturated, dtype=bool),
        )
    )
    key += n_saturated

    n_scoring_only = 60
    season_cycle = (*KEPT_SEASONS, SATURATED_SEASON)
    scoring_leverage = rng.uniform(0.1, 3.0, size=n_scoring_only).astype(np.float64)
    scoring_leverage[1] = np.nan
    frames.append(
        _block(
            rng,
            start_key=key,
            n=n_scoring_only,
            dimension=DIMENSION,
            seasons=[
                season_cycle[i % len(season_cycle)] for i in range(n_scoring_only)
            ],
            game_pool=game_pool,
            training_weight=0.0,
            is_observed=(np.arange(n_scoring_only) % 2 == 1),
            overrides={
                "score_margin": np.full(
                    n_scoring_only, EXTREME_SCORE_MARGIN, dtype=np.int64
                ),
                "leverage_index": scoring_leverage,
                "park_id": pl.Series(
                    "park_id",
                    [UNSEEN_PARK] + ["PRK1"] * (n_scoring_only - 1),
                    dtype=pl.Utf8,
                ),
                "game_type": pl.Series(
                    "game_type",
                    [UNSEEN_GAME_TYPE] + ["RegularSeason"] * (n_scoring_only - 1),
                    dtype=pl.Utf8,
                ),
            },
        )
    )
    key += n_scoring_only

    n_other = 30
    frames.append(
        _block(
            rng,
            start_key=key,
            n=n_other,
            dimension=OTHER_DIMENSION,
            seasons=[KEPT_SEASONS[i % len(KEPT_SEASONS)] for i in range(n_other)],
            game_pool=game_pool,
            training_weight=1.0,
            is_observed=(np.arange(n_other) % 2 == 0),
        )
    )

    parquet_path = tmp_path / "dataset.parquet"
    pl.concat(frames, how="vertical").write_parquet(parquet_path)
    return parquet_path


def _prepare(
    tmp_path: Path,
) -> tuple[Path, EventObservationInputs, ObservationHeldOutSet]:
    parquet_path = _write_dataset(tmp_path)
    inputs = prepare_event_observation_inputs(parquet_path, dimension=DIMENSION)
    scoring = build_observation_scoring_frame(
        parquet_path, dimension=DIMENSION, inputs=inputs
    )
    return parquet_path, inputs, scoring


def test_scoring_frame_covers_all_rows_within_fitted_seasons(tmp_path: Path) -> None:
    parquet_path, inputs, scoring = _prepare(tmp_path)
    df = pl.read_parquet(parquet_path)

    assert SATURATED_SEASON not in inputs.coords["season"]
    in_scope = df.filter(
        (pl.col("dimension") == DIMENSION)
        & pl.col("season").is_in(list(inputs.coords["season"]))
    )
    expected_keys = np.sort(
        in_scope.get_column("event_key").to_numpy().astype(np.int64)
    )
    np.testing.assert_array_equal(scoring.event_keys, expected_keys)

    scoring_key_set = set(scoring.event_keys.tolist())
    unweighted_keys = set(
        in_scope.filter(pl.col("training_weight") == 0.0)
        .get_column("event_key")
        .to_list()
    )
    assert unweighted_keys
    assert unweighted_keys <= scoring_key_set

    assert inputs.held_out is not None
    assert scoring_key_set == (
        set(inputs.event_keys.tolist())
        | set(inputs.held_out.event_keys.tolist())
        | unweighted_keys
    )

    excluded = df.filter(
        (pl.col("dimension") != DIMENSION) | (pl.col("season") == SATURATED_SEASON)
    )
    assert scoring_key_set.isdisjoint(excluded.get_column("event_key").to_list())

    observed_by_key = dict(in_scope.select(["event_key", "is_observed"]).iter_rows())
    expected_y = np.array(
        [int(observed_by_key[k]) for k in scoring.event_keys.tolist()], dtype=np.int8
    )
    np.testing.assert_array_equal(scoring.y, expected_y)
    assert set(np.unique(scoring.y).tolist()) == {0, 1}


def test_unseen_levels_encode_minus_one_and_score_without_their_terms(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pymc")
    az = pytest.importorskip("arviz")
    from python_models.statistical.bayes.training import (
        _posterior_held_out_means_bernoulli,
    )

    parquet_path, inputs, scoring = _prepare(tmp_path)
    df = pl.read_parquet(parquet_path)

    assert UNSEEN_PARK not in inputs.coords["park"]
    assert UNSEEN_GAME_TYPE not in inputs.fixed_effects["game_type"].levels
    unseen_key = (
        df.filter(pl.col("park_id") == UNSEEN_PARK).get_column("event_key").item()
    )
    i = int(np.searchsorted(scoring.event_keys, unseen_key))
    assert scoring.event_keys[i] == unseen_key
    assert scoring.park_idx[i] == -1
    assert scoring.fixed_effects["game_type"].codes[i] == -1
    assert scoring.scorer_idx[i] >= 0
    assert scoring.season_idx[i] >= 0

    seen_candidates = np.flatnonzero(
        (scoring.park_idx >= 0) & (scoring.fixed_effects["game_type"].codes >= 0)
    )
    assert seen_candidates.size
    j = int(seen_candidates[0])

    rng = np.random.default_rng(20260611)
    n_chain, n_draw = 2, 4
    alpha = rng.normal(size=(n_chain, n_draw))
    beta_season = rng.normal(size=(n_chain, n_draw, len(inputs.coords["season"])))
    beta_scorer = rng.normal(size=(n_chain, n_draw, len(inputs.coords["scorer"])))
    beta_park = rng.normal(size=(n_chain, n_draw, len(inputs.coords["park"])))
    delta_game_type = rng.normal(
        size=(n_chain, n_draw, len(inputs.fixed_effects["game_type"].levels))
    )
    gamma_outs_start = rng.normal(size=(n_chain, n_draw))
    idata = az.from_dict(
        posterior={
            "alpha": alpha,
            "beta_season": beta_season,
            "beta_scorer": beta_scorer,
            "beta_park": beta_park,
            "delta_game_type": delta_game_type,
            "gamma_outs_start": gamma_outs_start,
        }
    )

    means = _posterior_held_out_means_bernoulli(idata, scoring)
    assert means.shape == (scoring.n_events,)

    def _sigmoid(x: np.ndarray) -> np.ndarray:
        return 1.0 / (1.0 + np.exp(-x))

    def _expected(row: int, *, include_park: bool, include_game_type: bool) -> float:
        eta = (
            alpha
            + beta_season[..., scoring.season_idx[row]]
            + beta_scorer[..., scoring.scorer_idx[row]]
            + gamma_outs_start * float(scoring.continuous["outs_start"].values[row])
        )
        if include_park:
            eta = eta + beta_park[..., scoring.park_idx[row]]
        if include_game_type:
            eta = (
                eta
                + delta_game_type[..., scoring.fixed_effects["game_type"].codes[row]]
            )
        return float(_sigmoid(eta).mean())

    assert means[i] == pytest.approx(
        _expected(i, include_park=False, include_game_type=False)
    )
    assert means[j] == pytest.approx(
        _expected(j, include_park=True, include_game_type=True)
    )


def test_scoring_continuous_standardization_uses_frozen_training_stats(
    tmp_path: Path,
) -> None:
    parquet_path, inputs, scoring = _prepare(tmp_path)
    df = pl.read_parquet(parquet_path)

    for column in CONTINUOUS_COLUMNS:
        assert scoring.continuous[column].raw_mean == inputs.continuous[column].raw_mean
        assert scoring.continuous[column].raw_std == inputs.continuous[column].raw_std

    train_feat = inputs.continuous["score_margin"]
    raw_by_key = dict(df.select(["event_key", "score_margin"]).iter_rows())
    raw = np.array(
        [float(raw_by_key[k]) for k in scoring.event_keys.tolist()], dtype=np.float64
    )
    expected = (raw - train_feat.raw_mean) / train_feat.raw_std
    np.testing.assert_allclose(scoring.continuous["score_margin"].values, expected)

    recomputed_mean = float(raw.mean())
    assert abs(recomputed_mean - train_feat.raw_mean) > 1e-6

    li = scoring.continuous["leverage_index"]
    assert int(li.is_missing.sum()) > 0
    assert np.all(li.values[li.is_missing == 1] == 0.0)


def test_export_schema_and_row_count_match_scoring_frame(tmp_path: Path) -> None:
    pytest.importorskip("pymc")
    pytest.importorskip("arviz")
    from python_models.statistical.bayes.training import _export_event_propensities

    _, _, scoring = _prepare(tmp_path)
    rng = np.random.default_rng(20260612)
    means = rng.uniform(0.0, 1.0, size=scoring.n_events)

    target_path = tmp_path / "event_propensity.parquet"
    written = _export_event_propensities(
        means, scoring.event_keys, DIMENSION, target_path=target_path
    )
    reloaded = pl.read_parquet(target_path)

    expected_schema = {
        "event_key": pl.Int64,
        "dimension": pl.Utf8,
        "p_observed_mean": pl.Float64,
    }
    assert dict(written.schema) == expected_schema
    assert dict(reloaded.schema) == expected_schema
    assert reloaded.height == scoring.n_events
    np.testing.assert_array_equal(
        reloaded.get_column("event_key").to_numpy(), scoring.event_keys
    )
    assert reloaded.get_column("dimension").unique().to_list() == [DIMENSION]
    np.testing.assert_allclose(reloaded.get_column("p_observed_mean").to_numpy(), means)
    assert (
        reloaded.with_columns(pl.col("event_key").cast(pl.UInt32)).height
        == reloaded.height
    )
