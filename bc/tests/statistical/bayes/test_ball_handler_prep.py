"""Synthetic-fixture tests for ``prepare_ball_handler_inputs`` (Model D)."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.models._ball_handler_data import (
    FIXED_EFFECT_COLUMNS,
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    N_POSITIONS,
    build_ball_handler_production_frame,
    prepare_ball_handler_inputs,
)
from python_models.statistical.splits import game_hash_fold

DIMENSION = "ball_handler_position"


def _observed_row(
    *,
    event_key: int,
    game_id: str,
    handler: int,
    season: int,
    league: str,
    source_family: str = "play_by_play",
) -> dict[str, object]:
    return {
        "event_key": event_key,
        "dimension": DIMENSION,
        "observed_status": "observed",
        "raw_value": str(handler),
        "training_weight": 1.0,
        "game_id": game_id,
        "season": season,
        "league": league,
        "source_family": source_family,
        "park_id": "ARL01",
        "scorer": "scorerA",
        "base_state_start": 0,
        "outs_start": 1,
        "result_family": "out_in_play",
        "alignment_regime": "shift_growth_era",
    }


def _unobserved_row(
    *,
    event_key: int,
    game_id: str,
    season: int,
    league: str,
    result_family: str | None,
) -> dict[str, object]:
    return {
        "event_key": event_key,
        "dimension": DIMENSION,
        "observed_status": "unobserved",
        "raw_value": None,
        "training_weight": 0.0,
        "game_id": game_id,
        "season": season,
        "league": league,
        "source_family": "play_by_play",
        "park_id": "ARL01",
        "scorer": "scorerA",
        "base_state_start": 0,
        "outs_start": 1,
        "result_family": result_family,
        "alignment_regime": "shift_growth_era",
    }


def _synthetic_dataset(
    tmp_path: Path,
    *,
    n_games: int = 24,
    events_per_game: int = 10,
    season: int = 1925,
    league: str = "AL",
    n_production_events: int = 0,
    production_result_family_rate: float = 1.0,
    seed: int = 20260525,
) -> Path:
    """Build a handler-observed fixture, optionally with production rows.

    Each observed event carries a single handler position 1..9 sampled
    from a skewed distribution. ``n_production_events`` adds
    handler-unobserved rows used by the FE-coverage guard and the
    production scoring frame; ``production_result_family_rate`` controls
    what fraction of them have a non-null ``result_family``.
    """
    rng = np.random.default_rng(seed)
    weights = np.linspace(1.0, 3.0, N_POSITIONS)
    weights = weights / weights.sum()
    rows: list[dict[str, object]] = []
    for g in range(n_games):
        for e in range(events_per_game):
            eid = 100_000 + g * events_per_game + e
            handler = int(rng.choice(N_POSITIONS, p=weights)) + 1
            rows.append(
                _observed_row(
                    event_key=eid,
                    game_id=f"G{g:03d}",
                    handler=handler,
                    season=season,
                    league=league,
                )
            )
    for i in range(n_production_events):
        eid = 900_000 + i
        keep = rng.random() < production_result_family_rate
        rows.append(
            _unobserved_row(
                event_key=eid,
                game_id=f"GP{i:04d}",
                season=season,
                league=league,
                result_family="out_in_play" if keep else None,
            )
        )
    dataset_path = tmp_path / "dataset.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)
    return dataset_path


def test_counts_are_one_hot_over_K9(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(tmp_path)
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.n_positions == N_POSITIONS == 9
    assert inputs.counts.shape == (inputs.n_events, N_POSITIONS)
    row_sums = inputs.counts.sum(axis=1)
    assert (row_sums == 1).all(), "every event must have exactly one handler class"
    assert inputs.counts.min() == 0 and inputs.counts.max() == 1
    assert len(inputs.coords["position"]) == N_POSITIONS


def test_indexer_contiguity(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(tmp_path)
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert set(np.unique(inputs.season_league_idx).tolist()) == set(
        range(len(inputs.coords["season_league"]))
    )
    assert set(np.unique(inputs.scorer_idx).tolist()) == set(
        range(len(inputs.coords["scorer"]))
    )


def test_train_and_holdout_games_disjoint(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(tmp_path, n_games=24, events_per_game=10)
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
    )
    assert inputs.held_out.n_events > 0, "fixture should seed the fold-0 holdout"
    train_keys = set(inputs.event_keys.tolist())
    holdout_keys = set(inputs.held_out.event_keys.tolist())
    assert train_keys.isdisjoint(holdout_keys)

    df = pl.read_parquet(dataset_path)
    held_game_event_keys = (
        df.filter(
            pl.col("game_id").map_elements(
                lambda g: game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT)
                == HOLDOUT_FOLD_ID,
                return_dtype=pl.Boolean,
            )
        )
        .get_column("event_key")
        .to_list()
    )
    assert train_keys.isdisjoint(set(held_game_event_keys))


def test_held_out_truth_is_zero_based_handler(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(tmp_path)
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
    )
    held = inputs.held_out
    if held.n_events == 0:
        pytest.skip("no holdout events in this fixture")
    assert held.true_position.min() >= 0
    assert held.true_position.max() < N_POSITIONS
    assert (held.U == 1).all()
    df = pl.read_parquet(dataset_path).filter(
        pl.col("event_key").is_in(held.event_keys.tolist())
    )
    truth_lookup = dict(
        zip(
            df.get_column("event_key").to_list(),
            (df.get_column("raw_value").cast(pl.Int64) - 1).to_list(),
        )
    )
    for ek, tp in zip(held.event_keys.tolist(), held.true_position.tolist()):
        assert truth_lookup[ek] == tp


def test_season_league_floor_drops_thin_cells(tmp_path: Path) -> None:
    rows: list[dict[str, object]] = []
    for g in range(20):
        for e in range(10):
            eid = 100_000 + g * 10 + e
            rows.append(
                _observed_row(
                    event_key=eid,
                    game_id=f"BIG{g:03d}",
                    handler=1 + (eid % N_POSITIONS),
                    season=1925,
                    league="AL",
                )
            )
    thin_keys: list[int] = []
    for e in range(8):
        eid = 700_000 + e
        thin_keys.append(eid)
        rows.append(
            _observed_row(
                event_key=eid,
                game_id=f"THIN{e:03d}",
                handler=1 + (e % N_POSITIONS),
                season=1926,
                league="NL",
            )
        )
    dataset_path = tmp_path / "dataset_floor.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)

    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=50,
        held_out_fold_count=999,
    )
    kept = set(inputs.event_keys.tolist()) | set(inputs.held_out.event_keys.tolist())
    assert kept.isdisjoint(set(thin_keys)), "thin 1926|NL cell must be dropped"
    assert "1926|NL" not in inputs.coords["season_league"]
    assert "1925|AL" in inputs.coords["season_league"]


def test_coverage_guard_raises_when_fe_sparse_on_production(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(
        tmp_path,
        n_production_events=300,
        production_result_family_rate=0.0,
    )
    with pytest.raises(ValueError, match=r"production slice.*result_family"):
        _ = prepare_ball_handler_inputs(
            dataset_path,
            min_events_per_season=1,
            held_out_fold_count=999,
        )


def test_coverage_guard_passes_when_fe_dense_on_production(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(
        tmp_path,
        n_production_events=300,
        production_result_family_rate=0.5,
    )
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.n_events > 0


def test_build_production_frame_encodes_against_training_vocab(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(
        tmp_path,
        n_production_events=200,
        production_result_family_rate=0.5,
    )
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    frame = build_ball_handler_production_frame(
        dataset_path, dimension=DIMENSION, fixed_effects=inputs.fixed_effects
    )
    production_keys = set(
        pl.scan_parquet(dataset_path)
        .filter(
            (pl.col("dimension") == DIMENSION)
            & (pl.col("observed_status") != "observed")
        )
        .select("event_key")
        .unique()
        .collect()
        .get_column("event_key")
        .to_list()
    )
    assert production_keys, "fixture should seed production-slice events"
    frame_keys = frame.event_keys.tolist()
    assert set(frame_keys) == production_keys
    assert len(frame_keys) == len(set(frame_keys)), "one row per production event_key"
    assert frame_keys == sorted(frame_keys)
    assert set(frame.fixed_effects.keys()) == set(inputs.fixed_effects.keys())
    for column, design in frame.fixed_effects.items():
        train_design = inputs.fixed_effects[column]
        assert design.levels == train_design.levels
        assert design.codes.shape[0] == frame.n_events
        n_levels = len(design.levels)
        for code in design.codes.tolist():
            assert code == -1 or 0 <= code < n_levels


def test_fixed_effect_columns_present(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(tmp_path)
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    for column in FIXED_EFFECT_COLUMNS:
        assert column in inputs.fixed_effects
        assert inputs.fixed_effects[column].codes.shape[0] == inputs.n_events
