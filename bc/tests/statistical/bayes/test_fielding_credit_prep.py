"""Synthetic-fixture tests for ``prepare_event_credit_inputs`` (v1.5 dual-arm)."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.models._credit_data import (
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    N_POSITIONS,
    REAL_UNKNOWN_RATES_BY_POSITION,
    prepare_event_credit_inputs,
)
from python_models.statistical.splits import game_hash_fold


DEFAULT_N_GAMES = 16
DEFAULT_EVENTS_PER_GAME = 12


def _synthetic_dataset(
    tmp_path: Path,
    *,
    n_games: int = DEFAULT_N_GAMES,
    events_per_game: int = DEFAULT_EVENTS_PER_GAME,
    season: int = 1925,
    fielding_team_id: str = "TEX",
    seed: int = 20260520,
) -> tuple[Path, dict[str, list[str]]]:
    """Build a fixture where every event has known_credit=1 at a chosen position.

    The position distribution loosely mirrors the v1 per-position
    weights so per-cell mask intensity has something to scale.
    """
    rng = np.random.default_rng(seed)
    lineups: dict[str, list[str]] = {
        f"G{g:03d}": [f"P{g:02d}_{p:02d}" for p in range(N_POSITIONS)]
        for g in range(n_games)
    }
    rows: list[dict[str, object]] = []
    weights = np.asarray(REAL_UNKNOWN_RATES_BY_POSITION, dtype=np.float64)
    weights = weights / weights.sum()
    for g in range(n_games):
        for e in range(events_per_game):
            eid = 100_000 + g * events_per_game + e
            true_pos_0 = int(rng.choice(N_POSITIONS, p=weights))
            lineup = lineups[f"G{g:03d}"]
            for k_pos in range(1, N_POSITIONS + 1):
                for ct in ("putout", "assist", "error"):
                    known_credit = (
                        1.0
                        if ct == "putout" and (k_pos - 1) == true_pos_0
                        else 0.0
                    )
                    rows.append(
                        {
                            "event_key": eid,
                            "player_id": lineup[k_pos - 1],
                            "fielding_position": k_pos,
                            "credit_type": ct,
                            "known_credit": known_credit,
                            "unknown_credit_need": 0.0,
                            "fielding_evidence_status": "complete_with_zero_unknowns",
                            "gap_class": "complete",
                            "personnel_hard_mask_available": True,
                            "eligible_for_allocation": False,
                            "game_id": f"G{g:03d}",
                            "season": season,
                            "league": "AL",
                            "game_type": "RegularSeason",
                            "source_type": "pbp",
                            "source_family": "play_by_play",
                            "park_id": "ARL01",
                            "scorer": "scorerA",
                            "fielding_team_id": fielding_team_id,
                            "result_family": "out_in_play",
                            "base_state_start": 0,
                            "outs_start": 1,
                            "frame_start": "Top",
                            "alignment_regime": "shift_growth_era",
                            "direct_handler_position": k_pos if known_credit > 0 else None,
                            "personnel_confidence": "high",
                            "context_confidence": "high",
                            "exposure_status": "complete",
                        }
                    )
    df = pl.DataFrame(rows)
    dataset_path = tmp_path / "dataset.parquet"
    df.write_parquet(dataset_path)
    return dataset_path, lineups


def test_filter_keeps_well_attributed_events_only(tmp_path: Path) -> None:
    dataset_path, _ = _synthetic_dataset(tmp_path)
    df = pl.read_parquet(dataset_path)
    df = df.with_columns(
        pl.when(pl.col("event_key") == 100_000)
        .then(0.0)
        .otherwise(pl.col("known_credit"))
        .alias("known_credit")
    )
    df.write_parquet(dataset_path)
    inputs = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        min_events_per_season=1,
        held_out_fold_count=DEFAULT_N_GAMES + 1,
    )
    assert 100_000 not in inputs.event_keys.tolist()


def test_indexer_contiguity(tmp_path: Path) -> None:
    dataset_path, _ = _synthetic_dataset(tmp_path)
    inputs = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        min_events_per_season=1,
        held_out_fold_count=DEFAULT_N_GAMES + 1,
    )
    for name, idx in (
        ("season", inputs.season_idx),
        ("scorer", inputs.scorer_idx),
        ("park", inputs.park_idx),
        ("source", inputs.source_idx),
    ):
        assert set(np.unique(idx).tolist()) == set(range(len(inputs.coords[name])))


def test_held_out_games_excluded_from_training(tmp_path: Path) -> None:
    dataset_path, _ = _synthetic_dataset(tmp_path, n_games=20, events_per_game=8)
    inputs = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        min_events_per_season=1,
    )
    df = pl.read_parquet(dataset_path)
    all_game_ids = sorted(df.get_column("game_id").unique().to_list())
    held_games = {
        g
        for g in all_game_ids
        if game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
    }
    assert inputs.held_out.n_events > 0
    train_event_keys = set(inputs.event_keys.tolist())
    holdout_event_keys = set(inputs.held_out.event_keys.tolist())
    assert train_event_keys.isdisjoint(holdout_event_keys)
    held_event_keys_from_df = (
        df.filter(pl.col("game_id").is_in(list(held_games)))
        .get_column("event_key")
        .unique()
        .to_list()
    )
    for ek in inputs.event_keys.tolist():
        assert ek not in held_event_keys_from_df


def test_supervised_and_aggregate_events_are_disjoint(tmp_path: Path) -> None:
    dataset_path, _ = _synthetic_dataset(tmp_path)
    inputs = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        min_events_per_season=1,
        held_out_fold_count=DEFAULT_N_GAMES + 1,
    )
    sup_idx = set(inputs.Y_supervised_event_idx.tolist())
    agg_idx = set(inputs.aggregate_event_idx.tolist())
    assert sup_idx.isdisjoint(agg_idx)
    assert int(inputs.is_masked.sum()) + len(sup_idx) == inputs.n_events


def test_mask_is_deterministic_given_seed(tmp_path: Path) -> None:
    dataset_path, _ = _synthetic_dataset(tmp_path)
    a = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        seed=42,
        min_events_per_season=1,
        held_out_fold_count=DEFAULT_N_GAMES + 1,
    )
    b = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        seed=42,
        min_events_per_season=1,
        held_out_fold_count=DEFAULT_N_GAMES + 1,
    )
    assert sorted(a.event_keys.tolist()) == sorted(b.event_keys.tolist())
    assert (a.is_masked == b.is_masked).all()


def test_aggregate_targets_match_hidden_known_credit(tmp_path: Path) -> None:
    dataset_path, _ = _synthetic_dataset(tmp_path)
    inputs = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        min_events_per_season=1,
        held_out_fold_count=DEFAULT_N_GAMES + 1,
    )
    expected_total = float(inputs.U[inputs.is_masked].sum())
    assert float(inputs.aggregate_targets.sum()) == pytest.approx(
        expected_total, rel=0.0, abs=1e-9
    )
    if inputs.n_targets > 0:
        sigmas = sorted(set(inputs.aggregate_sigma.tolist()))
        assert sigmas == [0.5]


def test_indicator_matrix_shape_consistency(tmp_path: Path) -> None:
    dataset_path, _ = _synthetic_dataset(tmp_path)
    inputs = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        min_events_per_season=1,
        held_out_fold_count=DEFAULT_N_GAMES + 1,
    )
    if inputs.n_targets > 0:
        assert (
            inputs.aggregate_event_idx.shape
            == inputs.aggregate_position_idx.shape
            == inputs.aggregate_row_idx.shape
        )
        assert int(inputs.aggregate_event_idx.max()) < inputs.n_events
        assert int(inputs.aggregate_position_idx.max()) < inputs.n_positions
        assert int(inputs.aggregate_row_idx.max()) < inputs.n_targets


def test_per_position_mask_rate_roughly_matches_weights(tmp_path: Path) -> None:
    """Mass-fixture test: at a high mask rate, mask shares per position track weights."""
    dataset_path, _ = _synthetic_dataset(
        tmp_path, n_games=80, events_per_game=20, season=1944
    )
    inputs = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        min_events_per_season=1,
        held_out_fold_count=DEFAULT_N_GAMES + 1,
    )
    masked = inputs.is_masked
    assert masked.any(), "expected non-trivial mask in 1944 PBP cell"
    n_masked = int(masked.sum())
    grids = []
    for ek in inputs.event_keys[masked].tolist():
        rows = (
            pl.read_parquet(dataset_path)
            .filter(
                (pl.col("event_key") == int(ek))
                & (pl.col("credit_type") == "putout")
            )
            .sort("fielding_position")
            .get_column("known_credit")
            .to_list()
        )
        grids.append(rows)
    true_pos = np.asarray([int(np.argmax(g)) for g in grids], dtype=np.int64)
    shares = np.bincount(true_pos, minlength=N_POSITIONS) / max(n_masked, 1)
    weights = np.asarray(REAL_UNKNOWN_RATES_BY_POSITION, dtype=np.float64)
    dominant = int(np.argmax(weights))
    assert shares[dominant] > shares.mean(), (
        f"dominant position {dominant + 1} share {shares[dominant]:.3f} not above mean {shares.mean():.3f}"
    )
