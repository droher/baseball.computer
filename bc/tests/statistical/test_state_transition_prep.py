"""Synthetic-fixture tests for ``prepare_state_transition_inputs``."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from python_models.statistical.models._state_transition_data import (
    BASE_STATES,
    INNING_END_INDEX,
    INNING_END_OUTS,
    MIN_EVENTS_PER_CELL,
    N_END_CLASSES,
    N_START_STATES,
    SINGLE_SOURCE_LABEL,
    prepare_state_transition_inputs,
)
from python_models.statistical.splits import game_hash_fold

SEASON = 1933
LEAGUE = "AL"
HOLDOUT_FOLD_COUNT = 10
HOLDOUT_FOLD_ID = 0


def _key(outs: int, base: int) -> str:
    return f"{SEASON}_{LEAGUE}_{outs}_{base}"


def _train_games(prefix: str, count: int) -> list[str]:
    games: list[str] = []
    i = 0
    while len(games) < count:
        gid = f"{prefix}_T{i:05d}"
        if game_hash_fold(gid, fold_count=HOLDOUT_FOLD_COUNT) != HOLDOUT_FOLD_ID:
            games.append(gid)
        i += 1
    return games


def _find_holdout_game(prefix: str) -> str:
    i = 0
    while True:
        gid = f"{prefix}_H{i:05d}"
        if game_hash_fold(gid, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID:
            return gid
        i += 1


def _write_dataset(tmp_path: Path) -> Path:
    rows: list[dict[str, object]] = []

    for gid in _train_games("S00", MIN_EVENTS_PER_CELL + 5):
        rows.append(
            {
                "game_id": gid,
                "season": SEASON,
                "league": LEAGUE,
                "run_expectancy_start_key": _key(0, 0),
                "run_expectancy_end_key": _key(1, 0),
                "runs_on_play": 0,
            }
        )
    for gid in _train_games("S00END", MIN_EVENTS_PER_CELL + 5):
        rows.append(
            {
                "game_id": gid,
                "season": SEASON,
                "league": LEAGUE,
                "run_expectancy_start_key": _key(2, 0),
                "run_expectancy_end_key": _key(3, 0),
                "runs_on_play": 1,
            }
        )

    thin = _find_holdout_game("THINHOLD")
    rows.append(
        {
            "game_id": thin,
            "season": SEASON,
            "league": LEAGUE,
            "run_expectancy_start_key": _key(0, 1),
            "run_expectancy_end_key": _key(0, 3),
            "runs_on_play": 0,
        }
    )

    path = tmp_path / "transition.parquet"
    pl.DataFrame(rows).write_parquet(path)
    return path


def test_vocab_and_coords(tmp_path: Path) -> None:
    path = _write_dataset(tmp_path)
    inputs = prepare_state_transition_inputs(path)

    assert inputs.coords["source"] == [SINGLE_SOURCE_LABEL]
    assert len(inputs.start_state_labels) == N_START_STATES == 24
    assert len(inputs.end_class_labels) == N_END_CLASSES == 25
    assert inputs.counts.shape[1] == N_END_CLASSES
    assert inputs.coords["cell"] == inputs.cell_labels
    assert inputs.coords["end_class_nonref"] == inputs.end_class_labels[1:]


def test_start_and_end_index_mapping(tmp_path: Path) -> None:
    path = _write_dataset(tmp_path)
    inputs = prepare_state_transition_inputs(path)

    rows = dict(zip(inputs.cell_labels, inputs.cell_start_idx.tolist()))
    assert rows["1933|AL|0_0"] == 0
    assert rows["1933|AL|2_0"] == 16

    end_class_of = {c: inputs.counts[i] for i, c in enumerate(inputs.cell_labels)}
    start_cell = end_class_of["1933|AL|0_0"]
    assert int(start_cell[1 * 8 + 0]) == MIN_EVENTS_PER_CELL + 5
    assert int(start_cell.sum()) == MIN_EVENTS_PER_CELL + 5

    end_cell = end_class_of["1933|AL|2_0"]
    assert int(end_cell[INNING_END_INDEX]) == MIN_EVENTS_PER_CELL + 5
    assert int(end_cell[: N_START_STATES].sum()) == 0


def test_per_cell_counts_sum_matches_events(tmp_path: Path) -> None:
    path = _write_dataset(tmp_path)
    raw = pl.read_parquet(path)
    inputs = prepare_state_transition_inputs(path)

    holdout_games = {
        g
        for g in raw.get_column("game_id").unique().to_list()
        if game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
    }
    train = raw.filter(~pl.col("game_id").is_in(holdout_games))
    assert inputs.n_events == train.height


def test_min_events_floor_and_holdout(tmp_path: Path) -> None:
    path = _write_dataset(tmp_path)
    inputs = prepare_state_transition_inputs(path)

    assert "1933|AL|0_1" not in inputs.cell_labels
    for total in inputs.counts.sum(axis=1).tolist():
        assert total >= MIN_EVENTS_PER_CELL


def _end_outs(end_class: int) -> int:
    return INNING_END_OUTS if end_class == INNING_END_INDEX else end_class // BASE_STATES


def test_reachable_mask_matches_outs_monotonicity(tmp_path: Path) -> None:
    path = _write_dataset(tmp_path)
    inputs = prepare_state_transition_inputs(path)

    mask = inputs.reachable_mask
    assert mask.shape == (N_START_STATES, N_END_CLASSES)
    for start in range(N_START_STATES):
        start_outs = start // BASE_STATES
        for end in range(N_END_CLASSES):
            assert bool(mask[start, end]) == (_end_outs(end) >= start_outs)


def test_no_observed_count_in_unreachable_entry(tmp_path: Path) -> None:
    path = _write_dataset(tmp_path)
    inputs = prepare_state_transition_inputs(path)

    cell_reachable = inputs.reachable_mask[inputs.cell_start_idx]
    assert int(inputs.counts[~cell_reachable].sum()) == 0


def test_reference_is_reachable_and_modal(tmp_path: Path) -> None:
    path = _write_dataset(tmp_path)
    inputs = prepare_state_transition_inputs(path)

    ref = inputs.ref_class_by_start
    assert ref.shape == (N_START_STATES,)
    agg = np.zeros((N_START_STATES, N_END_CLASSES), dtype=np.int64)
    for cell, start in enumerate(inputs.cell_start_idx.tolist()):
        agg[start] += inputs.counts[cell]
    for start in range(N_START_STATES):
        assert bool(inputs.reachable_mask[start, ref[start]])
        if agg[start].sum() > 0:
            assert int(agg[start, ref[start]]) == int(agg[start].max())
