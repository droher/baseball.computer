"""Synthetic-fixture tests for ``prepare_run_expectancy_inputs``."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

from pathlib import Path

import polars as pl

from python_models.statistical.models._run_values_data import (
    BASE_STATES,
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    MIN_EVENTS_PER_CELL,
    SINGLE_SOURCE_LABEL,
    prepare_run_expectancy_inputs,
)
from python_models.statistical.splits import game_hash_fold

SEASON = 1933
LEAGUE = "AL"


def _start_key(*, season: int, league: str, outs: int, base: int) -> str:
    return f"{season}_{league}_{outs}_{base}"


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


def _cell_rows(
    *, game_id: str, outs: int, base: int, runs: list[int]
) -> list[dict[str, object]]:
    key = _start_key(season=SEASON, league=LEAGUE, outs=outs, base=base)
    return [
        {
            "game_id": game_id,
            "run_expectancy_start_key": key,
            "runs_to_end_of_inning": r,
        }
        for r in runs
    ]


def _write_dataset(
    tmp_path: Path,
) -> tuple[Path, tuple[int, int], tuple[int, int], tuple[int, int], str]:
    rows: list[dict[str, object]] = []

    dense_a = (0, 0)
    dense_b = (2, 7)
    thin = (1, 3)
    unseen = (1, 5)

    runs_cycle = [0, 1, 2, 0, 1]

    for cell in (dense_a, dense_b):
        outs, base = cell
        for gid in _train_games(f"P{outs}{base}", MIN_EVENTS_PER_CELL + 5):
            rows.extend(
                _cell_rows(game_id=gid, outs=outs, base=base, runs=[runs_cycle[0]])
            )

    thin_outs, thin_base = thin
    for gid in _train_games("THIN", MIN_EVENTS_PER_CELL - 5):
        rows.extend(_cell_rows(game_id=gid, outs=thin_outs, base=thin_base, runs=[1]))

    holdout_dense = _find_holdout_game("HOLDD")
    rows.extend(
        _cell_rows(
            game_id=holdout_dense,
            outs=dense_a[0],
            base=dense_a[1],
            runs=runs_cycle,
        )
    )

    holdout_unseen = _find_holdout_game("HOLDU")
    rows.extend(
        _cell_rows(
            game_id=holdout_unseen,
            outs=unseen[0],
            base=unseen[1],
            runs=[2, 3],
        )
    )

    dataset_path = tmp_path / "run_values.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)
    return dataset_path, dense_a, dense_b, thin, holdout_dense


def test_sum_runs_and_event_count_match_groupby(tmp_path: Path) -> None:
    dataset_path, _a, _b, _thin, _holdout = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)
    inputs = prepare_run_expectancy_inputs(dataset_path)

    holdout_games = [
        g
        for g in raw.get_column("game_id").unique().to_list()
        if game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
    ]
    train = raw.filter(~pl.col("game_id").is_in(holdout_games))

    expected = (
        train.group_by("run_expectancy_start_key")
        .agg(
            pl.col("runs_to_end_of_inning").sum().alias("sum_runs"),
            pl.len().alias("n_events"),
        )
        .filter(pl.col("n_events") >= MIN_EVENTS_PER_CELL)
        .sort("run_expectancy_start_key")
    )

    got = pl.DataFrame(
        {
            "run_expectancy_start_key": inputs.cell_labels,
            "sum_runs": inputs.sum_runs.tolist(),
            "n_events": inputs.cell_event_count.tolist(),
        }
    ).sort("run_expectancy_start_key")

    assert (
        got.get_column("run_expectancy_start_key").to_list()
        == expected.get_column("run_expectancy_start_key").to_list()
    )
    assert (
        got.get_column("sum_runs").to_list()
        == expected.get_column("sum_runs").to_list()
    )
    assert (
        got.get_column("n_events").to_list()
        == expected.get_column("n_events").to_list()
    )
    assert inputs.n_events == int(inputs.cell_event_count.sum())
    assert inputs.n_cells == len(inputs.cell_labels)


def test_state_parsing_round_trips(tmp_path: Path) -> None:
    dataset_path, _a, _b, _thin, _holdout = _write_dataset(tmp_path)
    inputs = prepare_run_expectancy_inputs(dataset_path)

    assert inputs.coords["source"] == [SINGLE_SOURCE_LABEL]
    assert inputs.coords["cell"] == inputs.cell_labels
    assert inputs.coords["state"] == inputs.state_labels

    for k, label in enumerate(inputs.cell_labels):
        rebuilt = (
            f"{inputs.season_by_cell[k]}_"
            f"{inputs.league_by_cell[k]}_"
            f"{inputs.outs_by_cell[k]}_"
            f"{inputs.base_state_by_cell[k]}"
        )
        assert rebuilt == label
        assert inputs.state_by_cell[k] == (
            inputs.outs_by_cell[k] * BASE_STATES + inputs.base_state_by_cell[k]
        )
        state_label = inputs.state_labels[inputs.cell_state_idx[k]]
        assert state_label == f"{inputs.outs_by_cell[k]}_{inputs.base_state_by_cell[k]}"


def test_min_events_floor_drops_thin_cell(tmp_path: Path) -> None:
    dataset_path, _a, _b, thin, _holdout = _write_dataset(tmp_path)
    inputs = prepare_run_expectancy_inputs(dataset_path)

    thin_key = _start_key(season=SEASON, league=LEAGUE, outs=thin[0], base=thin[1])
    assert thin_key not in inputs.cell_labels
    assert all(
        count >= MIN_EVENTS_PER_CELL for count in inputs.cell_event_count.tolist()
    )


def test_holdout_games_disjoint_and_unseen_encodes_negative_one(
    tmp_path: Path,
) -> None:
    dataset_path, _a, _b, _thin, holdout_game = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)
    inputs = prepare_run_expectancy_inputs(dataset_path)

    held = inputs.held_out
    assert held.n_cells > 0

    holdout_games = {
        g
        for g in raw.get_column("game_id").unique().to_list()
        if game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
    }
    assert holdout_game in holdout_games

    n_cells = inputs.n_cells
    for code in held.cell_idx.tolist():
        assert 0 <= code < n_cells
    for code in held.cell_state_idx.tolist():
        assert 0 <= code < len(inputs.state_labels)

    unseen_key = _start_key(season=SEASON, league=LEAGUE, outs=1, base=5)
    assert unseen_key not in inputs.cell_labels
    held_labels = (
        raw.filter(pl.col("game_id").is_in(holdout_games))
        .group_by("run_expectancy_start_key")
        .agg(pl.len())
        .get_column("run_expectancy_start_key")
        .to_list()
    )
    assert unseen_key in held_labels
    assert held.n_cells < len(held_labels)
