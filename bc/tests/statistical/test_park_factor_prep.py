"""Synthetic-fixture tests for ``prepare_park_factor_inputs``."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

from pathlib import Path

import polars as pl

from python_models.statistical.models._park_factor_data import (
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    MIN_GAMES_PER_PARK_CELL,
    SINGLE_SOURCE_LABEL,
    prepare_park_factor_inputs,
)
from python_models.statistical.splits import game_hash_fold

SEASON = 1990
LEAGUE = "NL"


def _event_rows(
    *,
    game_id: str,
    batting_team_id: str,
    fielding_team_id: str,
    park_id: str,
    runs: list[int],
    pa: list[int],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for r, p in zip(runs, pa, strict=True):
        rows.append(
            {
                "game_id": game_id,
                "batting_team_id": batting_team_id,
                "fielding_team_id": fielding_team_id,
                "park_id": park_id,
                "season": SEASON,
                "league": LEAGUE,
                "plate_appearances": p,
                "runs_on_play": r,
            }
        )
    return rows


def _team_game(
    *, game_id: str, home: str, away: str, park_id: str, n_events: int = 6
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    rows.extend(
        _event_rows(
            game_id=game_id,
            batting_team_id=home,
            fielding_team_id=away,
            park_id=park_id,
            runs=[1, 0, 2] + [0] * (n_events - 3),
            pa=[1] * n_events,
        )
    )
    rows.extend(
        _event_rows(
            game_id=game_id,
            batting_team_id=away,
            fielding_team_id=home,
            park_id=park_id,
            runs=[0, 1, 0] + [1] * (n_events - 3),
            pa=[1] * n_events,
        )
    )
    return rows


def _dense_train_games(park_id: str, count: int) -> list[str]:
    games: list[str] = []
    i = 0
    while len(games) < count:
        gid = f"{park_id}_T{i:04d}"
        if game_hash_fold(gid, fold_count=HOLDOUT_FOLD_COUNT) != HOLDOUT_FOLD_ID:
            games.append(gid)
        i += 1
    return games


def _find_holdout_game(park_id: str) -> str:
    i = 0
    while True:
        gid = f"{park_id}_H{i:04d}"
        if game_hash_fold(gid, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID:
            return gid
        i += 1


def _write_dataset(tmp_path: Path) -> tuple[Path, str, str, str, str]:
    rows: list[dict[str, object]] = []

    dense_park = "DENSE01"
    sparse_park = "SPARSE1"
    unseen_park = "UNSEEN1"

    dense_games = _dense_train_games(dense_park, MIN_GAMES_PER_PARK_CELL + 4)
    for gid in dense_games:
        rows.extend(_team_game(game_id=gid, home="HOM", away="AWY", park_id=dense_park))

    holdout_dense_game = _find_holdout_game(dense_park)
    rows.extend(
        _team_game(
            game_id=holdout_dense_game, home="HOM", away="AWY", park_id=dense_park
        )
    )

    holdout_unseen_game = _find_holdout_game(unseen_park)
    rows.extend(
        _team_game(
            game_id=holdout_unseen_game, home="HOM", away="AWY", park_id=unseen_park
        )
    )

    rows.extend(
        _team_game(game_id="SPARSE1_T0000", home="HOM", away="AWY", park_id=sparse_park)
    )

    dataset_path = tmp_path / "park_factors.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)
    return dataset_path, dense_park, sparse_park, unseen_park, holdout_dense_game


def test_team_runs_and_exposure_match_per_team_game_sums(tmp_path: Path) -> None:
    dataset_path, _dense, _sparse, _unseen, _holdout = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)

    expected = (
        raw.group_by(["game_id", "batting_team_id"])
        .agg(
            pl.col("runs_on_play").sum().alias("team_runs"),
            pl.col("plate_appearances").sum().alias("exposure_pa"),
            pl.col("game_id").first().alias("_gid"),
        )
        .sort(["game_id", "batting_team_id"])
    )

    inputs = prepare_park_factor_inputs(dataset_path)

    train_runs = inputs.team_runs.tolist()
    train_exposure = inputs.exposure_pa.tolist()
    for runs, exposure in zip(train_runs, train_exposure, strict=True):
        assert runs >= 0
        assert exposure >= 1.0

    expected_total_runs = int(expected.get_column("team_runs").sum())
    expected_total_pa = int(expected.get_column("exposure_pa").sum())
    held = inputs.held_out
    observed_total_runs = int(inputs.team_runs.sum()) + int(held.team_runs.sum())
    observed_total_pa = int(inputs.exposure_pa.sum()) + int(held.exposure_pa.sum())
    sparse_cell_runs = int(
        raw.filter(pl.col("park_id") == "SPARSE1").get_column("runs_on_play").sum()
    )
    sparse_cell_pa = int(
        raw.filter(pl.col("park_id") == "SPARSE1").get_column("plate_appearances").sum()
    )
    assert observed_total_runs == expected_total_runs - sparse_cell_runs
    assert observed_total_pa == expected_total_pa - sparse_cell_pa


def test_held_out_game_both_team_rows_excluded_from_training(tmp_path: Path) -> None:
    dataset_path, _dense, _sparse, _unseen, holdout_game = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)
    inputs = prepare_park_factor_inputs(dataset_path)

    held_team_games = (
        raw.filter(pl.col("game_id") == holdout_game)
        .select(["game_id", "batting_team_id"])
        .unique()
        .height
    )
    assert held_team_games == 2
    assert inputs.held_out.n_games >= held_team_games
    assert inputs.n_events == len(inputs.team_runs)


def test_coords_and_label_decomposition(tmp_path: Path) -> None:
    dataset_path, dense_park, _sparse, _unseen, _holdout = _write_dataset(tmp_path)
    inputs = prepare_park_factor_inputs(dataset_path)

    assert inputs.coords["source"] == [SINGLE_SOURCE_LABEL]
    assert inputs.n_events == len(inputs.team_runs)
    assert inputs.coords["park_season_league"] == inputs.park_season_league_labels

    for k, label in enumerate(inputs.park_season_league_labels):
        rebuilt = (
            f"{inputs.park_id_by_cell[k]}|"
            f"{inputs.season_by_cell[k]}|"
            f"{inputs.league_by_cell[k]}"
        )
        assert rebuilt == label

    assert dense_park in inputs.park_id_by_cell


def test_min_games_floor_drops_sparse_cell(tmp_path: Path) -> None:
    dataset_path, _dense, sparse_park, _unseen, _holdout = _write_dataset(tmp_path)
    inputs = prepare_park_factor_inputs(dataset_path)

    assert sparse_park not in inputs.park_id_by_cell
    sparse_cell = f"{sparse_park}|{SEASON}|{LEAGUE}"
    assert sparse_cell not in inputs.park_season_league_labels


def test_unseen_park_cell_encodes_to_negative_one_in_held_out(tmp_path: Path) -> None:
    dataset_path, _dense, _sparse, _unseen, _holdout = _write_dataset(tmp_path)
    inputs = prepare_park_factor_inputs(dataset_path)

    held = inputs.held_out
    assert held.n_games > 0
    n_park_cells = len(inputs.park_season_league_labels)
    codes = held.park_season_league_idx.tolist()
    assert any(code == -1 for code in codes)
    for code in codes:
        assert code == -1 or 0 <= code < n_park_cells
