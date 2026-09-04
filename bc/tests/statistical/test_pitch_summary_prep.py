"""Synthetic-fixture tests for ``prepare_pitch_summary_inputs``."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.models import _pitch_summary_data as prep_module
from python_models.statistical.models._pitch_summary_data import (
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    MAX_BALLS,
    MAX_STRIKES,
    MIN_EVENTS_PER_CELL,
    N_CLASSES,
    SINGLE_SOURCE_LABEL,
    STRIKEOUT_FAMILY,
    UNKNOWN_RESULT,
    WALK_FAMILY,
    prepare_pitch_summary_inputs,
    reachable_classes,
)
from python_models.statistical.splits import game_hash_fold

SEASON = 2023
LEAGUE = "AL"

IMPOSSIBLE_STRIKEOUT_EVENTS = 3
IMPOSSIBLE_WALK_EVENTS = 2


def _cell_key(result_family: str) -> str:
    return f"{result_family}|{SEASON}|{LEAGUE}"


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


def _row(
    *,
    game_id: str,
    result_family: str | None,
    balls: int,
    strikes: int,
    has_count: bool = True,
) -> dict[str, object]:
    return {
        "game_id": game_id,
        "has_count": has_count,
        "count_balls_raw": str(balls),
        "count_strikes_raw": str(strikes),
        "result_family": result_family,
        "season": SEASON,
        "league": LEAGUE,
    }


def _write_dataset(tmp_path: Path) -> tuple[Path, str, str]:
    rows: list[dict[str, object]] = []

    strikeout_games = _train_games("STRIKEOUT", MIN_EVENTS_PER_CELL + 5)
    for i, gid in enumerate(strikeout_games):
        rows.append(
            _row(
                game_id=gid,
                result_family=STRIKEOUT_FAMILY,
                balls=i % (MAX_BALLS + 1),
                strikes=MAX_STRIKES,
            )
        )
    for gid in strikeout_games[:IMPOSSIBLE_STRIKEOUT_EVENTS]:
        rows.append(
            _row(game_id=gid, result_family=STRIKEOUT_FAMILY, balls=0, strikes=0)
        )

    walk_games = _train_games("WALK", MIN_EVENTS_PER_CELL + 5)
    for i, gid in enumerate(walk_games):
        rows.append(
            _row(
                game_id=gid,
                result_family=WALK_FAMILY,
                balls=MAX_BALLS,
                strikes=i % (MAX_STRIKES + 1),
            )
        )
    for gid in walk_games[:IMPOSSIBLE_WALK_EVENTS]:
        rows.append(_row(game_id=gid, result_family=WALK_FAMILY, balls=1, strikes=1))

    for gid in _train_games("OUTINPLAY", MIN_EVENTS_PER_CELL + 5):
        rows.append(_row(game_id=gid, result_family="out_in_play", balls=1, strikes=1))

    for gid in _train_games("UNKNOWN", MIN_EVENTS_PER_CELL + 5):
        rows.append(_row(game_id=gid, result_family=None, balls=0, strikes=0))

    thin_family = "hbp"
    for gid in _train_games("THIN", MIN_EVENTS_PER_CELL - 5):
        rows.append(_row(game_id=gid, result_family=thin_family, balls=2, strikes=0))

    for gid in _train_games("EXCLUDED", 10):
        rows.append(
            _row(
                game_id=gid,
                result_family=STRIKEOUT_FAMILY,
                balls=0,
                strikes=0,
                has_count=False,
            )
        )

    holdout_game = _find_holdout_game("HOLD")
    for _ in range(4):
        rows.append(
            _row(
                game_id=holdout_game,
                result_family=STRIKEOUT_FAMILY,
                balls=3,
                strikes=MAX_STRIKES,
            )
        )
    rows.append(
        _row(game_id=holdout_game, result_family=STRIKEOUT_FAMILY, balls=1, strikes=0)
    )

    dataset_path = tmp_path / "pitch_summary.parquet"
    pl.DataFrame(
        rows,
        schema={
            "game_id": pl.Utf8,
            "has_count": pl.Boolean,
            "count_balls_raw": pl.Utf8,
            "count_strikes_raw": pl.Utf8,
            "result_family": pl.Utf8,
            "season": pl.Int64,
            "league": pl.Utf8,
        },
    ).write_parquet(dataset_path)
    return dataset_path, thin_family, holdout_game


def _recompute_train_cell_counts(raw: pl.DataFrame) -> pl.DataFrame:
    holdout_games = [
        g
        for g in raw.get_column("game_id").unique().to_list()
        if game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
    ]
    frame = (
        raw.filter(
            pl.col("has_count")
            & ~pl.col("game_id").is_in(holdout_games)
            & pl.col("count_balls_raw").cast(pl.Int64).is_between(0, 3)
            & pl.col("count_strikes_raw").cast(pl.Int64).is_between(0, MAX_STRIKES)
        )
        .with_columns(
            pl.col("result_family").fill_null(UNKNOWN_RESULT).alias("result_family"),
            (
                pl.col("count_balls_raw").cast(pl.Int64) * (MAX_STRIKES + 1)
                + pl.col("count_strikes_raw").cast(pl.Int64)
            ).alias("final_class"),
        )
        .with_columns(
            (
                pl.col("result_family")
                + pl.lit("|")
                + pl.col("season").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("league")
            ).alias("cell")
        )
    )
    keep = [
        bool(reachable_classes(str(family))[int(final_class)])
        for family, final_class in frame.select(
            ["result_family", "final_class"]
        ).iter_rows()
    ]
    return (
        frame.filter(pl.Series(keep))
        .group_by(["cell", "final_class"])
        .agg(pl.len().alias("n"))
    )


def test_reachability_rule_matches_structural_constraint() -> None:
    labels, balls, strikes = prep_module._class_labels()
    assert len(labels) == N_CLASSES
    strikeout = reachable_classes(STRIKEOUT_FAMILY)
    walk = reachable_classes(WALK_FAMILY)
    for j in range(N_CLASSES):
        assert bool(strikeout[j]) == (strikes[j] == MAX_STRIKES)
        assert bool(walk[j]) == (balls[j] == MAX_BALLS)
    for family in ("hit", "out_in_play", "hbp", UNKNOWN_RESULT, "sacrifice"):
        assert reachable_classes(family).all()
    assert int(strikeout.sum()) == MAX_BALLS + 1
    assert int(walk.sum()) == MAX_STRIKES + 1


def test_counts_row_sums_match_groupby(tmp_path: Path) -> None:
    dataset_path, _thin, _holdout = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)
    inputs = prepare_pitch_summary_inputs(dataset_path)

    recomputed = _recompute_train_cell_counts(raw)
    kept = (
        recomputed.group_by("cell")
        .agg(pl.col("n").sum().alias("total"))
        .filter(pl.col("total") >= MIN_EVENTS_PER_CELL)
        .get_column("cell")
        .to_list()
    )

    cell_total = {
        cell: int(recomputed.filter(pl.col("cell") == cell).get_column("n").sum())
        for cell in kept
    }
    got = {
        cell: int(inputs.counts[i].sum()) for i, cell in enumerate(inputs.cell_labels)
    }
    assert got == cell_total
    assert inputs.n_events == int(inputs.counts.sum())
    assert inputs.n_cells == len(inputs.cell_labels)


def test_impossible_cells_dropped_and_logged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    dataset_path, _thin, _holdout = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)
    with caplog.at_level(logging.INFO, logger=prep_module.__name__):
        inputs = prepare_pitch_summary_inputs(dataset_path)

    cell_reachable = inputs.cell_reachable_mask
    assert int(inputs.counts[~cell_reachable].sum()) == 0
    assert int(inputs.held_out.counts[~inputs.reachable_mask[inputs.held_out.cell_result_idx]].sum()) == 0

    def _raw_impossible(family: str) -> int:
        frame = raw.filter(
            pl.col("has_count") & (pl.col("result_family") == family)
        ).with_columns(
            (
                pl.col("count_balls_raw").cast(pl.Int64) * (MAX_STRIKES + 1)
                + pl.col("count_strikes_raw").cast(pl.Int64)
            ).alias("final_class")
        )
        mask = reachable_classes(family)
        return sum(
            1 for c in frame.get_column("final_class").to_list() if not mask[int(c)]
        )

    expected = {
        STRIKEOUT_FAMILY: _raw_impossible(STRIKEOUT_FAMILY),
        WALK_FAMILY: _raw_impossible(WALK_FAMILY),
    }
    assert expected[STRIKEOUT_FAMILY] > 0 and expected[WALK_FAMILY] > 0

    logged: dict[str, int] = {}
    for record in caplog.records:
        if "structurally impossible" in record.getMessage():
            assert isinstance(record.args, tuple)
            dropped, family = record.args
            logged[str(family)] = int(str(dropped))
    assert logged == expected


def test_reference_class_is_reachable_and_modal(tmp_path: Path) -> None:
    dataset_path, _thin, _holdout = _write_dataset(tmp_path)
    inputs = prepare_pitch_summary_inputs(dataset_path)

    assert inputs.reachable_mask.shape == (inputs.n_result_families, N_CLASSES)
    assert inputs.ref_class_by_result.shape == (inputs.n_result_families,)
    per_result = np.zeros_like(inputs.reachable_mask, dtype=np.int64)
    np.add.at(per_result, inputs.cell_result_idx, inputs.counts)
    for r, family in enumerate(inputs.result_family_labels):
        ref = int(inputs.ref_class_by_result[r])
        assert inputs.reachable_mask[r, ref]
        np.testing.assert_array_equal(inputs.reachable_mask[r], reachable_classes(family))
        reachable_counts = np.where(inputs.reachable_mask[r], per_result[r], -1)
        assert per_result[r, ref] == reachable_counts.max()


def test_class_encoding_round_trips(tmp_path: Path) -> None:
    dataset_path, _thin, _holdout = _write_dataset(tmp_path)
    inputs = prepare_pitch_summary_inputs(dataset_path)

    assert inputs.n_classes == N_CLASSES == 12
    assert len(inputs.class_labels) == 12

    for j, label in enumerate(inputs.class_labels):
        b = inputs.balls_by_class[j]
        s = inputs.strikes_by_class[j]
        assert label == f"b{b}_s{s}"
        assert j == b * (MAX_STRIKES + 1) + s

    assert inputs.coords["source"] == [SINGLE_SOURCE_LABEL]
    assert inputs.coords["class"] == inputs.class_labels
    assert inputs.coords["cell"] == inputs.cell_labels
    assert inputs.coords["result_family"] == inputs.result_family_labels
    assert "class_nonref" not in inputs.coords

    for k, label in enumerate(inputs.cell_labels):
        rebuilt = (
            f"{inputs.result_by_cell[k]}|"
            f"{inputs.season_by_cell[k]}|"
            f"{inputs.league_by_cell[k]}"
        )
        assert rebuilt == label
        family = inputs.result_family_labels[inputs.cell_result_idx[k]]
        assert family == inputs.result_by_cell[k]


def test_strikeout_cell_lands_in_two_strike_classes(tmp_path: Path) -> None:
    dataset_path, _thin, _holdout = _write_dataset(tmp_path)
    inputs = prepare_pitch_summary_inputs(dataset_path)

    strikeout_cell = _cell_key(STRIKEOUT_FAMILY)
    assert strikeout_cell in inputs.cell_labels
    row = inputs.counts[inputs.cell_labels.index(strikeout_cell)]
    two_strike_classes = {
        j for j, s in enumerate(inputs.strikes_by_class) if s == MAX_STRIKES
    }
    nonzero = {j for j in range(inputs.n_classes) if int(row[j]) > 0}
    assert nonzero
    assert nonzero <= two_strike_classes


def test_unknown_result_family_bucketed(tmp_path: Path) -> None:
    dataset_path, _thin, _holdout = _write_dataset(tmp_path)
    inputs = prepare_pitch_summary_inputs(dataset_path)

    assert UNKNOWN_RESULT in inputs.result_family_labels
    assert _cell_key(UNKNOWN_RESULT) in inputs.cell_labels


def test_min_events_floor_drops_thin_cell(tmp_path: Path) -> None:
    dataset_path, thin_family, _holdout = _write_dataset(tmp_path)
    inputs = prepare_pitch_summary_inputs(dataset_path)

    assert _cell_key(thin_family) not in inputs.cell_labels
    assert all(
        int(inputs.counts[i].sum()) >= MIN_EVENTS_PER_CELL
        for i in range(inputs.n_cells)
    )


def test_has_count_false_rows_excluded(tmp_path: Path) -> None:
    dataset_path, _thin, _holdout = _write_dataset(tmp_path)
    inputs = prepare_pitch_summary_inputs(dataset_path)

    strikeout_cell = _cell_key(STRIKEOUT_FAMILY)
    row = inputs.counts[inputs.cell_labels.index(strikeout_cell)]
    b0_s0 = next(
        j
        for j in range(inputs.n_classes)
        if inputs.balls_by_class[j] == 0 and inputs.strikes_by_class[j] == 0
    )
    assert int(row[b0_s0]) == 0


def test_holdout_disjoint_and_cells_subset_of_training(tmp_path: Path) -> None:
    dataset_path, _thin, holdout_game = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)
    inputs = prepare_pitch_summary_inputs(dataset_path)

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
    for code in held.cell_result_idx.tolist():
        assert 0 <= code < len(inputs.result_family_labels)

    held_labels = {inputs.cell_labels[code] for code in held.cell_idx.tolist()}
    assert held_labels <= set(inputs.cell_labels)
    assert held.counts.shape == (held.n_cells, inputs.n_classes)
