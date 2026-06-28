"""Synthetic-fixture tests for ``prepare_pitch_summary_inputs``."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

from pathlib import Path

import polars as pl

from python_models.statistical.models._pitch_summary_data import (
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    MAX_STRIKES,
    MIN_EVENTS_PER_CELL,
    N_CLASSES,
    SINGLE_SOURCE_LABEL,
    UNKNOWN_RESULT,
    prepare_pitch_summary_inputs,
)
from python_models.statistical.splits import game_hash_fold

SEASON = 2023
LEAGUE = "AL"


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

    for gid in _train_games("STRIKEOUT", MIN_EVENTS_PER_CELL + 5):
        rows.append(
            _row(game_id=gid, result_family="strikeout", balls=2, strikes=MAX_STRIKES)
        )

    for gid in _train_games("OUTINPLAY", MIN_EVENTS_PER_CELL + 5):
        rows.append(_row(game_id=gid, result_family="out_in_play", balls=1, strikes=1))

    for gid in _train_games("UNKNOWN", MIN_EVENTS_PER_CELL + 5):
        rows.append(_row(game_id=gid, result_family=None, balls=0, strikes=0))

    thin_family = "walk"
    for gid in _train_games("THIN", MIN_EVENTS_PER_CELL - 5):
        rows.append(
            _row(
                game_id=gid, result_family=thin_family, balls=MAX_STRIKES + 1, strikes=0
            )
        )

    for gid in _train_games("EXCLUDED", 10):
        rows.append(
            _row(
                game_id=gid,
                result_family="strikeout",
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
                result_family="strikeout",
                balls=3,
                strikes=MAX_STRIKES,
            )
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
    return (
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
        .group_by(["cell", "final_class"])
        .agg(pl.len().alias("n"))
    )


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

    strikeout_cell = _cell_key("strikeout")
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

    strikeout_cell = _cell_key("strikeout")
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
