"""Cell-grain prep for the pitch-summary count-distribution model.

Reads the per-event ``model_input_pitch_summary`` Parquet, restricts to the
``has_count`` slice (the era/sources where the plate appearance's final
ball-strike count is recorded), and aggregates to ``(result_family, season,
league)`` cells. Each cell carries a length-12 count vector over the final-count
classes (balls 0-3 x strikes 0-2, ``class = balls*3 + strikes``). Cells below
``MIN_EVENTS_PER_CELL`` training events are dropped as unidentified. A
deterministic 10% game holdout (``game_hash_fold``, fold 0) ships alongside
training for OOS scoring, with cells unseen in training encoded to ``-1``.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import ClassVar

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict

from python_models.statistical.splits import game_hash_fold

_log = logging.getLogger(__name__)

IntArray = npt.NDArray[np.int64]

DEFAULT_SEED: int = 20260513
OUTCOME: str = "final_count"

HOLDOUT_FOLD_COUNT: int = 10
HOLDOUT_FOLD_ID: int = 0

MIN_EVENTS_PER_CELL: int = 25

MAX_BALLS: int = 3
MAX_STRIKES: int = 2
N_CLASSES: int = (MAX_BALLS + 1) * (MAX_STRIKES + 1)

UNKNOWN_RESULT: str = "__unknown__"
SINGLE_SOURCE_LABEL: str = "__single__"


class PitchSummaryHeldOutSet(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    counts: IntArray
    cell_idx: IntArray
    cell_result_idx: IntArray

    @property
    def n_cells(self) -> int:
        return int(self.counts.shape[0])


class PitchSummaryInputs(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    counts: IntArray
    cell_result_idx: IntArray

    cell_labels: list[str]
    result_by_cell: list[str]
    season_by_cell: list[int]
    league_by_cell: list[str]
    class_labels: list[str]
    balls_by_class: list[int]
    strikes_by_class: list[int]
    result_family_labels: list[str]

    outcome: str
    coords: dict[str, list[str]]
    held_out: PitchSummaryHeldOutSet

    @property
    def n_cells(self) -> int:
        return int(self.counts.shape[0])

    @property
    def n_events(self) -> int:
        return int(self.counts.sum())

    @property
    def n_classes(self) -> int:
        return int(self.counts.shape[1])


def _class_labels() -> tuple[list[str], list[int], list[int]]:
    labels: list[str] = []
    balls: list[int] = []
    strikes: list[int] = []
    for b in range(MAX_BALLS + 1):
        for s in range(MAX_STRIKES + 1):
            labels.append(f"b{b}_s{s}")
            balls.append(b)
            strikes.append(s)
    return labels, balls, strikes


def _aggregate_cells(parquet_path: Path) -> pl.DataFrame:
    balls = pl.col("count_balls_raw").cast(pl.Int64, strict=False)
    strikes = pl.col("count_strikes_raw").cast(pl.Int64, strict=False)
    return (
        pl.scan_parquet(parquet_path)
        .filter(
            pl.col("has_count")
            & pl.col("game_id").is_not_null()
            & balls.is_between(0, MAX_BALLS)
            & strikes.is_between(0, MAX_STRIKES)
        )
        .with_columns(
            (balls * (MAX_STRIKES + 1) + strikes).alias("final_class"),
            pl.col("result_family").fill_null(UNKNOWN_RESULT).alias("result_family"),
            (
                pl.col("season").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("league").cast(pl.Utf8)
            ).alias("season_league"),
        )
        .with_columns(
            (pl.col("result_family") + pl.lit("|") + pl.col("season_league")).alias(
                "cell"
            )
        )
        .group_by(["cell", "game_id", "final_class"])
        .agg(pl.len().alias("n"))
        .collect()
    )


def _smoke_subsample(
    per_game: pl.DataFrame, smoke_limit: int, seed: int
) -> pl.DataFrame:
    games = (
        per_game.group_by("game_id")
        .agg(pl.col("n").sum().alias("game_events"))
        .sort("game_id")
    )
    if int(games.get_column("game_events").sum()) <= smoke_limit:
        return per_game
    shuffled = games.sample(fraction=1.0, seed=seed, shuffle=True).with_columns(
        pl.col("game_events").cum_sum().alias("cum_events")
    )
    kept = shuffled.filter(pl.col("cum_events") <= smoke_limit).get_column("game_id")
    if kept.len() == 0:
        kept = shuffled.head(1).get_column("game_id")
    out = per_game.filter(pl.col("game_id").is_in(kept.implode()))
    _log.info(
        "prepare_pitch_summary_inputs smoke-subsampled to %d games (budget=%d events, seed=%d)",
        kept.len(),
        smoke_limit,
        seed,
    )
    return out


def _count_matrix(per_game: pl.DataFrame, cell_labels: Sequence[str]) -> IntArray:
    cell_to_row = {c: i for i, c in enumerate(cell_labels)}
    matrix = np.zeros((len(cell_labels), N_CLASSES), dtype=np.int64)
    for cell, final_class, n in per_game.select(
        ["cell", "final_class", "n_cell"]
    ).iter_rows():
        row = cell_to_row.get(str(cell))
        if row is None:
            continue
        matrix[row, int(final_class)] = int(n)
    return matrix


def prepare_pitch_summary_inputs(
    parquet_path: Path,
    *,
    dimension: str | None = None,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
) -> PitchSummaryInputs:
    """Aggregate the ``has_count`` slice to final-count cell-grain counts.

    The ``dimension`` kwarg is accepted for interface parity with other prep
    entrypoints and ignored (this dataset has no dimension column). The
    ``MIN_EVENTS_PER_CELL`` floor is applied to the full training corpus first;
    ``smoke_limit`` then subsamples whole games to that event budget (full fits
    set a budget above the corpus and never subsample).
    """
    _ = dimension

    per_game = _aggregate_cells(parquet_path)
    if per_game.height == 0:
        raise ValueError(
            f"no has_count rows with a parseable final count in {parquet_path}"
        )

    holdout_mask = [
        game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
        for g in per_game.get_column("game_id").to_list()
    ]
    per_game = per_game.with_columns(pl.Series("_held_out", holdout_mask))
    held_games = per_game.filter(pl.col("_held_out"))
    train_games = per_game.filter(~pl.col("_held_out"))
    _log.info(
        "prepare_pitch_summary_inputs split %d/%d game-cell-class rows to held-out via fold %d/%d",
        held_games.height,
        per_game.height,
        HOLDOUT_FOLD_ID,
        HOLDOUT_FOLD_COUNT,
    )
    if train_games.height == 0:
        raise ValueError("every game landed in the held-out fold; nothing to fit")

    cell_totals = train_games.group_by("cell").agg(pl.col("n").sum().alias("total"))
    kept_cells = cell_totals.filter(pl.col("total") >= MIN_EVENTS_PER_CELL).get_column(
        "cell"
    )
    dropped = cell_totals.height - kept_cells.len()
    if dropped:
        _log.info(
            "prepare_pitch_summary_inputs dropped %d cells with <%d training events",
            dropped,
            MIN_EVENTS_PER_CELL,
        )
    train_games = train_games.filter(pl.col("cell").is_in(kept_cells.implode()))
    if train_games.height == 0:
        raise ValueError(
            f"every cell fell below MIN_EVENTS_PER_CELL={MIN_EVENTS_PER_CELL}; nothing to fit"
        )

    if smoke_limit is not None:
        train_games = _smoke_subsample(train_games, smoke_limit, seed)

    train_cells = (
        train_games.group_by(["cell", "final_class"])
        .agg(pl.col("n").sum().alias("n_cell"))
        .sort("cell")
    )
    cell_labels = sorted(train_cells.get_column("cell").unique().to_list())
    counts = _count_matrix(train_cells, cell_labels)

    result_by_cell = [str(c).split("|", 1)[0] for c in cell_labels]
    season_by_cell = [int(str(c).split("|")[1]) for c in cell_labels]
    league_by_cell = [str(c).split("|")[2] for c in cell_labels]

    result_family_labels = sorted(set(result_by_cell))
    result_to_idx = {r: i for i, r in enumerate(result_family_labels)}
    cell_result_idx = np.fromiter(
        (result_to_idx[r] for r in result_by_cell),
        dtype=np.int64,
        count=len(result_by_cell),
    )
    _assert_contiguous(cell_result_idx, result_family_labels, "cell_result_idx")

    class_labels, balls_by_class, strikes_by_class = _class_labels()

    held_out = _build_held_out_set(
        held_games,
        cell_labels=cell_labels,
        result_by_cell=result_by_cell,
        result_family_labels=result_family_labels,
    )

    coords: dict[str, list[str]] = {
        "source": [SINGLE_SOURCE_LABEL],
        "class": list(class_labels),
        "class_nonref": list(class_labels[1:]),
        "result_family": list(result_family_labels),
        "cell": list(cell_labels),
    }

    _log.info(
        "prepare_pitch_summary_inputs train_cells=%d held_out_cells=%d results=%d events=%d",
        len(cell_labels),
        held_out.n_cells,
        len(result_family_labels),
        int(counts.sum()),
    )

    return PitchSummaryInputs(
        counts=counts,
        cell_result_idx=cell_result_idx,
        cell_labels=list(cell_labels),
        result_by_cell=result_by_cell,
        season_by_cell=season_by_cell,
        league_by_cell=league_by_cell,
        class_labels=class_labels,
        balls_by_class=balls_by_class,
        strikes_by_class=strikes_by_class,
        result_family_labels=result_family_labels,
        outcome=OUTCOME,
        coords=coords,
        held_out=held_out,
    )


def _assert_contiguous(idx: IntArray, labels: list[str], column: str) -> None:
    expected = set(range(len(labels)))
    actual: set[int] = {int(v) for v in np.unique(idx).tolist()}
    if actual != expected:
        raise AssertionError(
            f"{column} indexer contiguity check failed: expected {expected}, got {actual}"
        )


def _build_held_out_set(
    held_games: pl.DataFrame,
    *,
    cell_labels: list[str],
    result_by_cell: list[str],
    result_family_labels: list[str],
) -> PitchSummaryHeldOutSet:
    empty = np.zeros(0, dtype=np.int64)
    if held_games.height == 0:
        return PitchSummaryHeldOutSet(
            counts=np.zeros((0, N_CLASSES), dtype=np.int64),
            cell_idx=empty,
            cell_result_idx=empty,
        )
    held_cells = (
        held_games.filter(pl.col("cell").is_in(pl.Series(cell_labels).implode()))
        .group_by(["cell", "final_class"])
        .agg(pl.col("n").sum().alias("n_cell"))
        .sort("cell")
    )
    if held_cells.height == 0:
        return PitchSummaryHeldOutSet(
            counts=np.zeros((0, N_CLASSES), dtype=np.int64),
            cell_idx=empty,
            cell_result_idx=empty,
        )
    held_labels = sorted(held_cells.get_column("cell").unique().to_list())
    counts = _count_matrix(held_cells, held_labels)
    cell_to_train = {c: i for i, c in enumerate(cell_labels)}
    result_by_train = dict(zip(cell_labels, result_by_cell))
    result_to_idx = {r: i for i, r in enumerate(result_family_labels)}
    cell_idx = np.fromiter(
        (cell_to_train[str(c)] for c in held_labels),
        dtype=np.int64,
        count=len(held_labels),
    )
    cell_result_idx = np.fromiter(
        (result_to_idx[result_by_train[str(c)]] for c in held_labels),
        dtype=np.int64,
        count=len(held_labels),
    )
    return PitchSummaryHeldOutSet(
        counts=counts,
        cell_idx=cell_idx,
        cell_result_idx=cell_result_idx,
    )
