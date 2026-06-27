"""Cell-grain prep for the Markov base-out transition submodel.

Reads the per-event ``model_input_run_values`` Parquet and aggregates to
transition cell grain — one row per ``(season, league, start_state)`` carrying a
length-25 end-state count vector. The start state is the ``(outs, base)`` suffix
of ``run_expectancy_start_key`` (``outs*8+base``, 24 base-out states). The end
class is the ``(outs, base)`` suffix of ``run_expectancy_end_key``: an
inning-ending transition is encoded as ``outs==3`` (suffix ``3_0``) and maps to a
single inning-end sentinel index, so the end vocabulary is 24 base-out states
plus the sentinel for 25 classes. Cells below ``MIN_EVENTS_PER_CELL`` training
events are dropped as unidentified. A deterministic 10% game holdout
(``game_hash_fold``, fold 0) ships alongside training for OOS scoring, with start
states unseen in training encoded to ``-1``.
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
BoolArray = npt.NDArray[np.bool_]

DEFAULT_SEED: int = 20260513
OUTCOME: str = "end_state"

HOLDOUT_FOLD_COUNT: int = 10
HOLDOUT_FOLD_ID: int = 0

MIN_EVENTS_PER_CELL: int = 25

SINGLE_SOURCE_LABEL: str = "__single__"

BASE_STATES: int = 8
N_OUTS: int = 3
N_START_STATES: int = N_OUTS * BASE_STATES
INNING_END_OUTS: int = 3
INNING_END_INDEX: int = N_START_STATES
N_END_CLASSES: int = N_START_STATES + 1
INNING_END_LABEL: str = "inning_end"


class StateTransitionHeldOutSet(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    counts: IntArray
    cell_idx: IntArray
    cell_start_idx: IntArray

    @property
    def n_cells(self) -> int:
        return int(self.counts.shape[0])


class StateTransitionInputs(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    counts: IntArray
    cell_start_idx: IntArray
    reachable_mask: BoolArray
    ref_class_by_start: IntArray

    cell_labels: list[str]
    season_by_cell: list[int]
    league_by_cell: list[str]
    start_state_by_cell: list[int]

    start_state_labels: list[str]
    end_class_labels: list[str]

    outcome: str
    coords: dict[str, list[str]]
    held_out: StateTransitionHeldOutSet

    @property
    def n_cells(self) -> int:
        return int(self.counts.shape[0])

    @property
    def n_events(self) -> int:
        return int(self.counts.sum())

    @property
    def n_start_states(self) -> int:
        return len(self.start_state_labels)

    @property
    def n_end_classes(self) -> int:
        return int(self.counts.shape[1])


def _start_state_labels() -> list[str]:
    return [f"{outs}_{base}" for outs in range(N_OUTS) for base in range(BASE_STATES)]


def _end_class_labels() -> list[str]:
    return [*_start_state_labels(), INNING_END_LABEL]


def _reachable_mask() -> BoolArray:
    mask = np.zeros((N_START_STATES, N_END_CLASSES), dtype=bool)
    for start in range(N_START_STATES):
        start_outs = start // BASE_STATES
        for end in range(N_END_CLASSES):
            end_outs = INNING_END_OUTS if end == INNING_END_INDEX else end // BASE_STATES
            if end_outs >= start_outs:
                mask[start, end] = True
    return mask


def _reference_by_start(
    counts: IntArray, cell_start_idx: IntArray, reachable: BoolArray
) -> IntArray:
    per_start = np.zeros((N_START_STATES, N_END_CLASSES), dtype=np.int64)
    np.add.at(per_start, cell_start_idx, counts)
    masked = np.where(reachable, per_start, -1)
    return masked.argmax(axis=1).astype(np.int64)


def _parse_suffix(key: str) -> tuple[int, int]:
    outs_str, base_str = key.rsplit("_", 2)[-2:]
    return int(outs_str), int(base_str)


def _end_class_index(key: str) -> int:
    outs, base = _parse_suffix(key)
    if outs >= INNING_END_OUTS:
        return INNING_END_INDEX
    return outs * BASE_STATES + base


def _aggregate_cells(parquet_path: Path) -> pl.DataFrame:
    return (
        pl.scan_parquet(parquet_path)
        .filter(
            pl.col("game_id").is_not_null()
            & pl.col("run_expectancy_start_key").is_not_null()
            & pl.col("run_expectancy_end_key").is_not_null()
        )
        .with_columns(
            (
                pl.col("season").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("league").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("run_expectancy_start_key").str.split("_").list.tail(2).list.join("_")
            ).alias("cell")
        )
        .group_by(
            ["cell", "game_id", "run_expectancy_start_key", "run_expectancy_end_key"]
        )
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
        "prepare_state_transition_inputs smoke-subsampled to %d games (budget=%d events, seed=%d)",
        kept.len(),
        smoke_limit,
        seed,
    )
    return out


def _count_matrix(per_cell: pl.DataFrame, cell_labels: Sequence[str]) -> IntArray:
    cell_to_row = {c: i for i, c in enumerate(cell_labels)}
    matrix = np.zeros((len(cell_labels), N_END_CLASSES), dtype=np.int64)
    for cell, end_key, n in per_cell.select(
        ["cell", "run_expectancy_end_key", "n"]
    ).iter_rows():
        row = cell_to_row.get(str(cell))
        if row is None:
            continue
        matrix[row, _end_class_index(str(end_key))] += int(n)
    return matrix


def prepare_state_transition_inputs(
    parquet_path: Path,
    *,
    dimension: str | None = None,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
) -> StateTransitionInputs:
    """Aggregate per-event rows to transition cell grain.

    The ``dimension`` kwarg is accepted for interface parity with other prep
    entrypoints and ignored (this dataset has no dimension column). The
    ``MIN_EVENTS_PER_CELL`` floor is applied to the full training corpus first;
    ``smoke_limit`` then subsamples whole games to that event budget, so the
    coord space is derived from the surviving cells.
    """
    _ = dimension

    per_game = _aggregate_cells(parquet_path)
    if per_game.height == 0:
        raise ValueError(f"no transition rows in {parquet_path}")

    holdout_mask = [
        game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
        for g in per_game.get_column("game_id").to_list()
    ]
    per_game = per_game.with_columns(pl.Series("_held_out", holdout_mask))
    held_games = per_game.filter(pl.col("_held_out"))
    train_games = per_game.filter(~pl.col("_held_out"))
    _log.info(
        "prepare_state_transition_inputs split %d/%d game-cell-transition rows to held-out via fold %d/%d",
        held_games.height,
        per_game.height,
        HOLDOUT_FOLD_ID,
        HOLDOUT_FOLD_COUNT,
    )
    if train_games.height == 0:
        raise ValueError("every game landed in the held-out fold; nothing to fit")

    cell_totals = train_games.group_by("cell").agg(pl.col("n").sum().alias("total"))
    kept_cells = cell_totals.filter(
        pl.col("total") >= MIN_EVENTS_PER_CELL
    ).get_column("cell")
    dropped = cell_totals.height - kept_cells.len()
    if dropped:
        _log.info(
            "prepare_state_transition_inputs dropped %d cells with <%d training events",
            dropped,
            MIN_EVENTS_PER_CELL,
        )
    train_games = train_games.filter(pl.col("cell").is_in(kept_cells.implode()))
    if train_games.height == 0:
        raise ValueError(
            f"every cell fell below MIN_EVENTS_PER_CELL={MIN_EVENTS_PER_CELL}; "
            "nothing to fit"
        )

    if smoke_limit is not None:
        train_games = _smoke_subsample(train_games, smoke_limit, seed)

    train_cells = (
        train_games.group_by(
            ["cell", "run_expectancy_start_key", "run_expectancy_end_key"]
        )
        .agg(pl.col("n").sum().alias("n"))
        .sort("cell")
    )
    cell_labels = sorted(train_cells.get_column("cell").unique().to_list())
    counts = _count_matrix(train_cells, cell_labels)

    season_by_cell = [int(str(c).split("|")[0]) for c in cell_labels]
    league_by_cell = [str(c).split("|")[1] for c in cell_labels]
    start_suffix_by_cell = [str(c).split("|")[2] for c in cell_labels]

    start_state_labels = _start_state_labels()
    start_to_idx = {lbl: i for i, lbl in enumerate(start_state_labels)}
    cell_start_idx = np.fromiter(
        (start_to_idx[s] for s in start_suffix_by_cell),
        dtype=np.int64,
        count=len(start_suffix_by_cell),
    )
    start_state_by_cell = [int(v) for v in cell_start_idx.tolist()]
    _assert_contiguous_subset(cell_start_idx, start_state_labels, "cell_start_idx")

    end_class_labels = _end_class_labels()

    reachable_mask = _reachable_mask()
    ref_class_by_start = _reference_by_start(counts, cell_start_idx, reachable_mask)

    held_out = _build_held_out_set(
        held_games,
        cell_labels=cell_labels,
        start_suffix_by_cell=start_suffix_by_cell,
        start_to_idx=start_to_idx,
    )

    coords: dict[str, list[str]] = {
        "source": [SINGLE_SOURCE_LABEL],
        "start_state": list(start_state_labels),
        "end_class": list(end_class_labels),
        "end_class_nonref": list(end_class_labels[1:]),
        "cell": list(cell_labels),
    }

    _log.info(
        "prepare_state_transition_inputs train_cells=%d held_out_cells=%d "
        "start_states=%d end_classes=%d events=%d",
        len(cell_labels),
        held_out.n_cells,
        len(start_state_labels),
        N_END_CLASSES,
        int(counts.sum()),
    )

    return StateTransitionInputs(
        counts=counts,
        cell_start_idx=cell_start_idx,
        reachable_mask=reachable_mask,
        ref_class_by_start=ref_class_by_start,
        cell_labels=list(cell_labels),
        season_by_cell=season_by_cell,
        league_by_cell=league_by_cell,
        start_state_by_cell=start_state_by_cell,
        start_state_labels=start_state_labels,
        end_class_labels=end_class_labels,
        outcome=OUTCOME,
        coords=coords,
        held_out=held_out,
    )


def _assert_contiguous_subset(
    idx: IntArray, labels: list[str], column: str
) -> None:
    actual: set[int] = {int(v) for v in np.unique(idx).tolist()}
    if not actual <= set(range(len(labels))):
        raise AssertionError(
            f"{column} indexer out of range for {len(labels)} labels: got {actual}"
        )


def _build_held_out_set(
    held_games: pl.DataFrame,
    *,
    cell_labels: list[str],
    start_suffix_by_cell: list[str],
    start_to_idx: dict[str, int],
) -> StateTransitionHeldOutSet:
    empty = np.zeros(0, dtype=np.int64)
    if held_games.height == 0:
        return StateTransitionHeldOutSet(
            counts=np.zeros((0, N_END_CLASSES), dtype=np.int64),
            cell_idx=empty,
            cell_start_idx=empty,
        )
    held_cells = (
        held_games.filter(pl.col("cell").is_in(pl.Series(cell_labels).implode()))
        .group_by(["cell", "run_expectancy_start_key", "run_expectancy_end_key"])
        .agg(pl.col("n").sum().alias("n"))
        .sort("cell")
    )
    if held_cells.height == 0:
        return StateTransitionHeldOutSet(
            counts=np.zeros((0, N_END_CLASSES), dtype=np.int64),
            cell_idx=empty,
            cell_start_idx=empty,
        )
    held_labels = sorted(held_cells.get_column("cell").unique().to_list())
    counts = _count_matrix(held_cells, held_labels)
    cell_to_train = {c: i for i, c in enumerate(cell_labels)}
    start_by_train = dict(zip(cell_labels, start_suffix_by_cell))
    cell_idx = np.fromiter(
        (cell_to_train[str(c)] for c in held_labels),
        dtype=np.int64,
        count=len(held_labels),
    )
    cell_start_idx = np.fromiter(
        (start_to_idx[start_by_train[str(c)]] for c in held_labels),
        dtype=np.int64,
        count=len(held_labels),
    )
    return StateTransitionHeldOutSet(
        counts=counts,
        cell_idx=cell_idx,
        cell_start_idx=cell_start_idx,
    )
