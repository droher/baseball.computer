"""Cell-grain prep for the run-expectancy count model.

Reads the per-event ``model_input_run_values`` Parquet and aggregates to
run-expectancy cell grain — one row per ``run_expectancy_start_key`` (which
already encodes ``season_league_outs_basestate``) carrying the summed
``runs_to_end_of_inning`` and the event count. The sum of independent
NegativeBinomial counts that share a cell mean is itself NegativeBinomial
(``mu = n*lambda``, ``alpha = n*phi``), so the aggregated cell-sum likelihood
is exact under a per-cell-iid assumption and collapses ~10M events to a few
thousand cells. Each ``(state, season, league)`` cell is a published
run-expectancy coord nested under its 24-state base-out parent; cells below
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
OUTCOME: str = "runs_to_end"

HOLDOUT_FOLD_COUNT: int = 10
HOLDOUT_FOLD_ID: int = 0

MIN_EVENTS_PER_CELL: int = 25

SINGLE_SOURCE_LABEL: str = "__single__"

BASE_STATES: int = 8
START_OUTS: int = 3


class RunExpectancyHeldOutSet(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    sum_runs: IntArray
    cell_event_count: IntArray
    cell_idx: IntArray
    cell_state_idx: IntArray

    @property
    def n_cells(self) -> int:
        return int(self.sum_runs.shape[0])


class RunExpectancyInputs(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    sum_runs: IntArray
    cell_event_count: IntArray
    cell_state_idx: IntArray

    cell_labels: list[str]
    state_by_cell: list[int]
    outs_by_cell: list[int]
    base_state_by_cell: list[int]
    season_by_cell: list[int]
    league_by_cell: list[str]
    state_labels: list[str]

    outcome: str
    coords: dict[str, list[str]]
    held_out: RunExpectancyHeldOutSet

    @property
    def n_events(self) -> int:
        return int(self.cell_event_count.sum())

    @property
    def n_cells(self) -> int:
        return int(self.sum_runs.shape[0])


def _parse_start_key(key: str) -> tuple[int, str, int, int]:
    season_str, league, outs_str, base_str = key.rsplit("_", 3)
    return int(season_str), league, int(outs_str), int(base_str)


def _aggregate_cells(parquet_path: Path) -> pl.DataFrame:
    return (
        pl.scan_parquet(parquet_path)
        .filter(
            pl.col("game_id").is_not_null()
            & pl.col("run_expectancy_start_key").is_not_null()
            & pl.col("runs_to_end_of_inning").is_not_null()
        )
        .group_by(["run_expectancy_start_key", "game_id"])
        .agg(
            pl.col("runs_to_end_of_inning").sum().alias("game_cell_runs"),
            pl.len().alias("game_cell_events"),
        )
        .collect()
    )


def _collapse_to_cells(per_game: pl.DataFrame) -> pl.DataFrame:
    return (
        per_game.group_by("run_expectancy_start_key")
        .agg(
            pl.col("game_cell_runs").sum().alias("sum_runs"),
            pl.col("game_cell_events").sum().alias("n_events"),
        )
        .sort("run_expectancy_start_key")
    )


def _smoke_subsample(
    train_games: pl.DataFrame, smoke_limit: int, seed: int
) -> pl.DataFrame:
    per_game = (
        train_games.group_by("game_id")
        .agg(pl.col("game_cell_events").sum().alias("game_events"))
        .sort("game_id")
    )
    total = int(per_game.get_column("game_events").sum())
    if total <= smoke_limit:
        return train_games
    shuffled = per_game.sample(fraction=1.0, seed=seed, shuffle=True).with_columns(
        pl.col("game_events").cum_sum().alias("cum_events")
    )
    kept_games = shuffled.filter(pl.col("cum_events") <= smoke_limit).get_column(
        "game_id"
    )
    if kept_games.len() == 0:
        kept_games = shuffled.head(1).get_column("game_id")
    out = train_games.filter(pl.col("game_id").is_in(kept_games.implode()))
    _log.info(
        "prepare_run_expectancy_inputs smoke-subsampled to %d games (budget=%d events, seed=%d)",
        kept_games.len(),
        smoke_limit,
        seed,
    )
    return out


def _assert_contiguous(idx: IntArray, labels: list[str], column: str) -> None:
    expected = set(range(len(labels)))
    actual: set[int] = {int(v) for v in np.unique(idx).tolist()}
    if actual != expected:
        raise AssertionError(
            f"{column} indexer contiguity check failed: expected {expected}, got {actual}"
        )


def _encode_codes_with_vocab(values: Sequence[str], labels: Sequence[str]) -> IntArray:
    mapping = {c: i for i, c in enumerate(labels)}
    return np.fromiter(
        (mapping.get(str(v), -1) for v in values),
        dtype=np.int64,
        count=len(values),
    )


def _decompose_cells(
    cell_labels: list[str],
) -> tuple[list[int], list[int], list[int], list[str], list[int], list[str]]:
    outs_by_cell: list[int] = []
    base_by_cell: list[int] = []
    season_by_cell: list[int] = []
    league_by_cell: list[str] = []
    state_by_cell: list[int] = []
    state_label_by_cell: list[str] = []
    for label in cell_labels:
        season, league, outs, base = _parse_start_key(label)
        outs_by_cell.append(outs)
        base_by_cell.append(base)
        season_by_cell.append(season)
        league_by_cell.append(league)
        state_by_cell.append(outs * BASE_STATES + base)
        state_label_by_cell.append(f"{outs}_{base}")
    return (
        state_by_cell,
        outs_by_cell,
        base_by_cell,
        league_by_cell,
        season_by_cell,
        state_label_by_cell,
    )


def prepare_run_expectancy_inputs(
    parquet_path: Path,
    *,
    dimension: str | None = None,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
) -> RunExpectancyInputs:
    """Aggregate per-event rows to run-expectancy cell grain.

    The ``dimension`` kwarg is accepted for interface parity with other prep
    entrypoints and ignored (this dataset has no dimension column). The
    ``MIN_EVENTS_PER_CELL`` floor is applied to the full training corpus
    first; ``smoke_limit`` then subsamples whole games to that event budget,
    so the coord space is derived from the surviving cells (full fits set a
    budget above the corpus and never subsample).
    """
    _ = dimension

    per_game = _aggregate_cells(parquet_path)
    if per_game.height == 0:
        raise ValueError(f"no run-value rows in {parquet_path}")

    holdout_mask = [
        game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
        for g in per_game.get_column("game_id").to_list()
    ]
    per_game = per_game.with_columns(pl.Series("_held_out", holdout_mask))
    held_out_games = per_game.filter(pl.col("_held_out"))
    train_games = per_game.filter(~pl.col("_held_out"))
    _log.info(
        "prepare_run_expectancy_inputs split %d/%d game-cell rows to held-out via fold %d/%d",
        held_out_games.height,
        per_game.height,
        HOLDOUT_FOLD_ID,
        HOLDOUT_FOLD_COUNT,
    )
    if train_games.height == 0:
        raise ValueError("every game landed in the held-out fold; nothing to fit")

    cell_counts = _collapse_to_cells(train_games)
    kept_labels = cell_counts.filter(
        pl.col("n_events") >= MIN_EVENTS_PER_CELL
    ).get_column("run_expectancy_start_key")
    dropped = cell_counts.height - kept_labels.len()
    if dropped:
        _log.info(
            "prepare_run_expectancy_inputs dropped %d cells with <%d training events",
            dropped,
            MIN_EVENTS_PER_CELL,
        )
    train_games = train_games.filter(
        pl.col("run_expectancy_start_key").is_in(kept_labels.implode())
    )
    if train_games.height == 0:
        raise ValueError(
            f"every cell fell below MIN_EVENTS_PER_CELL={MIN_EVENTS_PER_CELL}; "
            "nothing to fit"
        )

    if smoke_limit is not None:
        train_games = _smoke_subsample(train_games, smoke_limit, seed)

    train_cells = _collapse_to_cells(train_games)
    held_cells = _collapse_to_cells(held_out_games)

    cell_labels = train_cells.get_column("run_expectancy_start_key").to_list()
    sum_runs = train_cells.get_column("sum_runs").to_numpy().astype(np.int64)
    cell_event_count = train_cells.get_column("n_events").to_numpy().astype(np.int64)

    (
        state_by_cell,
        outs_by_cell,
        base_state_by_cell,
        league_by_cell,
        season_by_cell,
        state_label_by_cell,
    ) = _decompose_cells(cell_labels)

    state_labels = sorted({lbl for lbl in state_label_by_cell})
    state_label_to_idx = {lbl: i for i, lbl in enumerate(state_labels)}
    cell_state_idx = np.fromiter(
        (state_label_to_idx[lbl] for lbl in state_label_by_cell),
        dtype=np.int64,
        count=len(state_label_by_cell),
    )
    _assert_contiguous(cell_state_idx, state_labels, "cell_state_idx")

    held_out = _build_held_out_set(
        held_cells,
        cell_labels=cell_labels,
        state_label_by_cell=state_label_by_cell,
        state_labels=state_labels,
    )

    coords: dict[str, list[str]] = {
        "source": [SINGLE_SOURCE_LABEL],
        "state": list(state_labels),
        "cell": list(cell_labels),
    }

    _log.info(
        "prepare_run_expectancy_inputs train_cells=%d held_out_cells=%d states=%d events=%d",
        train_cells.height,
        held_out.n_cells,
        len(state_labels),
        int(cell_event_count.sum()),
    )

    return RunExpectancyInputs(
        sum_runs=sum_runs,
        cell_event_count=cell_event_count,
        cell_state_idx=cell_state_idx,
        cell_labels=list(cell_labels),
        state_by_cell=state_by_cell,
        outs_by_cell=outs_by_cell,
        base_state_by_cell=base_state_by_cell,
        season_by_cell=season_by_cell,
        league_by_cell=league_by_cell,
        state_labels=state_labels,
        outcome=OUTCOME,
        coords=coords,
        held_out=held_out,
    )


def _build_held_out_set(
    held_cells: pl.DataFrame,
    *,
    cell_labels: list[str],
    state_label_by_cell: list[str],
    state_labels: list[str],
) -> RunExpectancyHeldOutSet:
    if held_cells.height == 0:
        empty = np.zeros(0, dtype=np.int64)
        return RunExpectancyHeldOutSet(
            sum_runs=empty,
            cell_event_count=empty,
            cell_idx=empty,
            cell_state_idx=empty,
        )

    held_labels = held_cells.get_column("run_expectancy_start_key").to_list()
    cell_idx = _encode_codes_with_vocab(held_labels, cell_labels)
    seen = cell_idx >= 0
    state_label_by_train_cell = dict(zip(cell_labels, state_label_by_cell))
    state_label_to_idx = {lbl: i for i, lbl in enumerate(state_labels)}
    held_state_idx = np.fromiter(
        (
            state_label_to_idx.get(state_label_by_train_cell.get(lbl, ""), -1)
            for lbl in held_labels
        ),
        dtype=np.int64,
        count=len(held_labels),
    )
    sum_runs = held_cells.get_column("sum_runs").to_numpy().astype(np.int64)
    cell_event_count = held_cells.get_column("n_events").to_numpy().astype(np.int64)
    return RunExpectancyHeldOutSet(
        sum_runs=sum_runs[seen],
        cell_event_count=cell_event_count[seen],
        cell_idx=cell_idx[seen],
        cell_state_idx=held_state_idx[seen],
    )
