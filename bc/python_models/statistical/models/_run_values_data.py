"""Cell-grain prep for the run-expectancy count model.

Reads the per-event ``model_input_run_values`` Parquet, restricts it to the
same population the deterministic ``run_expectancy_matrix`` uses (regular
season, innings before the ninth, untruncated exposure, and rows that are
real events rather than no-op substitutions), and aggregates to
run-expectancy cell grain — one row per ``season|league|outs_base`` cell
carrying the summed ``runs_to_end_of_inning`` and the event count. ``season``
and ``league`` come from the dataset columns, not from
``run_expectancy_start_key`` (whose ``season_group`` / ``league_group``
buckets collapse pre-1914 seasons and non-AL/NL/FL leagues), so the cell
matches the state-transition prep exactly. The sum of independent
NegativeBinomial counts that share a cell mean is itself NegativeBinomial
(``mu = n*lambda``, ``alpha = n*phi``), so the aggregated cell-sum likelihood
is exact under a per-cell-iid assumption and collapses ~14M events to a few
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
FloatArray = npt.NDArray[np.float64]

DEFAULT_SEED: int = 20260513
OUTCOME: str = "runs_to_end"

ERA_REGIME_LABELS: tuple[str, ...] = (
    "pre_DH",
    "DH_AL_only",
    "full_DH",
    "ghost_runner",
    "extra_inning_ghost_plus_expanded_DH",
)

REGULAR_SEASON_GAME_TYPE: str = "RegularSeason"
FIRST_EXCLUDED_INNING: int = 9
INCLUDED_DENOMINATOR_POLICY: str = "include"

POPULATION_FILTER_COLUMNS: tuple[str, ...] = (
    "game_type",
    "inning_start",
    "denominator_policy",
    "result_family",
    "run_expectancy_start_key",
    "run_expectancy_end_key",
    "runs_on_play",
)

CELL_SEPARATOR: str = "|"


def _era_regime_row(season: int, league: str) -> list[float]:
    """Multi-hot regime indicators for one (season, league) cell.

    Regimes overlap; the vector is a set of indicators, not a categorical.
    At the (season, league) cell grain the model cannot resolve extra-innings
    or game-type, so ghost-runner regimes are encoded as their season proxy
    and collinear columns are pruned downstream by ``_build_era_regime_design``.
    """
    return [
        1.0 if season < 1973 else 0.0,
        1.0 if 1973 <= season <= 2021 and league == "AL" else 0.0,
        1.0 if season >= 2022 else 0.0,
        1.0 if season >= 2020 else 0.0,
        1.0 if season >= 2022 else 0.0,
    ]


def _build_era_regime_design(
    season_by_cell: list[int], league_by_cell: list[str]
) -> tuple[FloatArray, list[str]]:
    """Per-cell regime design, pruned to full rank.

    Drops all-zero columns (a regime absent from the corpus) and exact
    duplicates (regimes that collapse to the same indicator at this grain,
    e.g. ``full_DH`` and ``extra_inning_ghost_plus_expanded_DH`` both reduce
    to ``season >= 2022``). The first occurrence is kept.
    """
    full = np.array(
        [_era_regime_row(s, lg) for s, lg in zip(season_by_cell, league_by_cell)],
        dtype=np.float64,
    )
    if full.shape[0] == 0:
        return full.reshape(0, 0), []
    kept_idx: list[int] = []
    kept_labels: list[str] = []
    for j, label in enumerate(ERA_REGIME_LABELS):
        col = full[:, j]
        if not col.any():
            _log.info("era_regime dropped all-zero column %s", label)
            continue
        if any(np.array_equal(col, full[:, k]) for k in kept_idx):
            _log.info("era_regime dropped duplicate column %s", label)
            continue
        kept_idx.append(j)
        kept_labels.append(label)
    return full[:, kept_idx], kept_labels


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
    cell_era_design: FloatArray

    cell_labels: list[str]
    era_labels: list[str]
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


def state_suffix_expr(key_column: str) -> pl.Expr:
    """``outs_base`` suffix of a run-expectancy key column."""
    return pl.col(key_column).str.split("_").list.tail(2).list.join("_")


def cell_label_expr() -> pl.Expr:
    """``season|league|outs_base`` cell label shared by the Model G preps."""
    return pl.concat_str(
        [
            pl.col("season").cast(pl.Int64).cast(pl.Utf8),
            pl.col("league").cast(pl.Utf8),
            state_suffix_expr("run_expectancy_start_key"),
        ],
        separator=CELL_SEPARATOR,
    )


def parse_cell_label(label: str) -> tuple[int, str, int, int]:
    """Split ``season|league|outs_base`` into ``(season, league, outs, base)``."""
    season_str, league, suffix = label.split(CELL_SEPARATOR)
    outs_str, base_str = suffix.split("_")
    return int(season_str), league, int(outs_str), int(base_str)


def _population_clauses() -> list[tuple[str, pl.Expr]]:
    state_changed = (
        pl.col("run_expectancy_start_key") != pl.col("run_expectancy_end_key")
    ).fill_null(False)
    scored = (pl.col("runs_on_play") > 0).fill_null(False)
    return [
        (
            f"game_type = {REGULAR_SEASON_GAME_TYPE}",
            pl.col("game_type").cast(pl.Utf8) == REGULAR_SEASON_GAME_TYPE,
        ),
        (
            f"inning_start < {FIRST_EXCLUDED_INNING}",
            pl.col("inning_start") < FIRST_EXCLUDED_INNING,
        ),
        (
            f"denominator_policy = {INCLUDED_DENOMINATOR_POLICY}",
            pl.col("denominator_policy") == INCLUDED_DENOMINATOR_POLICY,
        ),
        (
            "real event (plate appearance, state change, or run scored)",
            pl.col("result_family").is_not_null() | state_changed | scored,
        ),
    ]


def filter_event_population(frame: pl.LazyFrame, *, label: str) -> pl.LazyFrame:
    """Restrict a run-values frame to the population the Model G preps fit on.

    Four clauses in order: ``game_type = 'RegularSeason'``; ``inning_start``
    before the ninth (walk-off censoring); ``denominator_policy = 'include'``,
    the dataset's game-level exposure gate (``exposure_status`` is ``complete``
    or ``walk_off`` for the game); and a real-event clause that drops rows
    with no plate-appearance result, no base-out state change, and no run
    scored, such as substitutions, while keeping stolen bases, wild pitches,
    and pickoffs.

    This is not the population of the deterministic ``run_expectancy_matrix``
    model. That SQL shares the first two clauses but gates exposure per frame
    (``QUALIFY NOT BOOL_OR(truncated_frame_flag)`` over the rest of the
    inning) rather than per game, and carries no real-event clause. The two
    populations therefore differ on partially truncated games and on non-event
    rows. Raises when the frame lacks any column the filter needs, so a stale
    dataset can never skip the filter silently. Logs the row count before and
    after every clause.
    """
    names = frame.collect_schema().names()
    missing = [c for c in POPULATION_FILTER_COLUMNS if c not in names]
    if missing:
        raise ValueError(
            f"{label}: dataset lacks population-filter columns {missing}; "
            f"expected all of {list(POPULATION_FILTER_COLUMNS)}"
        )
    remaining = frame
    n_before = int(remaining.select(pl.len()).collect().item())
    for clause, expr in _population_clauses():
        remaining = remaining.filter(expr)
        n_after = int(remaining.select(pl.len()).collect().item())
        _log.info(
            "%s population filter [%s]: %d -> %d rows (dropped %d)",
            label,
            clause,
            n_before,
            n_after,
            n_before - n_after,
        )
        n_before = n_after
    return remaining


def _aggregate_cells(parquet_path: Path) -> pl.DataFrame:
    frame = filter_event_population(
        pl.scan_parquet(parquet_path), label="prepare_run_expectancy_inputs"
    )
    return (
        frame.filter(
            pl.col("game_id").is_not_null()
            & pl.col("season").is_not_null()
            & pl.col("league").is_not_null()
            & pl.col("run_expectancy_start_key").is_not_null()
            & pl.col("runs_to_end_of_inning").is_not_null()
        )
        .with_columns(cell_label_expr().alias("cell"))
        .group_by(["cell", "game_id"])
        .agg(
            pl.col("runs_to_end_of_inning").sum().alias("game_cell_runs"),
            pl.len().alias("game_cell_events"),
        )
        .collect()
    )


def _collapse_to_cells(per_game: pl.DataFrame) -> pl.DataFrame:
    return (
        per_game.group_by("cell")
        .agg(
            pl.col("game_cell_runs").sum().alias("sum_runs"),
            pl.col("game_cell_events").sum().alias("n_events"),
        )
        .sort("cell")
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
        season, league, outs, base = parse_cell_label(label)
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
    ).get_column("cell")
    dropped = cell_counts.height - kept_labels.len()
    if dropped:
        _log.info(
            "prepare_run_expectancy_inputs dropped %d cells with <%d training events",
            dropped,
            MIN_EVENTS_PER_CELL,
        )
    train_games = train_games.filter(pl.col("cell").is_in(kept_labels.implode()))
    if train_games.height == 0:
        raise ValueError(
            f"every cell fell below MIN_EVENTS_PER_CELL={MIN_EVENTS_PER_CELL}; "
            "nothing to fit"
        )

    if smoke_limit is not None:
        train_games = _smoke_subsample(train_games, smoke_limit, seed)

    train_cells = _collapse_to_cells(train_games)
    held_cells = _collapse_to_cells(held_out_games)

    cell_labels = train_cells.get_column("cell").to_list()
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

    cell_era_design, era_labels = _build_era_regime_design(
        season_by_cell, league_by_cell
    )

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
        "era_regime": list(era_labels),
    }

    _log.info(
        "prepare_run_expectancy_inputs train_cells=%d held_out_cells=%d states=%d "
        "events=%d era_regimes=%d",
        train_cells.height,
        held_out.n_cells,
        len(state_labels),
        int(cell_event_count.sum()),
        len(era_labels),
    )

    return RunExpectancyInputs(
        sum_runs=sum_runs,
        cell_event_count=cell_event_count,
        cell_state_idx=cell_state_idx,
        cell_era_design=cell_era_design,
        cell_labels=list(cell_labels),
        era_labels=list(era_labels),
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

    held_labels = held_cells.get_column("cell").to_list()
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
