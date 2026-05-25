"""Event-grain prep for the ball-handler imputation model (Model D).

Reads the frozen ``model_input_observation_batted_ball`` Parquet at grain
``(event_key, dimension)``, filters to the handler-observed rows
(``dimension='ball_handler_position', observed_status='observed',
training_weight > 0``), and shapes a single-arm K=9 categorical softmax
over the fielder position that handled the ball. Truth is the directly
recorded handler in ``raw_value`` (1..9), one class per event — there is
no aggregate-box arm and no synthetic masking.

The output ``BallHandlerInputs`` reuses the ``FixedEffectDesign`` /
``HeldOutSet`` / ``ProductionScoringFrame`` types from ``_credit_data`` so
the ``training`` reconstruction and held-out evaluation helpers accept it
unchanged. A deterministic per-game held-out split
(``HOLDOUT_FOLD_COUNT=10`` via ``game_hash_fold``, fold 0) is removed
before training; the held-out events ship alongside training inputs for
OOS scoring. The production scoring slice is the handler-unobserved rows
(``observed_status != 'observed'``).
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

from python_models.statistical.models._credit_data import (
    FixedEffectDesign,
    HeldOutSet,
    ProductionScoringFrame,
)
from python_models.statistical.splits import game_hash_fold

_log = logging.getLogger(__name__)

IntArray = npt.NDArray[np.int64]

DEFAULT_SEED: int = 20260513
MIN_EVENTS_PER_SEASON: int = 50
N_POSITIONS: int = 9
POSITION_LABELS: tuple[str, ...] = tuple(str(p) for p in range(1, N_POSITIONS + 1))

UNKNOWN_LEVEL: str = "__unknown__"
CREDIT_TYPE: str = "ball_handler"

HOLDOUT_FOLD_COUNT: int = 10
HOLDOUT_FOLD_ID: int = 0

OBSERVED_STATUS: str = "observed"

FIXED_EFFECT_COLUMNS: tuple[str, ...] = (
    "result_family",
    "base_state_start",
    "outs_start",
    "alignment_regime",
)

PRODUCTION_FE_COVERAGE_FLOOR: float = 0.01


class BallHandlerInputs(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    event_keys: IntArray
    credit_type: str
    n_positions: int

    counts: IntArray

    season_league_idx: IntArray
    scorer_idx: IntArray

    fixed_effects: dict[str, FixedEffectDesign]

    coords: dict[str, list[str]]

    held_out: HeldOutSet

    @property
    def n_events(self) -> int:
        return int(self.counts.shape[0])


def _assert_contiguous(idx: IntArray, labels: list[str], column: str) -> None:
    expected = set(range(len(labels)))
    actual: set[int] = {int(v) for v in np.unique(idx).tolist()}
    if actual != expected:
        raise AssertionError(
            f"{column} indexer contiguity check failed: expected {expected}, got {actual}"
        )


def _category_index(
    df: pl.DataFrame, column: str, *, fill_value: str = UNKNOWN_LEVEL
) -> tuple[IntArray, list[str]]:
    series = df.get_column(column)
    if series.dtype == pl.Boolean:
        series = series.cast(pl.Utf8)
    series = series.fill_null(fill_value).cast(pl.Utf8)
    raw_values: list[object] = list(series.unique().to_list())
    labels: list[str] = sorted(str(v) for v in raw_values)
    mapping: dict[str, int] = {c: i for i, c in enumerate(labels)}
    codes = (
        series.replace_strict(mapping, return_dtype=pl.Int64)
        .to_numpy()
        .astype(np.int64)
    )
    _assert_contiguous(codes, labels, column)
    return codes, labels


def _build_fixed_effect_design(df: pl.DataFrame, column: str) -> FixedEffectDesign:
    codes, labels = _category_index(df, column)
    if not labels:
        raise ValueError(f"fixed-effect column {column!r} has zero levels")
    return FixedEffectDesign(levels=tuple(labels), codes=codes)


def _encode_codes_with_vocab(
    per_event: pl.DataFrame, column: str, labels: Sequence[str]
) -> IntArray:
    """Map a per-event column to int codes against a fixed ``labels`` vocab.

    Booleans cast to utf8, NULLs fill to ``UNKNOWN_LEVEL``; unseen values
    (and NULLs) fall back to ``UNKNOWN_LEVEL``'s index, or ``-1`` when
    ``UNKNOWN_LEVEL`` is not in the vocab.
    """
    series = per_event.get_column(column)
    if series.dtype == pl.Boolean:
        series = series.cast(pl.Utf8)
    series = series.fill_null(UNKNOWN_LEVEL).cast(pl.Utf8)
    mapping = {c: i for i, c in enumerate(labels)}
    fallback = mapping.get(UNKNOWN_LEVEL, -1)
    return np.fromiter(
        (mapping.get(str(v), fallback) for v in series.to_list()),
        dtype=np.int64,
        count=series.len(),
    )


def _with_handler_and_season_league(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns(
        pl.col("raw_value").cast(pl.Int64, strict=False).alias("_handler"),
        (
            pl.col("season").cast(pl.Utf8)
            + pl.lit("|")
            + pl.col("league").cast(pl.Utf8).fill_null(UNKNOWN_LEVEL)
        ).alias("season_league"),
    )


def _one_hot_counts(handler_1based: IntArray) -> IntArray:
    n = int(handler_1based.shape[0])
    counts = np.zeros((n, N_POSITIONS), dtype=np.int64)
    counts[np.arange(n), handler_1based - 1] = 1
    sums = counts.sum(axis=1)
    if not (sums == 1).all():
        bad = int((sums != 1).sum())
        raise AssertionError(f"_one_hot_counts: {bad} rows do not sum to 1")
    return counts


def _assert_fixed_effects_cover_production_slice(
    parquet_path: Path,
    *,
    dimension: str,
    floor: float = PRODUCTION_FE_COVERAGE_FLOOR,
) -> None:
    """Each FE must be populated on more than ``floor`` of the production slice.

    The production target is the handler-unobserved slice
    (``dimension=<dim> AND observed_status != 'observed'``) — the events
    Model D actually scores. A fixed effect that is mostly NULL there
    cannot generalize to the inference target.
    """
    production = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col("dimension") == dimension)
            & (pl.col("observed_status") != OBSERVED_STATUS)
        )
        .select(["event_key", *FIXED_EFFECT_COLUMNS])
        .collect()
    )
    n_production = production.get_column("event_key").n_unique()
    if n_production == 0:
        _log.info(
            "FE coverage guard skipped: no handler-unobserved production events "
            "(dimension=%r)",
            dimension,
        )
        return
    sparse: list[tuple[str, float]] = []
    for column in FIXED_EFFECT_COLUMNS:
        if column not in production.columns:
            sparse.append((column, 0.0))
            continue
        events_with_value = (
            production.filter(pl.col(column).is_not_null())
            .get_column("event_key")
            .n_unique()
        )
        rate = events_with_value / n_production
        if rate < floor:
            sparse.append((column, rate))
        else:
            _log.info(
                "FE coverage on production slice (n=%d, dimension=%s): %s = %.4f",
                n_production,
                dimension,
                column,
                rate,
            )
    if sparse:
        detail = ", ".join(f"{name}={rate:.4f}" for name, rate in sparse)
        raise ValueError(
            f"fixed-effect coverage on the handler-unobserved production slice "
            f"(n={n_production}, dimension={dimension!r}) below floor {floor:.4f}: {detail}. "
            "Drop the column from FIXED_EFFECT_COLUMNS — features that are not "
            "populated on the inference target cannot generalize."
        )


def build_ball_handler_production_frame(
    parquet_path: Path,
    *,
    dimension: str,
    fixed_effects: dict[str, FixedEffectDesign],
) -> ProductionScoringFrame:
    """One row per handler-unobserved event with FE codes in the training vocab.

    The production slice is ``dimension=<dim> AND observed_status !=
    'observed'``. Every FE is encoded against its training ``levels``
    vocabulary; unseen levels encode to ``-1`` and are dropped by the
    softmax reconstruction's validity mask.
    """
    per_event = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col("dimension") == dimension)
            & (pl.col("observed_status") != OBSERVED_STATUS)
        )
        .select(["event_key", *FIXED_EFFECT_COLUMNS])
        .unique(subset=["event_key"])
        .sort("event_key")
        .collect()
    )
    if per_event.height == 0:
        return ProductionScoringFrame(
            event_keys=np.zeros(0, dtype=np.int64),
            fixed_effects={},
        )

    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )
    scoring_fe: dict[str, FixedEffectDesign] = {
        column: FixedEffectDesign(
            levels=design.levels,
            codes=_encode_codes_with_vocab(per_event, column, list(design.levels)),
        )
        for column, design in fixed_effects.items()
    }
    return ProductionScoringFrame(event_keys=event_keys, fixed_effects=scoring_fe)


def prepare_ball_handler_inputs(
    parquet_path: Path,
    *,
    dimension: str = "ball_handler_position",
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
    min_events_per_season: int = MIN_EVENTS_PER_SEASON,
    held_out_fold_id: int = HOLDOUT_FOLD_ID,
    held_out_fold_count: int = HOLDOUT_FOLD_COUNT,
) -> BallHandlerInputs:
    """Read the modeling-dataset parquet and shape K=9 categorical inputs.

    Filters to the handler-observed slice (``observed_status='observed'``,
    ``training_weight > 0``), casts ``raw_value`` to an integer position
    in 1..9, drops season-league cells below ``min_events_per_season``,
    holds out 10% of games for OOS scoring, and emits one-hot per-event
    truth plus FE / season-league / scorer designs.
    """
    _assert_fixed_effects_cover_production_slice(parquet_path, dimension=dimension)

    df = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col("dimension") == dimension)
            & (pl.col("observed_status") == OBSERVED_STATUS)
            & (pl.col("training_weight") > 0.0)
        )
        .pipe(_with_handler_and_season_league)
        .filter(
            pl.col("_handler").is_not_null()
            & (pl.col("_handler") >= 1)
            & (pl.col("_handler") <= N_POSITIONS)
        )
        .collect()
    )
    if df.height == 0:
        raise ValueError(
            f"no handler-observed rows in 1..{N_POSITIONS} for dimension={dimension!r} "
            f"in {parquet_path}"
        )

    cell_counts = df.group_by("season_league").agg(pl.len().alias("_n"))
    informative_cells = cell_counts.filter(
        pl.col("_n") >= min_events_per_season
    ).get_column("season_league")
    dropped = cell_counts.height - informative_cells.len()
    if dropped:
        dropped_cells = (
            cell_counts.filter(pl.col("_n") < min_events_per_season)
            .sort("season_league")
            .get_column("season_league")
            .to_list()
        )
        _log.info(
            "prepare_ball_handler_inputs dropped %d season-league cells with <%d events: %s",
            dropped,
            min_events_per_season,
            dropped_cells,
        )
        df = df.filter(pl.col("season_league").is_in(informative_cells.implode()))
        if df.height == 0:
            raise ValueError(
                f"every season-league cell fell below min_events_per_season="
                f"{min_events_per_season} for dimension={dimension!r}; nothing to fit"
            )

    distinct_game_ids = df.get_column("game_id").unique().to_list()
    holdout_game_ids = [
        g
        for g in distinct_game_ids
        if game_hash_fold(g, fold_count=held_out_fold_count) == held_out_fold_id
    ]
    held_out_df = df.filter(pl.col("game_id").is_in(holdout_game_ids))
    df_train = df.filter(~pl.col("game_id").is_in(holdout_game_ids))
    _log.info(
        "prepare_ball_handler_inputs held-out %d/%d games via fold %d/%d",
        len(holdout_game_ids),
        len(distinct_game_ids),
        held_out_fold_id,
        held_out_fold_count,
    )
    if df_train.height == 0:
        raise ValueError("every game landed in the held-out fold; nothing to fit")

    if smoke_limit is not None:
        events_per_game = (
            df_train.select(["game_id", "event_key"])
            .unique()
            .group_by("game_id")
            .agg(pl.len().alias("n_events"))
            .sort("game_id")
        )
        total_events = int(events_per_game.get_column("n_events").sum())
        if total_events > smoke_limit:
            shuffled = events_per_game.sample(
                fraction=1.0, seed=seed, shuffle=True
            ).with_columns(pl.col("n_events").cum_sum().alias("cum_events"))
            kept_games = shuffled.filter(
                pl.col("cum_events") <= smoke_limit
            ).get_column("game_id")
            if kept_games.len() == 0:
                kept_games = shuffled.head(1).get_column("game_id")
            df_train = df_train.filter(pl.col("game_id").is_in(kept_games.implode()))
            _log.info(
                "prepare_ball_handler_inputs game-subsampled to %d games (%d events; "
                "budget=%d, seed=%d)",
                kept_games.len(),
                df_train.height,
                smoke_limit,
                seed,
            )

    per_event = df_train.sort("event_key")

    handler = per_event.get_column("_handler").to_numpy().astype(np.int64)
    counts = _one_hot_counts(handler)
    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )

    season_league_idx, season_league_labels = _category_index(
        per_event, "season_league"
    )
    scorer_idx, scorer_labels = _category_index(per_event, "scorer")
    _, park_labels = _category_index(per_event, "park_id")
    _, source_labels = _category_index(per_event, "source_family")

    per_event_cols = set(per_event.columns)
    fixed_effects: dict[str, FixedEffectDesign] = {}
    for column in FIXED_EFFECT_COLUMNS:
        if column not in per_event_cols:
            _log.info(
                "prepare_ball_handler_inputs skipping FE column %r — not present",
                column,
            )
            continue
        fixed_effects[column] = _build_fixed_effect_design(per_event, column)

    coords: dict[str, list[str]] = {
        "position": list(POSITION_LABELS),
        "season_league": list(season_league_labels),
        "scorer": list(scorer_labels),
        "source": list(source_labels),
        "park": list(park_labels),
    }
    for name, design in fixed_effects.items():
        coords[f"{name}_levels"] = list(design.levels)

    held_out = _build_ball_handler_held_out_set(
        held_out_df,
        season_league_labels=season_league_labels,
        scorer_labels=scorer_labels,
        park_labels=park_labels,
        source_labels=source_labels,
        fixed_effects=fixed_effects,
    )

    _log.info(
        "prepare_ball_handler_inputs K=%d train_events=%d held_out_events=%d "
        "season_leagues=%d scorers=%d parks=%d sources=%d",
        N_POSITIONS,
        per_event.height,
        held_out.n_events,
        len(season_league_labels),
        len(scorer_labels),
        len(park_labels),
        len(source_labels),
    )

    return BallHandlerInputs(
        event_keys=event_keys,
        credit_type=CREDIT_TYPE,
        n_positions=N_POSITIONS,
        counts=counts,
        season_league_idx=season_league_idx,
        scorer_idx=scorer_idx,
        fixed_effects=fixed_effects,
        coords=coords,
        held_out=held_out,
    )


def _build_ball_handler_held_out_set(
    df: pl.DataFrame,
    *,
    season_league_labels: list[str],
    scorer_labels: list[str],
    park_labels: list[str],
    source_labels: list[str],
    fixed_effects: dict[str, FixedEffectDesign],
) -> HeldOutSet:
    empty = np.zeros(0, dtype=np.int64)
    if df.height == 0:
        return HeldOutSet(
            event_keys=empty,
            true_position=empty,
            U=empty,
            season_idx=empty,
            scorer_idx=empty,
            park_idx=empty,
            source_idx=empty,
            fixed_effects={},
        )

    per_event = df.sort("event_key")
    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )
    true_position = per_event.get_column("_handler").to_numpy().astype(np.int64) - 1
    u_counts = np.ones_like(true_position, dtype=np.int64)

    season_idx = _encode_codes_with_vocab(
        per_event, "season_league", season_league_labels
    )
    scorer_idx = _encode_codes_with_vocab(per_event, "scorer", scorer_labels)
    park_idx = _encode_codes_with_vocab(per_event, "park_id", park_labels)
    source_idx = _encode_codes_with_vocab(per_event, "source_family", source_labels)

    held_fe: dict[str, FixedEffectDesign] = {
        column: FixedEffectDesign(
            levels=design.levels,
            codes=_encode_codes_with_vocab(per_event, column, list(design.levels)),
        )
        for column, design in fixed_effects.items()
    }

    return HeldOutSet(
        event_keys=event_keys,
        true_position=true_position,
        U=u_counts,
        season_idx=season_idx,
        scorer_idx=scorer_idx,
        park_idx=park_idx,
        source_idx=source_idx,
        fixed_effects=held_fe,
    )
