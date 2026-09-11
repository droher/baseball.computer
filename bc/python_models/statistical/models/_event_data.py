"""Event-grain prep for the Phase-4 observation propensity model.

Reads the frozen ``model_input_observation_batted_ball`` Parquet (which
already INNER JOINs ``event_observation_context`` upstream), filters to
``(dimension, training_weight > 0)``, and emits ``EventObservationInputs``:
integer indexers per random-effect dim, design matrices per fixed-effect
categorical, standardized continuous slopes plus paired missing
indicators, and the coord dict for PyMC.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAny=false

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import ClassVar

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict

from python_models.statistical.geometry_contract import require_geometry_parquet
from python_models.statistical.splits import game_hash_fold

_log = logging.getLogger(__name__)

IntArray = npt.NDArray[np.int64]
FloatArray = npt.NDArray[np.float64]
ByteArray = npt.NDArray[np.int8]

DEFAULT_SMOKE_LIMIT: int = 100_000
DEFAULT_SEED: int = 20260513
MIN_RARE_CLASS_COUNT_PER_SEASON: int = 100
MIN_RARE_CLASS_RATE_PER_SEASON: float = 0.005

UNKNOWN_LEVEL: str = "__unknown__"

HOLDOUT_FOLD_COUNT: int = 10
HOLDOUT_FOLD_ID: int = 0

RANDOM_EFFECT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("season", "season"),
    ("scorer", "scorer"),
    ("park", "park_id"),
    ("source", "source_family"),
)

FIXED_EFFECT_COLUMNS: tuple[str, ...] = (
    "game_type",
    "frame_start",
    "exposure_status",
    "league",
    "result_family",
    "pa_result",
    "leverage_bucket",
    "batter_hand",
    "pitcher_hand",
    "personnel_confidence",
    "context_confidence",
)

CONTINUOUS_COLUMNS: tuple[str, ...] = (
    "outs_start",
    "inning_start",
    "score_margin",
    "leverage_index",
    "runs_on_play",
)


class FixedEffectDesign(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    levels: tuple[str, ...]
    codes: IntArray


class ContinuousFeature(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    values: FloatArray
    is_missing: ByteArray
    raw_mean: float
    raw_std: float


class ObservationHeldOutSet(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    y: ByteArray
    event_keys: IntArray

    season_idx: IntArray
    scorer_idx: IntArray
    park_idx: IntArray
    source_idx: IntArray

    fixed_effects: dict[str, FixedEffectDesign]
    continuous: dict[str, ContinuousFeature]

    @property
    def n_events(self) -> int:
        return int(self.y.shape[0])


class EventObservationInputs(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    y: ByteArray
    event_keys: IntArray
    dimension: str

    season_idx: IntArray
    scorer_idx: IntArray
    park_idx: IntArray
    source_idx: IntArray

    fixed_effects: dict[str, FixedEffectDesign]
    continuous: dict[str, ContinuousFeature]

    coords: dict[str, list[str]]

    held_out: ObservationHeldOutSet | None = None

    @property
    def n_events(self) -> int:
        return int(self.y.shape[0])


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


def _assert_contiguous(idx: IntArray, labels: list[str], column: str) -> None:
    expected = set(range(len(labels)))
    actual: set[int] = {int(v) for v in np.unique(idx).tolist()}
    if actual != expected:
        raise AssertionError(
            f"{column} indexer contiguity check failed: expected {expected}, got {actual}"
        )


def _build_fixed_effect_design(df: pl.DataFrame, column: str) -> FixedEffectDesign:
    codes, labels = _category_index(df, column)
    if not labels:
        raise ValueError(f"fixed-effect column {column!r} has zero levels")
    return FixedEffectDesign(levels=tuple(labels), codes=codes)


def _build_continuous_feature(df: pl.DataFrame, column: str) -> ContinuousFeature:
    series = df.get_column(column)
    raw = series.cast(pl.Float64).to_numpy().astype(np.float64)
    is_missing = (~np.isfinite(raw)).astype(np.int8)
    finite_mask = is_missing == 0
    if not finite_mask.any():
        raw_mean = 0.0
        raw_std = 1.0
    else:
        raw_mean = float(np.mean(raw[finite_mask]))
        raw_std = float(np.std(raw[finite_mask], ddof=0))
        if raw_std < 1e-12:
            raw_std = 1.0
    standardized = np.where(finite_mask, (raw - raw_mean) / raw_std, 0.0)
    return ContinuousFeature(
        values=standardized.astype(np.float64),
        is_missing=is_missing,
        raw_mean=raw_mean,
        raw_std=raw_std,
    )


def _encode_codes_with_vocab(
    per_event: pl.DataFrame, column: str, labels: Sequence[str]
) -> IntArray:
    """Map a per-event column to int codes against a fixed ``labels`` vocab.

    Booleans cast to utf8. A NULL encodes to ``UNKNOWN_LEVEL``'s index (the
    level the training NULLs were fit under), or ``-1`` when the vocabulary
    has no NULL level. A non-null value absent from the vocabulary encodes to
    ``-1`` so every consumer gives it a zero effect rather than the NULL
    level's effect.
    """
    series = per_event.get_column(column)
    if series.dtype == pl.Boolean:
        series = series.cast(pl.Utf8)
    series = series.fill_null(UNKNOWN_LEVEL).cast(pl.Utf8)
    mapping = {c: i for i, c in enumerate(labels)}
    return (
        series.replace_strict(mapping, default=-1, return_dtype=pl.Int64)
        .to_numpy()
        .astype(np.int64)
    )


def _log_unseen_level_rates(
    frame_label: str, codes_by_column: dict[str, IntArray]
) -> None:
    for column, codes in codes_by_column.items():
        n = int(codes.shape[0])
        if n == 0:
            continue
        unseen = int((codes < 0).sum())
        _log.log(
            logging.INFO if unseen else logging.DEBUG,
            "%s column=%s unseen_levels=%d/%d rate=%.4f",
            frame_label,
            column,
            unseen,
            n,
            unseen / n,
        )


def _standardize_with_training_stats(
    df: pl.DataFrame, column: str, training: ContinuousFeature
) -> ContinuousFeature:
    raw = df.get_column(column).cast(pl.Float64).to_numpy().astype(np.float64)
    is_missing = (~np.isfinite(raw)).astype(np.int8)
    finite_mask = is_missing == 0
    standardized = np.where(
        finite_mask, (raw - training.raw_mean) / training.raw_std, 0.0
    )
    return ContinuousFeature(
        values=standardized.astype(np.float64),
        is_missing=is_missing,
        raw_mean=training.raw_mean,
        raw_std=training.raw_std,
    )


def _build_observation_held_out_set(
    df: pl.DataFrame,
    *,
    season_labels: list[str],
    scorer_labels: list[str],
    park_labels: list[str],
    source_labels: list[str],
    fixed_effects: dict[str, FixedEffectDesign],
    continuous: dict[str, ContinuousFeature],
    frame_label: str,
) -> ObservationHeldOutSet:
    empty_int = np.zeros(0, dtype=np.int64)
    if df.height == 0:
        return ObservationHeldOutSet(
            y=np.zeros(0, dtype=np.int8),
            event_keys=empty_int,
            season_idx=empty_int,
            scorer_idx=empty_int,
            park_idx=empty_int,
            source_idx=empty_int,
            fixed_effects={},
            continuous={},
        )

    per_event = df.sort("event_key")
    y = (
        per_event.get_column("is_observed")
        .fill_null(False)
        .cast(pl.Int8)
        .to_numpy()
        .astype(np.int8)
    )
    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )

    season_idx = _encode_codes_with_vocab(per_event, "season", season_labels)
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
    held_continuous: dict[str, ContinuousFeature] = {
        column: _standardize_with_training_stats(per_event, column, feature)
        for column, feature in continuous.items()
    }
    _log_unseen_level_rates(
        frame_label,
        {
            "season": season_idx,
            "scorer": scorer_idx,
            "park_id": park_idx,
            "source_family": source_idx,
            **{column: design.codes for column, design in held_fe.items()},
        },
    )

    return ObservationHeldOutSet(
        y=y,
        event_keys=event_keys,
        season_idx=season_idx,
        scorer_idx=scorer_idx,
        park_idx=park_idx,
        source_idx=source_idx,
        fixed_effects=held_fe,
        continuous=held_continuous,
    )


def build_observation_scoring_frame(
    parquet_path: Path,
    *,
    dimension: str,
    inputs: EventObservationInputs,
) -> ObservationHeldOutSet:
    """Build the full-coverage production scoring frame for one dimension.

    Selects every row of ``dimension`` from the dataset Parquet —
    observed and unobserved, with no ``training_weight`` filter —
    restricted to the seasons present in ``inputs.coords["season"]``
    (the seasons that survived the saturated-season filter and carry a
    fitted season random effect). Categoricals encode against the
    training vocabularies (unseen levels -> ``-1``, the convention
    ``_posterior_held_out_means_bernoulli`` masks to the prior mean)
    and continuous covariates standardize with the frozen training
    ``raw_mean`` / ``raw_std``. Rows come back sorted by ``event_key``.
    """
    if dimension == "location_side":
        require_geometry_parquet(parquet_path, dimension_column="dimension")
    needed_columns = sorted(
        {
            "event_key",
            "is_observed",
            "season",
            "scorer",
            "park_id",
            "source_family",
            *inputs.fixed_effects,
            *inputs.continuous,
        }
    )
    df = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col("dimension") == dimension)
            & pl.col("season").cast(pl.Utf8).is_in(inputs.coords["season"])
        )
        .select(needed_columns)
        .collect()
    )
    if df.height == 0:
        raise ValueError(
            f"no rows for dimension={dimension!r} within fitted seasons in {parquet_path}"
        )

    frame = _build_observation_held_out_set(
        df,
        season_labels=list(inputs.coords["season"]),
        scorer_labels=list(inputs.coords["scorer"]),
        park_labels=list(inputs.coords["park"]),
        source_labels=list(inputs.coords["source"]),
        fixed_effects=inputs.fixed_effects,
        continuous=inputs.continuous,
        frame_label=f"build_observation_scoring_frame dim={dimension}",
    )
    _log.info(
        "build_observation_scoring_frame dim=%s rows=%d seasons=%d",
        dimension,
        frame.n_events,
        len(inputs.coords["season"]),
    )
    return frame


def prepare_event_observation_inputs(
    parquet_path: Path,
    *,
    dimension: str,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
    held_out_fold_id: int = HOLDOUT_FOLD_ID,
    held_out_fold_count: int = HOLDOUT_FOLD_COUNT,
) -> EventObservationInputs:
    """Load and shape the event-grain inputs for the observation builder.

    Seasons whose rare-class share (the smaller of observed /
    unobserved row counts, divided by season N) falls below
    ``MIN_RARE_CLASS_RATE_PER_SEASON`` are dropped before sampling, as
    are seasons with a rare-class count below
    ``MIN_RARE_CLASS_COUNT_PER_SEASON``. After the saturated-season
    filter, a deterministic game-disjoint holdout
    (``game_hash_fold(game_id, fold_count=held_out_fold_count) ==
    held_out_fold_id``) is removed before any row subsample; the
    held-out events ship on ``EventObservationInputs.held_out`` encoded
    against the training vocabularies (unseen levels -> ``-1``,
    continuous covariates standardized with the training means/stds)
    for OOS scoring. The 2021+ trajectory data is
    functionally saturated (1-31 unobserved out of ~125k each year)
    and the modern era is near-saturated; the likelihood gives
    near-zero gradient per parameter and creates ridge correlations
    among ``alpha``, ``beta_season[s]``, and any season-only
    ``beta_park[p]`` (e.g. the 2025-only ``SAC01`` / ``TAM02`` /
    ``BST01`` parks). Downstream consumers should interpret a missing
    per-event ``p_observed_mean`` as "fully observed by construction"
    (p=1) and skip MNAR reweighting on those events.
    """
    if dimension == "location_side":
        require_geometry_parquet(parquet_path, dimension_column="dimension")
    df = pl.read_parquet(parquet_path)
    df = df.filter(
        (pl.col("dimension") == dimension) & (pl.col("training_weight") > 0.0)
    )
    if df.height == 0:
        raise ValueError(
            f"no rows for dimension={dimension!r} with training_weight>0 in {parquet_path}"
        )

    season_counts = df.group_by("season").agg(
        pl.col("is_observed").cast(pl.Int64).sum().alias("_observed"),
        (~pl.col("is_observed")).cast(pl.Int64).sum().alias("_unobserved"),
    )
    season_counts = season_counts.with_columns(
        (pl.col("_observed") + pl.col("_unobserved")).alias("_n"),
        pl.min_horizontal("_observed", "_unobserved").alias("_rare_count"),
    )
    season_counts = season_counts.with_columns(
        (pl.col("_rare_count") / pl.col("_n")).alias("_rare_rate"),
    )
    saturated_mask = (pl.col("_rare_count") < MIN_RARE_CLASS_COUNT_PER_SEASON) | (
        pl.col("_rare_rate") < MIN_RARE_CLASS_RATE_PER_SEASON
    )
    informative_seasons = season_counts.filter(~saturated_mask).get_column("season")
    dropped = season_counts.height - informative_seasons.len()
    if dropped:
        dropped_seasons = (
            season_counts.filter(saturated_mask)
            .sort("season")
            .get_column("season")
            .to_list()
        )
        _log.info(
            "prepare_event_observation_inputs dropped %d saturated seasons (rare_count<%d or rare_rate<%.4f): %s",
            dropped,
            MIN_RARE_CLASS_COUNT_PER_SEASON,
            MIN_RARE_CLASS_RATE_PER_SEASON,
            dropped_seasons,
        )
        df = df.filter(pl.col("season").is_in(informative_seasons.implode()))
        if df.height == 0:
            raise ValueError(
                f"every season was saturated (rare_count<{MIN_RARE_CLASS_COUNT_PER_SEASON} "
                f"or rare_rate<{MIN_RARE_CLASS_RATE_PER_SEASON}) for "
                f"dimension={dimension!r}; nothing to fit"
            )

    distinct_game_ids = df.get_column("game_id").unique().to_list()
    holdout_game_ids = [
        g
        for g in distinct_game_ids
        if game_hash_fold(g, fold_count=held_out_fold_count) == held_out_fold_id
    ]
    held_out_df = df.filter(pl.col("game_id").is_in(holdout_game_ids))
    df = df.filter(~pl.col("game_id").is_in(holdout_game_ids))
    _log.info(
        "prepare_event_observation_inputs held-out %d/%d games via fold %d/%d",
        len(holdout_game_ids),
        len(distinct_game_ids),
        held_out_fold_id,
        held_out_fold_count,
    )
    if df.height == 0:
        raise ValueError("every game landed in the held-out fold; nothing to fit")

    if smoke_limit is not None and df.height > smoke_limit:
        df = df.sample(n=smoke_limit, seed=seed)
        _log.info(
            "prepare_event_observation_inputs subsampled to %d rows (seed=%d)",
            smoke_limit,
            seed,
        )

    season_idx, season_labels = _category_index(df, "season")
    scorer_idx, scorer_labels = _category_index(df, "scorer")
    park_idx, park_labels = _category_index(df, "park_id")
    source_idx, source_labels = _category_index(df, "source_family")

    y = df.get_column("is_observed").fill_null(False).cast(pl.Int8).to_numpy()
    event_keys = df.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)

    fixed_effects: dict[str, FixedEffectDesign] = {}
    df_columns = set(df.columns)
    for column in FIXED_EFFECT_COLUMNS:
        if column not in df_columns:
            _log.info(
                "prepare_event_observation_inputs skipping FE column %r — not present in dataset parquet",
                column,
            )
            continue
        fixed_effects[column] = _build_fixed_effect_design(df, column)

    continuous: dict[str, ContinuousFeature] = {}
    for column in CONTINUOUS_COLUMNS:
        continuous[column] = _build_continuous_feature(df, column)

    coords: dict[str, list[str]] = {
        "season": list(season_labels),
        "scorer": list(scorer_labels),
        "park": list(park_labels),
        "source": list(source_labels),
    }
    for name, design in fixed_effects.items():
        coords[f"{name}_levels"] = list(design.levels)

    held_out = _build_observation_held_out_set(
        held_out_df,
        season_labels=list(season_labels),
        scorer_labels=list(scorer_labels),
        park_labels=list(park_labels),
        source_labels=list(source_labels),
        fixed_effects=fixed_effects,
        continuous=continuous,
        frame_label=f"prepare_event_observation_inputs held_out dim={dimension}",
    )

    _log.info(
        "prepare_event_observation_inputs dim=%s rows=%d held_out_events=%d "
        "seasons=%d scorers=%d parks=%d sources=%d",
        dimension,
        df.height,
        held_out.n_events,
        len(season_labels),
        len(scorer_labels),
        len(park_labels),
        len(source_labels),
    )

    return EventObservationInputs(
        y=y.astype(np.int8),
        event_keys=event_keys,
        dimension=dimension,
        season_idx=season_idx,
        scorer_idx=scorer_idx,
        park_idx=park_idx,
        source_idx=source_idx,
        fixed_effects=fixed_effects,
        continuous=continuous,
        coords=coords,
        held_out=held_out,
    )
