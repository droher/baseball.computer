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
from pathlib import Path
from typing import ClassVar

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict

_log = logging.getLogger(__name__)

IntArray = npt.NDArray[np.int64]
FloatArray = npt.NDArray[np.float64]
ByteArray = npt.NDArray[np.int8]

DEFAULT_SMOKE_LIMIT: int = 100_000
DEFAULT_SEED: int = 20260513
MIN_RARE_CLASS_COUNT_PER_SEASON: int = 100
MIN_RARE_CLASS_RATE_PER_SEASON: float = 0.005

UNKNOWN_LEVEL: str = "__unknown__"

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


def _build_fixed_effect_design(
    df: pl.DataFrame, column: str
) -> FixedEffectDesign:
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


def prepare_event_observation_inputs(
    parquet_path: Path,
    *,
    dimension: str,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
) -> EventObservationInputs:
    """Load and shape the event-grain inputs for the observation builder.

    Seasons whose rare-class share (the smaller of observed /
    unobserved row counts, divided by season N) falls below
    ``MIN_RARE_CLASS_RATE_PER_SEASON`` are dropped before sampling, as
    are seasons with a rare-class count below
    ``MIN_RARE_CLASS_COUNT_PER_SEASON``. The 2021+ trajectory data is
    functionally saturated (1-31 unobserved out of ~125k each year)
    and the modern era is near-saturated; the likelihood gives
    near-zero gradient per parameter and creates ridge correlations
    among ``alpha``, ``beta_season[s]``, and any season-only
    ``beta_park[p]`` (e.g. the 2025-only ``SAC01`` / ``TAM02`` /
    ``BST01`` parks). Downstream consumers should interpret a missing
    per-event ``p_observed_mean`` as "fully observed by construction"
    (p=1) and skip MNAR reweighting on those events.
    """
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
    saturated_mask = (
        (pl.col("_rare_count") < MIN_RARE_CLASS_COUNT_PER_SEASON)
        | (pl.col("_rare_rate") < MIN_RARE_CLASS_RATE_PER_SEASON)
    )
    informative_seasons = season_counts.filter(~saturated_mask).get_column("season")
    dropped = season_counts.height - informative_seasons.len()
    if dropped:
        dropped_seasons = (
            season_counts.filter(saturated_mask).sort("season").get_column("season").to_list()
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

    _log.info(
        "prepare_event_observation_inputs dim=%s rows=%d seasons=%d scorers=%d parks=%d sources=%d",
        dimension,
        df.height,
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
    )
