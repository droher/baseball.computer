"""Event-grain prep for the dual-arm synthetic-mask credit allocation model.

Reads the frozen ``model_input_fielding_credit`` Parquet at grain
``(event_key, player_id, fielding_position, credit_type)``, filters to
well-attributed events (``credit_type=<c>, known_credit > 0,
personnel_hard_mask_available=TRUE``) where the true position of every
credit is observed, and synthetically masks a position-weighted subset
per game to mimic the natural-unknown allocation task. The output
``EventCreditInputs`` carries:

* per-event design matrices (REs, FEs, U_e),
* supervised-arm arrays (``Y_supervised_event_idx``,
  ``Y_supervised_count``) over the unmasked events where Y is observed,
* aggregate-arm arrays at the per-(game, team, player, position) target
  grain over the masked events, with T_target deterministically computed
  from the hidden known_credit (we control the masking).

A deterministic per-game held-out split (``HOLDOUT_FOLD_COUNT=10`` via
``game_hash_fold``, fold 0) is removed before training; the held-out
events ship alongside training inputs for OOS scoring.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging
import os
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
ByteArray = npt.NDArray[np.int8]

DEFAULT_SMOKE_LIMIT: int = 100_000
DEFAULT_SEED: int = 20260513
MIN_EVENTS_PER_SEASON: int = 50
N_POSITIONS: int = 9
POSITION_LABELS: tuple[str, ...] = tuple(str(p) for p in range(1, N_POSITIONS + 1))

NONE_POSITION_LABEL: str = "NONE"
N_POSITIONS_ASSIST: int = 10
POSITION_LABELS_ASSIST: tuple[str, ...] = (*POSITION_LABELS, NONE_POSITION_LABEL)

PUTOUT_POSITION_FE_COLUMN: str = "putout_position"
PUTOUT_POSITION_LEVELS: tuple[str, ...] = POSITION_LABELS

UNKNOWN_LEVEL: str = "__unknown__"

HOLDOUT_FOLD_COUNT: int = 10
HOLDOUT_FOLD_ID: int = 0

NATURAL_UNK_RATE_FILTER_ENV: str = "BC_CREDIT_MIN_NATURAL_UNK_RATE"

REAL_UNKNOWN_RATES_BY_POSITION: tuple[float, ...] = (
    0.017672,
    0.068823,
    0.421782,
    0.090316,
    0.042862,
    0.077579,
    0.090769,
    0.111029,
    0.079167,
)

DEFAULT_NATURAL_UNKNOWN_RATE_FALLBACK: float = 0.10

RANDOM_EFFECT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("season", "season"),
    ("scorer", "scorer"),
    ("park", "park_id"),
    ("source", "source_family"),
)

FIXED_EFFECT_COLUMNS: tuple[str, ...] = (
    "result_family",
    "base_state_start",
    "outs_start",
    "frame_start",
    "alignment_regime",
)

PRODUCTION_FE_COVERAGE_FLOOR: float = 0.01

GLOBAL_EFFECT_COLUMNS: tuple[str, ...] = (
    "personnel_confidence",
    "context_confidence",
    "exposure_status",
)


class FixedEffectDesign(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    levels: tuple[str, ...]
    codes: IntArray


class HeldOutSet(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    event_keys: IntArray
    true_position: IntArray
    U: IntArray
    season_idx: IntArray
    scorer_idx: IntArray
    park_idx: IntArray
    source_idx: IntArray
    fixed_effects: dict[str, FixedEffectDesign]

    @property
    def n_events(self) -> int:
        return int(self.event_keys.shape[0])


class ProductionScoringFrame(BaseModel):
    """Event-grain FE codes for scoring the production (unknown-credit) slice.

    Carries one entry per production event_key plus per-FE codes encoded
    in the training vocabulary; the per-event softmax depends only on
    these fixed-effect codes.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    event_keys: IntArray
    fixed_effects: dict[str, FixedEffectDesign]

    @property
    def n_events(self) -> int:
        return int(self.event_keys.shape[0])


class EventCreditInputs(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    U: IntArray
    event_keys: IntArray
    credit_type: str
    n_positions: int

    season_idx: IntArray
    scorer_idx: IntArray
    park_idx: IntArray
    source_idx: IntArray

    fixed_effects: dict[str, FixedEffectDesign]
    global_effects: dict[str, FixedEffectDesign]

    coords: dict[str, list[str]]

    is_masked: npt.NDArray[np.bool_]

    Y_supervised_event_idx: IntArray
    Y_supervised_counts: IntArray
    Y_supervised_U: IntArray

    aggregate_targets: FloatArray
    aggregate_sigma: FloatArray
    aggregate_event_idx: IntArray
    aggregate_position_idx: IntArray
    aggregate_row_idx: IntArray

    held_out: HeldOutSet

    @property
    def n_events(self) -> int:
        return int(self.U.shape[0])

    @property
    def n_supervised_events(self) -> int:
        return int(self.Y_supervised_event_idx.shape[0])

    @property
    def n_targets(self) -> int:
        return int(self.aggregate_targets.shape[0])


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


def _collapse_event_grain(df: pl.DataFrame) -> pl.DataFrame:
    sorted_df = df.sort(["event_key", "fielding_position"])
    position_grid = sorted_df.group_by("event_key", maintain_order=True).agg(
        pl.col("player_id").alias("player_id_grid"),
        pl.col("fielding_position").alias("position_grid"),
        pl.col("known_credit").alias("known_credit_grid"),
    )

    expected_positions = list(range(1, N_POSITIONS + 1))
    has_full_grid = position_grid.with_columns(
        pl.col("position_grid")
        .map_elements(
            lambda lst: list(lst) == expected_positions,
            return_dtype=pl.Boolean,
        )
        .alias("_full_grid")
    )
    kept = has_full_grid.filter(pl.col("_full_grid"))
    dropped = has_full_grid.height - kept.height
    if dropped:
        _log.info(
            "_collapse_event_grain dropped %d events without full 1..9 position grid",
            dropped,
        )
    keep_keys = kept.get_column("event_key")
    sorted_df = sorted_df.filter(pl.col("event_key").is_in(keep_keys.implode()))

    per_event = sorted_df.group_by("event_key", maintain_order=True).agg(
        [
            pl.col(c).first().alias(c)
            for c in sorted_df.columns
            if c not in {"event_key", "player_id", "fielding_position", "known_credit"}
        ]
    )

    out = per_event.join(
        kept.select(["event_key", "player_id_grid", "known_credit_grid"]),
        on="event_key",
        how="left",
    )
    return out


def _per_event_true_position(known_grid: list[list[float]]) -> IntArray:
    arr = np.asarray(known_grid, dtype=np.float64)
    out = np.full(arr.shape[0], -1, dtype=np.int64)
    has_any = arr.max(axis=1) > 0
    out[has_any] = arr[has_any].argmax(axis=1)
    return out


def n_positions_for(dimension: str) -> int:
    if dimension == "assist":
        return N_POSITIONS_ASSIST
    return N_POSITIONS


def position_labels_for(dimension: str) -> tuple[str, ...]:
    if dimension == "assist":
        return POSITION_LABELS_ASSIST
    return POSITION_LABELS


def _resolve_putout_position_per_event(parquet_path: Path) -> pl.DataFrame:
    """Return one row per event_key with the observed putout position 1..9.

    Events with zero putouts (no putout known_credit) or more than one
    putout (DPs / TPs) are excluded. v3 cut 1 restricts the assist
    training set to single-putout events so the putout_position FE is
    well-defined per event.
    """
    putout = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col("credit_type") == "putout")
            & (pl.col("known_credit") > 0)
            & (pl.col("personnel_hard_mask_available") == True)  # noqa: E712
        )
        .select(["event_key", "fielding_position", "known_credit"])
        .collect()
    )
    if putout.height == 0:
        return pl.DataFrame(
            schema={"event_key": putout.schema.get("event_key", pl.UInt32()), "putout_position": pl.Int64()}
        )
    per_event = (
        putout.group_by("event_key", maintain_order=True)
        .agg(
            pl.col("known_credit").sum().alias("_total_putout"),
            pl.col("fielding_position").alias("_putout_positions"),
        )
        .filter(
            (pl.col("_total_putout") == 1.0)
            & (pl.col("_putout_positions").list.len() == 1)
        )
        .with_columns(
            pl.col("_putout_positions")
            .list.first()
            .cast(pl.Int64)
            .alias("putout_position"),
        )
        .select(["event_key", "putout_position"])
    )
    return per_event


def _assist_truth_per_event(per_event: pl.DataFrame) -> tuple[IntArray, IntArray]:
    """Compute A_count and A_position_0based per event from the assist grid.

    ``per_event`` must already carry ``known_credit_grid`` of length 9
    (the per-position assist counts) — built by ``_collapse_event_grain``.
    Returns ``(A_count, A_position_0based)`` where ``A_position_0based``
    is ``N_POSITIONS`` (= NONE sentinel index) for zero-assist events
    and the argmax otherwise. Rounds the grid to integers; multi-assist
    events (A_count > 1) are filtered upstream and are not expected.
    """
    grids = per_event.get_column("known_credit_grid").to_list()
    n = len(grids)
    a_count = np.zeros(n, dtype=np.int64)
    a_pos = np.full(n, N_POSITIONS, dtype=np.int64)
    for i, grid in enumerate(grids):
        arr = np.rint(np.asarray(grid, dtype=np.float64)).astype(np.int64)
        total = int(arr.sum())
        a_count[i] = total
        if total == 1:
            a_pos[i] = int(arr.argmax())
    return a_count, a_pos


def _natural_unknown_rate_lookup() -> dict[str, float]:
    return {
        f"{season}|{source}": rate
        for season, source, rate in _CACHED_NATURAL_UNKNOWN_RATES
    }


def _apply_synthetic_mask(
    per_event: pl.DataFrame,
    *,
    true_pos_0based: IntArray,
    seed: int,
    per_position_weights: tuple[float, ...] = REAL_UNKNOWN_RATES_BY_POSITION,
    fallback_rate: float = DEFAULT_NATURAL_UNKNOWN_RATE_FALLBACK,
) -> npt.NDArray[np.bool_]:
    """Per-event Bernoulli synthetic mask calibrated to natural unknowns.

    Per-event mask probability:
        P_e = clip(alpha_c * w[true_pos(e)], 0, 1)
    with alpha_c chosen per-(season, source_family) so the cell-mean
    mask rate matches the empirical natural-unknown rate from the v1
    authority cache (``_CACHED_NATURAL_UNKNOWN_RATES``). Cells missing
    from the cache fall back to ``fallback_rate``. At least one event
    per game stays unmasked so the supervised arm always covers every
    game.

    ``per_position_weights`` is length-K (9 for putout, 10 for assist
    with the NONE sentinel as the last entry). For the assist v3 cut 1
    we pass uniform weights — there is no empirical assist-unknown
    per-position rate yet.
    """
    rng = np.random.default_rng(seed)
    n = per_event.height
    w = np.asarray(per_position_weights, dtype=np.float64)
    k = int(w.shape[0])
    w_e = w[np.clip(true_pos_0based, 0, k - 1)]

    natural_rate = _natural_unknown_rate_lookup()
    cell_keys = (
        per_event.select(
            (
                pl.col("season").cast(pl.Utf8).fill_null(UNKNOWN_LEVEL)
                + "|"
                + pl.col("source_family").cast(pl.Utf8).fill_null(UNKNOWN_LEVEL)
            ).alias("cell_key")
        )
        .get_column("cell_key")
        .to_list()
    )

    p = np.zeros(n, dtype=np.float64)
    cell_to_indices: dict[str, list[int]] = {}
    for i, key in enumerate(cell_keys):
        cell_to_indices.setdefault(key, []).append(i)
    for key, idxs in cell_to_indices.items():
        target_rate = natural_rate.get(key, fallback_rate)
        if target_rate <= 0.0 or not np.isfinite(target_rate):
            continue
        weights = w_e[idxs]
        mean_w = float(weights.mean()) if weights.size > 0 else 0.0
        if mean_w <= 0.0:
            continue
        alpha = float(target_rate) / mean_w
        p[idxs] = np.clip(alpha * weights, 0.0, 1.0)

    draws = rng.random(n)
    mask = draws < p

    game_ids = per_event.get_column("game_id").to_list()
    games_to_event_idx: dict[str, list[int]] = {}
    for i, g in enumerate(game_ids):
        games_to_event_idx.setdefault(g, []).append(i)
    for g, idxs in games_to_event_idx.items():
        if all(mask[i] for i in idxs):
            unmask_pick = int(idxs[int(rng.integers(0, len(idxs)))])
            mask[unmask_pick] = False
    return mask


_CACHED_NATURAL_UNKNOWN_RATES: tuple[tuple[int, str, float], ...] = (
    (1910, "play_by_play", 0.194238),
    (1911, "play_by_play", 0.159857),
    (1912, "play_by_play", 0.119341),
    (1913, "play_by_play", 0.162400),
    (1914, "play_by_play", 0.206249),
    (1915, "play_by_play", 0.131900),
    (1916, "play_by_play", 0.117800),
    (1917, "play_by_play", 0.151900),
    (1918, "play_by_play", 0.114800),
    (1919, "play_by_play", 0.106800),
    (1920, "play_by_play", 0.114800),
    (1921, "play_by_play", 0.107200),
    (1922, "play_by_play", 0.106400),
    (1923, "play_by_play", 0.090000),
    (1924, "play_by_play", 0.099900),
    (1925, "play_by_play", 0.124529),
    (1926, "play_by_play", 0.098160),
    (1927, "play_by_play", 0.087257),
    (1928, "play_by_play", 0.097537),
    (1929, "play_by_play", 0.087658),
    (1930, "play_by_play", 0.083500),
    (1931, "play_by_play", 0.080900),
    (1932, "play_by_play", 0.083000),
    (1933, "play_by_play", 0.079400),
    (1934, "play_by_play", 0.074200),
    (1935, "play_by_play", 0.071800),
    (1936, "play_by_play", 0.070100),
    (1937, "play_by_play", 0.069900),
    (1938, "play_by_play", 0.069300),
    (1939, "play_by_play", 0.063700),
    (1940, "play_by_play", 0.059700),
    (1941, "play_by_play", 0.058700),
    (1942, "play_by_play", 0.063900),
    (1943, "play_by_play", 0.198600),
    (1944, "play_by_play", 0.204400),
    (1945, "play_by_play", 0.214800),
    (1946, "play_by_play", 0.063500),
    (1947, "play_by_play", 0.057000),
    (1948, "play_by_play", 0.055000),
    (1949, "play_by_play", 0.054000),
    (1950, "play_by_play", 0.052500),
    (1951, "play_by_play", 0.050000),
    (1952, "play_by_play", 0.046000),
    (1953, "play_by_play", 0.043900),
    (1954, "play_by_play", 0.042100),
    (1955, "play_by_play", 0.040200),
    (1956, "play_by_play", 0.039500),
    (1957, "play_by_play", 0.038700),
    (1958, "play_by_play", 0.037000),
    (1959, "play_by_play", 0.034000),
    (1960, "play_by_play", 0.030000),
    (1961, "play_by_play", 0.026000),
    (1962, "play_by_play", 0.022000),
    (1963, "play_by_play", 0.018000),
    (1964, "play_by_play", 0.014000),
    (1965, "play_by_play", 0.010000),
    (1966, "play_by_play", 0.008000),
    (1967, "play_by_play", 0.006000),
    (1968, "play_by_play", 0.004500),
    (1969, "play_by_play", 0.003500),
    (1970, "play_by_play", 0.002500),
    (1971, "play_by_play", 0.002000),
    (1972, "play_by_play", 0.001500),
    (1973, "play_by_play", 0.001000),
    (1974, "play_by_play", 0.000800),
    (1975, "play_by_play", 0.000700),
    (1976, "play_by_play", 0.000600),
    (1977, "play_by_play", 0.000500),
    (1978, "play_by_play", 0.000400),
    (1979, "play_by_play", 0.000400),
    (1980, "play_by_play", 0.000300),
    (1981, "play_by_play", 0.000300),
    (1982, "play_by_play", 0.000300),
    (1983, "play_by_play", 0.000200),
    (1984, "play_by_play", 0.000200),
    (1985, "play_by_play", 0.000200),
    (1986, "play_by_play", 0.000200),
    (1987, "play_by_play", 0.000200),
    (1988, "play_by_play", 0.000200),
    (1989, "play_by_play", 0.000100),
    (1990, "play_by_play", 0.000100),
    (1991, "play_by_play", 0.000100),
    (1992, "play_by_play", 0.000100),
    (1993, "play_by_play", 0.000100),
    (1994, "play_by_play", 0.000100),
    (1995, "play_by_play", 0.000100),
    (1996, "play_by_play", 0.000100),
    (1997, "play_by_play", 0.000100),
    (1998, "play_by_play", 0.000100),
    (1999, "play_by_play", 0.000100),
    (2000, "play_by_play", 0.000100),
    (2001, "play_by_play", 0.000100),
    (2002, "play_by_play", 0.000100),
    (2003, "play_by_play", 0.000100),
    (2004, "play_by_play", 0.000100),
    (2005, "play_by_play", 0.000050),
    (2006, "play_by_play", 0.000050),
    (2007, "play_by_play", 0.000050),
    (2008, "play_by_play", 0.000050),
    (2009, "play_by_play", 0.000050),
    (2010, "play_by_play", 0.000050),
    (2011, "play_by_play", 0.000050),
    (2012, "play_by_play", 0.000050),
    (2013, "play_by_play", 0.000050),
    (2014, "play_by_play", 0.000050),
    (2015, "play_by_play", 0.000050),
    (2016, "play_by_play", 0.000050),
    (2017, "play_by_play", 0.000050),
    (2018, "play_by_play", 0.000050),
    (2019, "play_by_play", 0.000050),
    (2020, "play_by_play", 0.000050),
    (2021, "play_by_play", 0.000050),
    (2022, "play_by_play", 0.000050),
    (2023, "play_by_play", 0.000050),
    (2024, "play_by_play", 0.000050),
    (2025, "play_by_play", 0.000050),
)


def _build_aggregate_targets_from_mask(
    per_event: pl.DataFrame,
    *,
    is_masked: npt.NDArray[np.bool_],
    sigma_box_aggregate: float,
) -> tuple[FloatArray, FloatArray, IntArray, IntArray, IntArray]:
    if not is_masked.any():
        zero_f = np.zeros(0, dtype=np.float64)
        zero_i = np.zeros(0, dtype=np.int64)
        return zero_f, zero_f, zero_i, zero_i, zero_i

    indexed = per_event.with_row_index("event_idx").with_columns(
        pl.Series("is_masked", is_masked.tolist())
    )
    masked = indexed.filter(pl.col("is_masked"))

    exploded = (
        masked.select(
            "event_idx",
            "event_key",
            "game_id",
            "fielding_team_id",
            pl.col("player_id_grid").alias("player_id_list"),
            pl.col("known_credit_grid").alias("known_list"),
        )
        .with_columns(pl.int_ranges(0, pl.col("player_id_list").list.len()).alias("k0"))
        .explode(["player_id_list", "known_list", "k0"])
        .with_columns(
            (pl.col("k0") + 1).alias("fielding_position"),
            pl.col("known_list").cast(pl.Float64).alias("known_credit"),
        )
        .rename({"player_id_list": "player_id"})
    )

    targets_long = (
        exploded.group_by(
            ["game_id", "fielding_team_id", "player_id", "fielding_position"]
        )
        .agg(pl.col("known_credit").sum().alias("t_target"))
        .filter(pl.col("t_target") > 0.0)
        .sort(["game_id", "fielding_team_id", "player_id", "fielding_position"])
        .with_row_index("target_row_dense")
    )
    targets = targets_long.get_column("t_target").to_numpy().astype(np.float64)
    sigmas = np.full(targets.shape[0], float(sigma_box_aggregate), dtype=np.float64)

    contributions = exploded.join(
        targets_long.select(
            "target_row_dense",
            "game_id",
            "fielding_team_id",
            "player_id",
            "fielding_position",
        ),
        on=["game_id", "fielding_team_id", "player_id", "fielding_position"],
        how="inner",
    ).sort(["target_row_dense", "event_idx", "fielding_position"])

    aggregate_event_idx = (
        contributions.get_column("event_idx").to_numpy().astype(np.int64)
    )
    aggregate_position_idx = (
        contributions.get_column("fielding_position").to_numpy().astype(np.int64) - 1
    )
    aggregate_row_idx = (
        contributions.get_column("target_row_dense").to_numpy().astype(np.int64)
    )
    return (
        targets,
        sigmas,
        aggregate_event_idx,
        aggregate_position_idx,
        aggregate_row_idx,
    )


def _build_supervised_arrays(
    counts_per_event: IntArray,
    *,
    is_masked: npt.NDArray[np.bool_],
    U: IntArray,
) -> tuple[IntArray, IntArray, IntArray]:
    """Slice the K-wide per-event truth counts to the unmasked subset.

    ``counts_per_event`` is shape ``(n_events, K)``. K is 9 for putout
    and 10 for assist (the trailing column is the NONE sentinel). The
    caller asserts ``counts_per_event.sum(axis=1) == U`` so a per-event
    Multinomial likelihood matches.
    """
    n, k = counts_per_event.shape
    sup_idx_int = np.where(~is_masked)[0].astype(np.int64)
    if sup_idx_int.size == 0:
        return (
            np.zeros(0, dtype=np.int64),
            np.zeros((0, k), dtype=np.int64),
            np.zeros(0, dtype=np.int64),
        )

    counts_sup = counts_per_event[sup_idx_int]
    U_sup = U[sup_idx_int]
    if int(counts_sup.sum()) != int(U_sup.sum()):
        raise AssertionError(
            "supervised counts do not sum to U over unmasked subset: "
            f"sum(counts)={int(counts_sup.sum())} sum(U)={int(U_sup.sum())}"
        )
    return sup_idx_int, counts_sup, U_sup


def _counts_grid_from_known(known_grid_list: list[list[float]]) -> IntArray:
    """Round a list-of-length-9 known_credit grid to a (n, 9) int matrix."""
    n = len(known_grid_list)
    out = np.zeros((n, N_POSITIONS), dtype=np.int64)
    for i, row in enumerate(known_grid_list):
        out[i] = np.rint(np.asarray(row, dtype=np.float64)).astype(np.int64)
    return out


def _counts_grid_for_assists(a_count: IntArray, a_pos_0based: IntArray) -> IntArray:
    """Build a (n, 10) one-hot for assists with NONE at column 9.

    A_count == 1 events get a 1 at the assist position. A_count == 0
    events get a 1 at the NONE sentinel column. Multi-assist events
    (A_count > 1) are filtered upstream and must not appear.
    """
    n = int(a_count.shape[0])
    out = np.zeros((n, N_POSITIONS_ASSIST), dtype=np.int64)
    out[np.arange(n), a_pos_0based] = 1
    sums = out.sum(axis=1)
    if not (sums == 1).all():
        bad = int((sums != 1).sum())
        raise AssertionError(
            f"_counts_grid_for_assists: {bad} rows do not sum to 1"
        )
    return out


def _assert_fixed_effects_cover_production_slice(
    parquet_path: Path,
    *,
    dimension: str,
    floor: float = PRODUCTION_FE_COVERAGE_FLOOR,
) -> None:
    """Each FE must be populated on more than ``floor`` of the production target.

    For ``putout``: filter on ``credit_type='putout' AND
    unknown_credit_need > 0`` directly (the natural production slice).
    For ``assist``: the dataset always has ``unknown_credit_need = 0``
    on assist rows (no upstream signal for unknown_assist yet), so we
    use the putout rows' ``unknown_credit_need > 0`` predicate to
    identify the inference target slice — events whose putout is
    unknown are the same events whose assist chain is unknown.
    ``putout_position`` is excluded from the standard check on
    assists because it's structurally NULL on the production slice
    (the whole point of v3 is to marginalize over it).
    """
    if dimension == "assist":
        production_credit_filter = "putout"
        guarded_columns = tuple(
            c for c in FIXED_EFFECT_COLUMNS if c != PUTOUT_POSITION_FE_COLUMN
        )
    else:
        production_credit_filter = dimension
        guarded_columns = FIXED_EFFECT_COLUMNS

    production = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col("credit_type") == production_credit_filter)
            & (pl.col("unknown_credit_need") > 0)
            & (pl.col("personnel_hard_mask_available") == True)  # noqa: E712
            & (pl.col("eligible_for_allocation") == True)  # noqa: E712
        )
        .select(["event_key", *[c for c in guarded_columns if c != PUTOUT_POSITION_FE_COLUMN]])
        .collect()
    )
    n_production = production.get_column("event_key").n_unique()
    if n_production == 0:
        _log.info(
            "FE coverage guard skipped: no production-target events "
            "(production_filter=%r, dimension=%r)",
            production_credit_filter,
            dimension,
        )
        return
    sparse: list[tuple[str, float]] = []
    for column in guarded_columns:
        if column == PUTOUT_POSITION_FE_COLUMN:
            continue
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
            f"fixed-effect coverage on the production unknown slice "
            f"(n={n_production}, dimension={dimension!r}) below floor {floor:.4f}: {detail}. "
            "Drop the column from FIXED_EFFECT_COLUMNS — features that "
            "are not populated on the inference target cannot generalize."
        )


def _assert_putout_position_levels_in_training(
    putout_position_codes: IntArray,
    *,
    floor: float = PRODUCTION_FE_COVERAGE_FLOOR,
) -> None:
    """Every j ∈ {1..9} must appear on at least ``floor`` of training events.

    Per the v3 plan: ``putout_position`` is structurally unobserved on
    the production target slice, so the standard per-event coverage
    check can't apply. Instead, every level needs to appear on a
    non-trivial fraction of training events so the per-level
    ``delta_putout_position`` is identifiable at the inference-time
    marginalization step.
    """
    n = int(putout_position_codes.shape[0])
    if n == 0:
        return
    sparse: list[tuple[int, float]] = []
    for j in range(1, N_POSITIONS + 1):
        rate = float((putout_position_codes == j).mean())
        if rate < floor:
            sparse.append((j, rate))
        else:
            _log.info(
                "training-set putout_position coverage (n=%d): pos=%d rate=%.4f",
                n,
                j,
                rate,
            )
    if sparse:
        detail = ", ".join(f"pos={j} rate={rate:.4f}" for j, rate in sparse)
        raise ValueError(
            f"putout_position levels under floor {floor:.4f} on training set "
            f"(n={n}): {detail}. Each j ∈ {{1..9}} must be observed on at "
            "least the floor fraction so delta_putout_position[j] is "
            "identifiable at inference-time marginalization."
        )


def build_production_scoring_frame(
    parquet_path: Path,
    *,
    dimension: str,
    fixed_effects: dict[str, FixedEffectDesign],
) -> ProductionScoringFrame:
    """One row per production-slice event with FE codes in the training vocab.

    The production slice is the unknown-credit inference target:
    ``credit_type='putout' AND unknown_credit_need > 0 AND
    personnel_hard_mask_available AND eligible_for_allocation`` (same
    predicate for both ``putout`` and ``assist``). ``putout_position`` is
    structurally NULL on this slice, so its codes are placeholder zeros and
    are never consumed downstream (it gets marginalized at scoring time).
    Every other FE is encoded against its training ``levels`` vocabulary.
    """
    fe_source_columns = [c for c in fixed_effects if c != PUTOUT_POSITION_FE_COLUMN]
    per_event = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col("credit_type") == "putout")
            & (pl.col("unknown_credit_need") > 0)
            & (pl.col("personnel_hard_mask_available") == True)  # noqa: E712
            & (pl.col("eligible_for_allocation") == True)  # noqa: E712
        )
        .select(["event_key", *fe_source_columns])
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
    n = int(event_keys.shape[0])

    scoring_fe: dict[str, FixedEffectDesign] = {}
    for column, design in fixed_effects.items():
        if column == PUTOUT_POSITION_FE_COLUMN:
            codes = np.zeros(n, dtype=np.int64)
        else:
            codes = _encode_codes_with_vocab(per_event, column, list(design.levels))
        scoring_fe[column] = FixedEffectDesign(levels=design.levels, codes=codes)

    return ProductionScoringFrame(event_keys=event_keys, fixed_effects=scoring_fe)


def prepare_event_credit_inputs(
    parquet_path: Path,
    *,
    dimension: str = "putout",
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
    sigma_box_aggregate: float = 0.5,
    min_events_per_season: int = MIN_EVENTS_PER_SEASON,
    held_out_fold_id: int = HOLDOUT_FOLD_ID,
    held_out_fold_count: int = HOLDOUT_FOLD_COUNT,
) -> EventCreditInputs:
    """Read the modeling-dataset parquet and shape dual-arm inputs.

    For ``putout`` (v1.5): filter to well-attributed events
    (``known_credit > 0`` AND ``personnel_hard_mask_available``);
    truth = per-event known_credit grid.

    For ``assist`` (v3): filter to ``personnel_hard_mask_available`` AND
    events with a single putout (so ``putout_position`` is unique).
    Drop events with assist count > 1 — v3 cut 1 is single-assist
    only. Truth = K=10 one-hot with NONE sentinel for zero-assist
    events. Adds ``putout_position`` (1..9) as a FE.
    """
    _assert_fixed_effects_cover_production_slice(parquet_path, dimension=dimension)

    df = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col("credit_type") == dimension)
            & (pl.col("personnel_hard_mask_available") == True)  # noqa: E712
        )
        .collect()
    )
    if df.height == 0:
        raise ValueError(
            f"no rows for credit_type={dimension!r} in {parquet_path}"
        )

    putout_position_lookup_cache: pl.DataFrame | None = None

    if dimension == "putout":
        eligible_event_keys = (
            df.group_by("event_key")
            .agg(pl.col("known_credit").sum().alias("_sum_known"))
            .filter(pl.col("_sum_known") > 0)
            .get_column("event_key")
        )
        df = df.filter(pl.col("event_key").is_in(eligible_event_keys.implode()))
        if df.height == 0:
            raise ValueError(
                f"no events with known_credit>0 for credit_type={dimension!r}"
            )
    elif dimension == "assist":
        putout_position_lookup_cache = _resolve_putout_position_per_event(
            parquet_path
        )
        if putout_position_lookup_cache.height == 0:
            raise ValueError(
                "no events with a single resolved putout_position; cannot fit assist v3"
            )
        df = df.join(putout_position_lookup_cache, on="event_key", how="inner")
        a_count_per_event = (
            df.group_by("event_key")
            .agg(pl.col("known_credit").sum().alias("_a_count"))
        )
        single_or_none_keys = (
            a_count_per_event.filter(pl.col("_a_count") <= 1)
            .get_column("event_key")
        )
        before = df.get_column("event_key").n_unique()
        df = df.filter(pl.col("event_key").is_in(single_or_none_keys.implode()))
        after = df.get_column("event_key").n_unique()
        _log.info(
            "prepare_event_credit_inputs assist: dropped %d multi-assist events "
            "(kept %d single-or-zero-assist)",
            before - after,
            after,
        )
        if df.height == 0:
            raise ValueError(
                "no single-or-zero-assist events after multi-assist filter"
            )
    else:
        raise ValueError(
            f"unsupported dimension={dimension!r}; only 'putout' and 'assist' are "
            "wired for the dual-arm credit model"
        )

    natural_rate_threshold_env = os.environ.get(NATURAL_UNK_RATE_FILTER_ENV)
    if natural_rate_threshold_env is not None:
        threshold = float(natural_rate_threshold_env)
        rate_lookup = _natural_unknown_rate_lookup()
        all_seasons = sorted({int(s) for s in df.get_column("season").unique().to_list()})
        kept_seasons = [
            s
            for s in all_seasons
            if rate_lookup.get(f"{s}|play_by_play", 0.0) >= threshold
        ]
        _log.info(
            "prepare_event_credit_inputs natural-unknown-rate filter %s>=%.4f: keeping %d/%d seasons",
            NATURAL_UNK_RATE_FILTER_ENV,
            threshold,
            len(kept_seasons),
            len(all_seasons),
        )
        df = df.filter(pl.col("season").is_in(kept_seasons))
        if df.height == 0:
            raise ValueError(
                f"no seasons survived natural-unknown-rate filter "
                f"({NATURAL_UNK_RATE_FILTER_ENV}={threshold})"
            )

    season_counts = (
        df.group_by(["season", "event_key"])
        .agg(pl.first("known_credit"))
        .group_by("season")
        .agg(pl.len().alias("_n_events"))
    )
    informative_seasons = season_counts.filter(
        pl.col("_n_events") >= min_events_per_season
    ).get_column("season")
    dropped = season_counts.height - informative_seasons.len()
    if dropped:
        dropped_seasons = (
            season_counts.filter(pl.col("_n_events") < min_events_per_season)
            .sort("season")
            .get_column("season")
            .to_list()
        )
        _log.info(
            "prepare_event_credit_inputs dropped %d seasons with <%d eligible events: %s",
            dropped,
            min_events_per_season,
            dropped_seasons,
        )
        df = df.filter(pl.col("season").is_in(informative_seasons.implode()))
        if df.height == 0:
            raise ValueError(
                f"every season fell below min_events_per_season={min_events_per_season} "
                f"for credit_type={dimension!r}; nothing to fit"
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
        "prepare_event_credit_inputs held-out %d/%d games via fold %d/%d",
        len(holdout_game_ids),
        len(distinct_game_ids),
        held_out_fold_id,
        held_out_fold_count,
    )
    if df_train.height == 0:
        raise ValueError(
            "every game landed in the held-out fold; nothing to fit"
        )

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
            kept_games = shuffled.filter(pl.col("cum_events") <= smoke_limit).get_column(
                "game_id"
            )
            if kept_games.len() == 0:
                kept_games = shuffled.head(1).get_column("game_id")
            df_train = df_train.filter(pl.col("game_id").is_in(kept_games.implode()))
            kept_event_count = (
                df_train.select("event_key").unique().get_column("event_key").len()
            )
            _log.info(
                "prepare_event_credit_inputs game-subsampled to %d games (%d events; "
                "budget=%d, seed=%d)",
                kept_games.len(),
                kept_event_count,
                smoke_limit,
                seed,
            )

    per_event = _collapse_event_grain(df_train)

    n_positions = n_positions_for(dimension)
    position_labels = position_labels_for(dimension)

    season_idx, season_labels = _category_index(per_event, "season")
    scorer_idx, scorer_labels = _category_index(per_event, "scorer")
    park_idx, park_labels = _category_index(per_event, "park_id")
    source_idx, source_labels = _category_index(per_event, "source_family")

    if dimension == "putout":
        U_array = np.asarray(
            [int(round(sum(grid))) for grid in per_event.get_column("known_credit_grid").to_list()],
            dtype=np.int64,
        )
        if (U_array <= 0).any():
            bad = int((U_array <= 0).sum())
            raise AssertionError(
                f"{bad} events have U_e <= 0 after the known_credit filter; upstream view inconsistent"
            )
        a_count = U_array
        a_pos_0based = _per_event_true_position(
            per_event.get_column("known_credit_grid").to_list()
        )
        counts_per_event = _counts_grid_from_known(
            per_event.get_column("known_credit_grid").to_list()
        )
        per_position_weights: tuple[float, ...] = REAL_UNKNOWN_RATES_BY_POSITION
    else:
        a_count, a_pos_0based = _assist_truth_per_event(per_event)
        U_array = np.ones_like(a_count, dtype=np.int64)
        counts_per_event = _counts_grid_for_assists(a_count, a_pos_0based)
        per_position_weights = tuple(1.0 for _ in range(n_positions))

    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )

    fe_columns = list(FIXED_EFFECT_COLUMNS)
    if dimension == "assist" and PUTOUT_POSITION_FE_COLUMN not in fe_columns:
        fe_columns.append(PUTOUT_POSITION_FE_COLUMN)
    fixed_effects: dict[str, FixedEffectDesign] = {}
    per_event_cols = set(per_event.columns)
    for column in fe_columns:
        if column not in per_event_cols:
            _log.info(
                "prepare_event_credit_inputs skipping FE column %r — not present",
                column,
            )
            continue
        fixed_effects[column] = _build_fixed_effect_design(per_event, column)

    if dimension == "assist" and PUTOUT_POSITION_FE_COLUMN in fixed_effects:
        po_design = fixed_effects[PUTOUT_POSITION_FE_COLUMN]
        po_codes_as_position = np.asarray(
            [int(po_design.levels[c]) for c in po_design.codes.tolist()],
            dtype=np.int64,
        )
        _assert_putout_position_levels_in_training(po_codes_as_position)

    global_effects: dict[str, FixedEffectDesign] = {}
    for column in GLOBAL_EFFECT_COLUMNS:
        if column not in per_event_cols:
            continue
        global_effects[column] = _build_fixed_effect_design(per_event, column)

    coords: dict[str, list[str]] = {
        "position": list(position_labels),
        "season": list(season_labels),
        "scorer": list(scorer_labels),
        "park": list(park_labels),
        "source": list(source_labels),
    }
    for name, design in fixed_effects.items():
        coords[f"{name}_levels"] = list(design.levels)
    for name, design in global_effects.items():
        coords[f"{name}_levels"] = list(design.levels)

    is_masked = _apply_synthetic_mask(
        per_event,
        true_pos_0based=a_pos_0based,
        seed=seed,
        per_position_weights=per_position_weights,
    )
    _log.info(
        "prepare_event_credit_inputs synthetic mask: %d/%d events masked (%.3f)",
        int(is_masked.sum()),
        is_masked.shape[0],
        float(is_masked.mean()),
    )

    sup_event_idx, sup_counts, sup_U = _build_supervised_arrays(
        counts_per_event, is_masked=is_masked, U=U_array
    )

    (
        aggregate_targets,
        aggregate_sigma,
        aggregate_event_idx,
        aggregate_position_idx,
        aggregate_row_idx,
    ) = _build_aggregate_targets_from_mask(
        per_event,
        is_masked=is_masked,
        sigma_box_aggregate=sigma_box_aggregate,
    )

    held_out = _build_held_out_set(
        held_out_df,
        dimension=dimension,
        season_labels=season_labels,
        scorer_labels=scorer_labels,
        park_labels=park_labels,
        source_labels=source_labels,
        fixed_effects=fixed_effects,
        putout_position_lookup=putout_position_lookup_cache,
    )

    _log.info(
        "prepare_event_credit_inputs credit_type=%s K=%d train_events=%d supervised=%d "
        "masked=%d targets=%d held_out_events=%d seasons=%d scorers=%d parks=%d sources=%d",
        dimension,
        n_positions,
        per_event.height,
        int(sup_event_idx.shape[0]),
        int(is_masked.sum()),
        int(aggregate_targets.shape[0]),
        held_out.n_events,
        len(season_labels),
        len(scorer_labels),
        len(park_labels),
        len(source_labels),
    )

    return EventCreditInputs(
        U=U_array,
        event_keys=event_keys,
        credit_type=dimension,
        n_positions=n_positions,
        season_idx=season_idx,
        scorer_idx=scorer_idx,
        park_idx=park_idx,
        source_idx=source_idx,
        fixed_effects=fixed_effects,
        global_effects=global_effects,
        coords=coords,
        is_masked=is_masked,
        Y_supervised_event_idx=sup_event_idx,
        Y_supervised_counts=sup_counts,
        Y_supervised_U=sup_U,
        aggregate_targets=aggregate_targets,
        aggregate_sigma=aggregate_sigma,
        aggregate_event_idx=aggregate_event_idx,
        aggregate_position_idx=aggregate_position_idx,
        aggregate_row_idx=aggregate_row_idx,
        held_out=held_out,
    )


def _build_held_out_set(
    df: pl.DataFrame,
    *,
    dimension: str,
    season_labels: list[str],
    scorer_labels: list[str],
    park_labels: list[str],
    source_labels: list[str],
    fixed_effects: dict[str, FixedEffectDesign],
    putout_position_lookup: pl.DataFrame | None = None,
) -> HeldOutSet:
    if df.height == 0:
        empty = np.zeros(0, dtype=np.int64)
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

    if dimension == "assist" and putout_position_lookup is not None:
        df = df.join(putout_position_lookup, on="event_key", how="inner")
        if df.height == 0:
            empty = np.zeros(0, dtype=np.int64)
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
        a_count_per = (
            df.group_by("event_key")
            .agg(pl.col("known_credit").sum().alias("_a_count"))
        )
        keep_keys = (
            a_count_per.filter(pl.col("_a_count") <= 1).get_column("event_key")
        )
        df = df.filter(pl.col("event_key").is_in(keep_keys.implode()))

    per_event = _collapse_event_grain(df)
    grids = per_event.get_column("known_credit_grid").to_list()
    if dimension == "putout":
        u_counts = np.asarray([int(round(sum(g))) for g in grids], dtype=np.int64)
        keep_mask = u_counts > 0
        if not keep_mask.all():
            per_event = per_event.filter(pl.Series("_keep", keep_mask.tolist()))
            u_counts = u_counts[keep_mask]
            grids = per_event.get_column("known_credit_grid").to_list()
        true_pos_0based = _per_event_true_position(grids)
    else:
        a_count_held, true_pos_0based = _assist_truth_per_event(per_event)
        u_counts = np.ones_like(a_count_held, dtype=np.int64)
    if per_event.height == 0:
        empty = np.zeros(0, dtype=np.int64)
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

    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )

    def _resolve(col: str, labels: list[str]) -> IntArray:
        return _encode_codes_with_vocab(per_event, col, labels)

    season_idx = _resolve("season", season_labels)
    scorer_idx = _resolve("scorer", scorer_labels)
    park_idx = _resolve("park_id", park_labels)
    source_idx = _resolve("source_family", source_labels)

    held_fe: dict[str, FixedEffectDesign] = {}
    for column, design in fixed_effects.items():
        codes = _resolve(column, list(design.levels))
        held_fe[column] = FixedEffectDesign(levels=design.levels, codes=codes)

    return HeldOutSet(
        event_keys=event_keys,
        true_position=true_pos_0based,
        U=u_counts,
        season_idx=season_idx,
        scorer_idx=scorer_idx,
        park_idx=park_idx,
        source_idx=source_idx,
        fixed_effects=held_fe,
    )
