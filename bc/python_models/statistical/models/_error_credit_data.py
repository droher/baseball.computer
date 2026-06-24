"""Event-grain prep for the error-credit allocation submodel.

Errors are scorer-discretion outcomes: the predictor weights the scorer
heavily and downweights base/out state relative to putouts. This prep
reads ``model_input_fielding_credit`` filtered to
``credit_type='error', known_credit > 0, personnel_hard_mask_available``,
collapses each event to a length-9 error-credit grid, and emits a
supervised per-event Multinomial over the eligible fielder positions:
``E_{e,1:9} ~ Multinomial(U^E_e, pi^E_e)`` where ``U^E_e`` is the known
event error count.

A deterministic per-game held-out split (``HOLDOUT_FOLD_COUNT=10`` via
``game_hash_fold``, fold 0) is removed before training; held-out events
ship alongside for OOS scoring. Reuses ``FixedEffectDesign`` /
``HeldOutSet`` / the category-index + collapse helpers from
``_credit_data`` so the posterior reconstruction + held-out eval
helpers accept ``ErrorCreditInputs`` unchanged.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging
from pathlib import Path
from typing import ClassVar

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict

from python_models.statistical.models._credit_data import (
    FixedEffectDesign,
    HeldOutSet,
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    MIN_EVENTS_PER_SEASON,
    N_POSITIONS,
    POSITION_LABELS,
    _build_fixed_effect_design,
    _category_index,
    _collapse_event_grain,
    _counts_grid_from_known,
    _encode_codes_with_vocab,
    _per_event_true_position,
)
from python_models.statistical.splits import game_hash_fold

_log = logging.getLogger(__name__)

IntArray = npt.NDArray[np.int64]

DEFAULT_SEED: int = 20260513

ERROR_FIXED_EFFECT_COLUMNS: tuple[str, ...] = (
    "result_family",
    "base_state_start",
    "alignment_regime",
)


class ErrorCreditInputs(BaseModel):
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

    coords: dict[str, list[str]]

    Y_supervised_event_idx: IntArray
    Y_supervised_counts: IntArray
    Y_supervised_U: IntArray

    held_out: HeldOutSet

    @property
    def n_events(self) -> int:
        return int(self.U.shape[0])

    @property
    def n_supervised_events(self) -> int:
        return int(self.Y_supervised_event_idx.shape[0])


def prepare_error_credit_inputs(
    parquet_path: Path,
    *,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
    min_events_per_season: int = MIN_EVENTS_PER_SEASON,
    held_out_fold_id: int = HOLDOUT_FOLD_ID,
    held_out_fold_count: int = HOLDOUT_FOLD_COUNT,
) -> ErrorCreditInputs:
    """Shape a supervised per-event error-allocation Multinomial input."""
    df = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col("credit_type") == "error")
            & (pl.col("personnel_hard_mask_available") == True)  # noqa: E712
        )
        .collect()
    )
    if df.height == 0:
        raise ValueError(f"no error rows on the hard-mask slice in {parquet_path}")

    eligible = (
        df.group_by("event_key")
        .agg(pl.col("known_credit").sum().alias("_sum"))
        .filter(pl.col("_sum") > 0)
        .get_column("event_key")
    )
    df = df.filter(pl.col("event_key").is_in(eligible.implode()))
    if df.height == 0:
        raise ValueError("no events with error known_credit>0")

    season_counts = (
        df.group_by(["season", "event_key"])
        .agg(pl.first("known_credit"))
        .group_by("season")
        .agg(pl.len().alias("_n"))
    )
    informative = season_counts.filter(
        pl.col("_n") >= min_events_per_season
    ).get_column("season")
    if season_counts.height - informative.len():
        df = df.filter(pl.col("season").is_in(informative.implode()))
        if df.height == 0:
            raise ValueError(
                f"every season fell below min_events_per_season={min_events_per_season}"
            )

    distinct_game_ids = df.get_column("game_id").unique().to_list()
    holdout_game_ids = [
        g
        for g in distinct_game_ids
        if game_hash_fold(g, fold_count=held_out_fold_count) == held_out_fold_id
    ]
    held_out_df = df.filter(pl.col("game_id").is_in(holdout_game_ids))
    df_train = df.filter(~pl.col("game_id").is_in(holdout_game_ids))
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

    per_event = _collapse_event_grain(df_train)

    season_idx, season_labels = _category_index(per_event, "season")
    scorer_idx, scorer_labels = _category_index(per_event, "scorer")
    park_idx, park_labels = _category_index(per_event, "park_id")
    source_idx, source_labels = _category_index(per_event, "source_family")

    grids = per_event.get_column("known_credit_grid").to_list()
    u_array = np.asarray([int(round(sum(g))) for g in grids], dtype=np.int64)
    if (u_array <= 0).any():
        bad = int((u_array <= 0).sum())
        raise AssertionError(f"{bad} error events have U_e <= 0 after the filter")
    counts_per_event = _counts_grid_from_known(grids)

    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )

    fixed_effects: dict[str, FixedEffectDesign] = {}
    per_event_cols = set(per_event.columns)
    for column in ERROR_FIXED_EFFECT_COLUMNS:
        if column not in per_event_cols:
            continue
        fixed_effects[column] = _build_fixed_effect_design(per_event, column)

    coords: dict[str, list[str]] = {
        "position": list(POSITION_LABELS),
        "season": list(season_labels),
        "scorer": list(scorer_labels),
        "park": list(park_labels),
        "source": list(source_labels),
    }
    for name, design in fixed_effects.items():
        coords[f"{name}_levels"] = list(design.levels)

    sup_event_idx = np.arange(per_event.height, dtype=np.int64)
    if int(counts_per_event.sum()) != int(u_array.sum()):
        raise AssertionError(
            "error supervised counts do not sum to U: "
            f"{int(counts_per_event.sum())} != {int(u_array.sum())}"
        )

    held_out = _build_error_held_out_set(
        held_out_df,
        season_labels=season_labels,
        scorer_labels=scorer_labels,
        park_labels=park_labels,
        source_labels=source_labels,
        fixed_effects=fixed_effects,
    )

    _log.info(
        "prepare_error_credit_inputs events=%d supervised=%d held_out=%d "
        "seasons=%d scorers=%d parks=%d sources=%d",
        per_event.height,
        int(sup_event_idx.shape[0]),
        held_out.n_events,
        len(season_labels),
        len(scorer_labels),
        len(park_labels),
        len(source_labels),
    )

    return ErrorCreditInputs(
        U=u_array,
        event_keys=event_keys,
        credit_type="error",
        n_positions=N_POSITIONS,
        season_idx=season_idx,
        scorer_idx=scorer_idx,
        park_idx=park_idx,
        source_idx=source_idx,
        fixed_effects=fixed_effects,
        coords=coords,
        Y_supervised_event_idx=sup_event_idx,
        Y_supervised_counts=counts_per_event,
        Y_supervised_U=u_array,
        held_out=held_out,
    )


def _build_error_held_out_set(
    df: pl.DataFrame,
    *,
    season_labels: list[str],
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

    per_event = _collapse_event_grain(df)
    grids = per_event.get_column("known_credit_grid").to_list()
    u_counts = np.asarray([int(round(sum(g))) for g in grids], dtype=np.int64)
    keep_mask = u_counts > 0
    if not keep_mask.all():
        per_event = per_event.filter(pl.Series("_keep", keep_mask.tolist()))
        u_counts = u_counts[keep_mask]
        grids = per_event.get_column("known_credit_grid").to_list()
    if per_event.height == 0:
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

    true_pos = _per_event_true_position(grids)
    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )

    def _resolve(col: str, labels: list[str]) -> IntArray:
        return _encode_codes_with_vocab(per_event, col, labels)

    held_fe: dict[str, FixedEffectDesign] = {}
    for column, design in fixed_effects.items():
        held_fe[column] = FixedEffectDesign(
            levels=design.levels, codes=_resolve(column, list(design.levels))
        )

    return HeldOutSet(
        event_keys=event_keys,
        true_position=true_pos,
        U=u_counts,
        season_idx=_resolve("season", season_labels),
        scorer_idx=_resolve("scorer", scorer_labels),
        park_idx=_resolve("park_id", park_labels),
        source_idx=_resolve("source_family", source_labels),
        fixed_effects=held_fe,
    )
