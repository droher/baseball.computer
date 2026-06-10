"""Event-grain prep for the fielding-responsibility model (Model I).

Reads the frozen ``model_input_responsibility`` Parquet at grain ``(event_key)``
and shapes a per-event categorical softmax over the range fielder position
(3..9) that handled the batted ball, used as the responsibility / opportunity
proxy. Pitcher and catcher are already excluded by the view; bunts are dropped
here. The covariates are the recorded batted-ball geometry classes plus the
pre-event game state and the era-normal alignment basis. The output is a
``GeometryInputs`` so the generic softmax builder, posterior reconstruction, and
held-out eval accept it unchanged.

Filters to rows with ``training_weight > 0``, drops bunts and season-league
cells below the floor, holds out 10% of games for OOS scoring, and emits
one-hot per-event truth plus season-league / scorer / FE designs. There is no
DL covariate (``dl_active`` False); the production scoring slice is every clean
range play in the view.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import polars as pl

from python_models.statistical.models._credit_data import FixedEffectDesign
from python_models.statistical.models._geometry_data import (
    GeometryInputs,
    GeometryProductionFrame,
    _build_fixed_effect_design,
    _build_geometry_held_out_set,
    _category_index,
    _encode_codes_with_vocab,
    _one_hot_counts,
)
from python_models.statistical.splits import game_hash_fold

_log = logging.getLogger(__name__)

DEFAULT_SEED: int = 20260513
MIN_EVENTS_PER_SEASON: int = 50

HOLDOUT_FOLD_COUNT: int = 10
HOLDOUT_FOLD_ID: int = 0

DIMENSION: str = "responsibility"
LABEL_COLUMN: str = "ball_handler_position"
UNKNOWN_LEVEL: str = "__unknown__"

MIN_POSITION: int = 3
MAX_POSITION: int = 9
RESPONSIBILITY_POSITION_LABELS: tuple[str, ...] = tuple(
    str(p) for p in range(MIN_POSITION, MAX_POSITION + 1)
)

BUNT_TRAJECTORY_CLASSES: tuple[str, ...] = (
    "FoulBunt",
    "GroundBallBunt",
    "LineDriveBunt",
    "PopUpBunt",
    "UnspecifiedBunt",
)

FIXED_EFFECT_COLUMNS: tuple[str, ...] = (
    "trajectory_class",
    "location_side_class",
    "location_depth_class",
    "location_edge_class",
    "base_state_start",
    "outs_start",
    "result_family",
    "alignment_regime",
    "batter_hand",
)


def _scan_clean_range(parquet_path: Path) -> pl.LazyFrame:
    return (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col("training_weight") > 0.0)
            & pl.col(LABEL_COLUMN).is_not_null()
            & ~pl.col("trajectory_class").is_in(list(BUNT_TRAJECTORY_CLASSES))
        )
        .with_columns(
            pl.col(LABEL_COLUMN).cast(pl.Int64, strict=False).alias("_position"),
            (
                pl.col("season").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("league").cast(pl.Utf8).fill_null(UNKNOWN_LEVEL)
            ).alias("season_league"),
        )
        .filter(pl.col("_position").is_between(MIN_POSITION, MAX_POSITION))
        .with_columns(pl.col("_position").cast(pl.Utf8).alias("_label"))
    )


def prepare_responsibility_inputs(
    parquet_path: Path,
    *,
    dimension: str | None = None,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
    min_events_per_season: int = MIN_EVENTS_PER_SEASON,
    held_out_fold_id: int = HOLDOUT_FOLD_ID,
    held_out_fold_count: int = HOLDOUT_FOLD_COUNT,
) -> GeometryInputs:
    """Read the modeling-dataset Parquet and shape per-event responsibility inputs.

    The ``dimension`` kwarg is accepted for interface parity and ignored. The
    class vocabulary is the fixed range-position set 3..9 (pitcher / catcher
    excluded upstream). Bunts and season-league cells below
    ``min_events_per_season`` are dropped; 10% of games are held out for OOS.
    """
    _ = dimension

    df = _scan_clean_range(parquet_path).collect()
    if df.height == 0:
        raise ValueError(f"no clean range-play rows in {parquet_path}")

    vocab = list(RESPONSIBILITY_POSITION_LABELS)
    class_to_idx = {label: i for i, label in enumerate(vocab)}
    df = df.with_columns(
        pl.col("_label")
        .replace_strict(class_to_idx, return_dtype=pl.Int64)
        .alias("_class_idx")
    )

    cell_counts = df.group_by("season_league").agg(pl.len().alias("_n"))
    informative_cells = cell_counts.filter(
        pl.col("_n") >= min_events_per_season
    ).get_column("season_league")
    dropped = cell_counts.height - informative_cells.len()
    if dropped:
        _log.info(
            "prepare_responsibility_inputs dropped %d season-league cells with <%d rows",
            dropped,
            min_events_per_season,
        )
        df = df.filter(pl.col("season_league").is_in(informative_cells.implode()))
        if df.height == 0:
            raise ValueError(
                f"every season-league cell fell below min_events_per_season="
                f"{min_events_per_season}; nothing to fit"
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
        "prepare_responsibility_inputs held-out %d/%d games via fold %d/%d",
        len(holdout_game_ids),
        len(distinct_game_ids),
        held_out_fold_id,
        held_out_fold_count,
    )
    if df_train.height == 0:
        raise ValueError("every game landed in the held-out fold; nothing to fit")

    if smoke_limit is not None:
        rows_per_game = (
            df_train.group_by("game_id").agg(pl.len().alias("n_rows")).sort("game_id")
        )
        total_rows = int(rows_per_game.get_column("n_rows").sum())
        if total_rows > smoke_limit:
            shuffled = rows_per_game.sample(
                fraction=1.0, seed=seed, shuffle=True
            ).with_columns(pl.col("n_rows").cum_sum().alias("cum_rows"))
            kept_games = shuffled.filter(pl.col("cum_rows") <= smoke_limit).get_column(
                "game_id"
            )
            if kept_games.len() == 0:
                kept_games = shuffled.head(1).get_column("game_id")
            df_train = df_train.filter(pl.col("game_id").is_in(kept_games.implode()))
            _log.info(
                "prepare_responsibility_inputs game-subsampled to %d games (%d rows; "
                "budget=%d, seed=%d)",
                kept_games.len(),
                df_train.height,
                smoke_limit,
                seed,
            )

    per_event = df_train.sort("event_key")
    n_classes = len(vocab)

    class_idx = per_event.get_column("_class_idx").to_numpy().astype(np.int64)
    counts = _one_hot_counts(class_idx, n_classes)
    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )
    dl_logit_per_class = np.zeros((per_event.height, n_classes), dtype=np.float64)
    dl_logit_class_means = np.zeros(n_classes, dtype=np.float64)

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
                "prepare_responsibility_inputs skipping FE column %r — not present",
                column,
            )
            continue
        fixed_effects[column] = _build_fixed_effect_design(per_event, column)

    coords: dict[str, list[str]] = {
        "season_league": list(season_league_labels),
        "scorer": list(scorer_labels),
        "source": list(source_labels),
        "park": list(park_labels),
    }
    for name, design in fixed_effects.items():
        coords[f"{name}_levels"] = list(design.levels)

    held_out = _build_geometry_held_out_set(
        held_out_df,
        vocab=vocab,
        season_league_labels=season_league_labels,
        scorer_labels=scorer_labels,
        park_labels=park_labels,
        source_labels=source_labels,
        fixed_effects=fixed_effects,
    )
    held_out_dl_logit_per_class = np.zeros(
        (held_out.n_events, n_classes), dtype=np.float64
    )

    _log.info(
        "prepare_responsibility_inputs K=%d train_rows=%d held_out_rows=%d "
        "season_leagues=%d scorers=%d parks=%d sources=%d fixed_effects=%d",
        n_classes,
        per_event.height,
        held_out.n_events,
        len(season_league_labels),
        len(scorer_labels),
        len(park_labels),
        len(source_labels),
        len(fixed_effects),
    )

    return GeometryInputs(
        event_keys=event_keys,
        dimension=DIMENSION,
        class_labels=list(vocab),
        n_classes=n_classes,
        dl_active=False,
        counts=counts,
        dl_logit_per_class=dl_logit_per_class,
        dl_logit_class_means=dl_logit_class_means,
        season_league_idx=season_league_idx,
        scorer_idx=scorer_idx,
        fixed_effects=fixed_effects,
        coords=coords,
        held_out=held_out,
        held_out_dl_logit_per_class=held_out_dl_logit_per_class,
    )


def build_responsibility_production_frame(
    parquet_path: Path,
    *,
    fixed_effects: dict[str, FixedEffectDesign],
    n_classes: int,
) -> GeometryProductionFrame:
    """One row per clean range play with FE codes in the training vocab.

    The scoring slice is every clean range play in the view (handler 3..9,
    non-bunt). Each FE is encoded against its training ``levels`` vocabulary;
    unseen levels encode to ``-1`` and drop out of the softmax reconstruction.
    The DL logit array is all-zeros (no responsibility DL proposal is consumed).
    """
    per_event = (
        _scan_clean_range(parquet_path)
        .select(["event_key", *FIXED_EFFECT_COLUMNS])
        .unique(subset=["event_key"])
        .sort("event_key")
        .collect()
    )
    if per_event.height == 0:
        return GeometryProductionFrame(
            event_keys=np.zeros(0, dtype=np.int64),
            fixed_effects={},
            dl_logit_per_class=np.zeros((0, n_classes), dtype=np.float64),
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
    return GeometryProductionFrame(
        event_keys=event_keys,
        fixed_effects=scoring_fe,
        dl_logit_per_class=np.zeros((per_event.height, n_classes), dtype=np.float64),
    )
