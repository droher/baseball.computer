"""Event-grain prep for the runner-advancement model (Model H).

Reads the frozen ``model_input_advancement`` Parquet at grain
``(event_key, baserunner)`` and shapes a per-row categorical softmax over the
recorded 7-class advancement outcome. The class set is the fixed advancement
vocabulary (relative to the runner's start base); the fixed effects are the
pre-advancement game state plus the recorded batted-ball geometry. The output
is a ``GeometryInputs`` so the generic softmax builder, posterior
reconstruction, and held-out eval accept it unchanged.

Filters to rows carrying a recorded ``advancement_class``
(``training_weight > 0``), drops season-league cells below the floor, holds
out 10% of games for OOS scoring, and emits one-hot per-row truth plus
season-league / scorer / FE designs. There is no DL covariate
(``dl_active`` False); the production scoring slice is every row with a
recorded advancement class.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import numpy.typing as npt
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

IntArray = npt.NDArray[np.int64]
FloatArray = npt.NDArray[np.float64]

DEFAULT_SEED: int = 20260513
MIN_EVENTS_PER_SEASON: int = 50

HOLDOUT_FOLD_COUNT: int = 10
HOLDOUT_FOLD_ID: int = 0

DIMENSION: str = "advancement"
LABEL_COLUMN: str = "advancement_class"
UNKNOWN_LEVEL: str = "__unknown__"

ADVANCEMENT_CLASS_LABELS: tuple[str, ...] = (
    "Stayed",
    "Advanced1",
    "Advanced2",
    "Scored",
    "OutAdvancing",
    "OutCaughtStealing",
    "OutPickoff",
)

FIXED_EFFECT_COLUMNS: tuple[str, ...] = (
    "base_start",
    "outs_start",
    "base_state_start",
    "leverage_bucket",
    "result_family",
    "alignment_regime",
    "trajectory_class",
    "location_depth_class",
    "ball_handler_position_class",
)


def _scan_recorded(parquet_path: Path) -> pl.LazyFrame:
    return (
        pl.scan_parquet(parquet_path)
        .filter(pl.col(LABEL_COLUMN).is_not_null() & (pl.col("training_weight") > 0.0))
        .with_columns(
            pl.col(LABEL_COLUMN).cast(pl.Utf8).alias("_label"),
            (
                pl.col("season").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("league").cast(pl.Utf8).fill_null(UNKNOWN_LEVEL)
            ).alias("season_league"),
        )
    )


def prepare_advancement_inputs(
    parquet_path: Path,
    *,
    dimension: str | None = None,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
    min_events_per_season: int = MIN_EVENTS_PER_SEASON,
    held_out_fold_id: int = HOLDOUT_FOLD_ID,
    held_out_fold_count: int = HOLDOUT_FOLD_COUNT,
) -> GeometryInputs:
    """Read the modeling-dataset Parquet and shape per-row advancement inputs.

    The ``dimension`` kwarg is accepted for interface parity and ignored
    (this dataset has no dimension column). The class vocabulary is the
    locked 7-class advancement ordering; rows whose label falls outside it
    are dropped with a warning. Season-league cells below
    ``min_events_per_season`` are pruned; 10% of games are held out for OOS.
    """
    _ = dimension

    df = _scan_recorded(parquet_path).collect()
    if df.height == 0:
        raise ValueError(f"no recorded advancement rows in {parquet_path}")

    vocab = list(ADVANCEMENT_CLASS_LABELS)
    in_vocab = df.get_column("_label").is_in(vocab)
    dropped_labels = int((~in_vocab).sum())
    if dropped_labels:
        offending = df.filter(~in_vocab).get_column("_label").unique().sort().to_list()
        _log.warning(
            "prepare_advancement_inputs dropped %d rows whose advancement_class is "
            "not in the locked vocabulary: %s",
            dropped_labels,
            offending,
        )
        df = df.filter(in_vocab)
        if df.height == 0:
            raise ValueError("every row fell outside the advancement vocabulary")

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
            "prepare_advancement_inputs dropped %d season-league cells with <%d rows",
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
        "prepare_advancement_inputs held-out %d/%d games via fold %d/%d",
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
                "prepare_advancement_inputs game-subsampled to %d games (%d rows; "
                "budget=%d, seed=%d)",
                kept_games.len(),
                df_train.height,
                smoke_limit,
                seed,
            )

    per_event = df_train.sort(["event_key", "base_start"])
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
                "prepare_advancement_inputs skipping FE column %r — not present",
                column,
            )
            continue
        fixed_effects[column] = _build_fixed_effect_design(per_event, column)

    coords: dict[str, list[str]] = {
        "position": list(vocab),
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
        "prepare_advancement_inputs K=%d train_rows=%d held_out_rows=%d "
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


def build_advancement_production_frame(
    parquet_path: Path,
    *,
    fixed_effects: dict[str, FixedEffectDesign],
    n_classes: int,
) -> tuple[GeometryProductionFrame, list[str]]:
    """One row per recorded ``(event_key, baserunner)`` with FE codes.

    The scoring slice is every row carrying a recorded ``advancement_class``.
    Each FE is encoded against its training ``levels`` vocabulary; unseen
    levels encode to ``-1`` and drop out of the softmax reconstruction. The
    returned ``baserunner`` labels run parallel to ``frame.event_keys`` so the
    export can write the ``(event_key, baserunner)`` grain. The DL logit array
    is all-zeros (no advancement DL proposal is consumed).
    """
    per_event = (
        _scan_recorded(parquet_path)
        .filter(pl.col("_label").is_in(list(ADVANCEMENT_CLASS_LABELS)))
        .select(["event_key", "baserunner", *FIXED_EFFECT_COLUMNS])
        .sort(["event_key", "baserunner"])
        .collect()
    )
    if per_event.height == 0:
        return (
            GeometryProductionFrame(
                event_keys=np.zeros(0, dtype=np.int64),
                fixed_effects={},
                dl_logit_per_class=np.zeros((0, n_classes), dtype=np.float64),
            ),
            [],
        )

    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )
    baserunner_labels = [str(b) for b in per_event.get_column("baserunner").to_list()]
    scoring_fe: dict[str, FixedEffectDesign] = {
        column: FixedEffectDesign(
            levels=design.levels,
            codes=_encode_codes_with_vocab(per_event, column, list(design.levels)),
        )
        for column, design in fixed_effects.items()
    }
    frame = GeometryProductionFrame(
        event_keys=event_keys,
        fixed_effects=scoring_fe,
        dl_logit_per_class=np.zeros((per_event.height, n_classes), dtype=np.float64),
    )
    return frame, baserunner_labels
