"""Geometry deep proposals: one DeepTargetSpec per atomic dimension.

PR3 ships 5 dimensions (trajectory, location_side, location_depth,
location_edge, region). ``general_location`` (composite of side+depth)
and ``ball_handler_position`` (Model D handler) are deferred.

Each spec filters the modeling-dataset Parquet to
``geometry_dimension = '<dim>' AND is_observed_class`` so the
fold runner sees one row per (event_key, dim) with the observed class as
the target. The fitted artifact's published-pointer name is
``dl_proposal_<dim>``; the dimension column in
``main_models.dl_proposal_manifest`` carries the same raw value so the
JOIN in ``model_input_geometry`` resolves.
"""

from __future__ import annotations

from python_models.ml.features import FeatureLayout
from python_models.statistical.deep.feature_layout import register_coverage_layout
from python_models.statistical.deep.registry import register_target
from python_models.statistical.deep.target_spec import DeepTargetSpec

DATASET_NAME: str = "model_input_geometry"
GAME_ID_COLUMN: str = "game_id"
SPLIT_COLUMN: str = "primary_fold"
WEIGHT_COLUMN: str = "training_weight"
TARGET_COLUMN: str = "class"

HIGH_CARD_COLUMNS: tuple[str, ...] = (
    "batter_id",
    "pitcher_id",
    "park_id",
)

LOW_CARD_COLUMNS: tuple[str, ...] = (
    "league",
    "game_type",
    "frame_start",
    "batter_hand",
    "pitcher_hand",
    "base_state_start",
    "leverage_bucket",
    "alignment_regime",
    "personnel_confidence",
    "context_confidence",
    "result_family",
)

NUMERIC_COLUMNS: tuple[str, ...] = (
    "season",
    "inning_start",
    "outs_start",
    "score_margin",
    "leverage_index",
)

GEOMETRY_LAYOUT: FeatureLayout = FeatureLayout(
    high_card_columns=HIGH_CARD_COLUMNS,
    low_card_columns=LOW_CARD_COLUMNS,
    numeric_columns=NUMERIC_COLUMNS,
    grain_column="event_key",
    split_column=SPLIT_COLUMN,
)

GEOMETRY_DIMENSIONS: tuple[str, ...] = (
    "trajectory",
    "location_side",
    "location_depth",
    "location_edge",
    "region",
)

GEOMETRY_SLICE_COLUMNS: tuple[str, ...] = (
    "season",
    "league",
    "source_family",
)


def _geometry_spec(dimension: str) -> DeepTargetSpec:
    return DeepTargetSpec(
        name=f"geometry_{dimension}",
        dataset_name=DATASET_NAME,
        target_column=TARGET_COLUMN,
        weight_column=WEIGHT_COLUMN,
        kind="multiclass",
        proposal_dimension=dimension,
        calibration_method="temperature",
        fold_count=5,
        slice_columns=GEOMETRY_SLICE_COLUMNS,
        game_id_column=GAME_ID_COLUMN,
        split_column=SPLIT_COLUMN,
        filter_predicate=(
            f"geometry_dimension = '{dimension}' AND is_observed_class"
        ),
    )


GEOMETRY_SPECS: tuple[DeepTargetSpec, ...] = tuple(
    _geometry_spec(d) for d in GEOMETRY_DIMENSIONS
)


def _register() -> None:
    register_coverage_layout(DATASET_NAME, GEOMETRY_LAYOUT)
    for spec in GEOMETRY_SPECS:
        register_target(spec, sibling_manifest="dl_proposal_manifest")


_register()
