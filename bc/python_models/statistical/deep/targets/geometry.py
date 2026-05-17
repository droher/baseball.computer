"""Geometry deep proposals: one DeepTargetSpec per atomic dimension.

Ships 4 dimensions emitted upstream by ``event_observation_geometry``:
trajectory, location_side, location_depth, location_edge.
``general_location`` (composite of side+depth), ``ball_handler_position``
(Model D handler), and ``region`` (broad infield/outfield bucket) are
deferred until upstream emits them.

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
from python_models.statistical.deep.feature_layout import (
    register_coverage_layout,
    validate_pre_event,
)
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
    "scorer",
)

LOW_CARD_COLUMNS: tuple[str, ...] = (
    "league",
    "game_type",
    "frame_start",
    "batter_hand",
    "pitcher_hand",
    "base_state_start",
    "leverage_bucket",
    "count_balls",
    "count_strikes",
    "alignment_regime",
    "personnel_confidence",
    "context_confidence",
)

NUMERIC_COLUMNS: tuple[str, ...] = (
    "season",
    "inning_start",
    "outs_start",
    "score_margin",
    "runners_count_start",
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
)

GEOMETRY_SLICE_COLUMNS: tuple[str, ...] = (
    "season",
    "league",
    "source_family",
)

TRAJECTORY_CLASS_LABELS: tuple[str, ...] = (
    "Fly",
    "GroundBall",
    "LineDrive",
    "PopUp",
    "Bunt",
)

TRAJECTORY_BUNT_REMAP: tuple[tuple[str, str], ...] = (
    ("FoulBunt", "Bunt"),
    ("GroundBallBunt", "Bunt"),
    ("LineDriveBunt", "Bunt"),
    ("PopUpBunt", "Bunt"),
    ("UnspecifiedBunt", "Bunt"),
)


def _geometry_spec(dimension: str) -> DeepTargetSpec:
    if dimension == "trajectory":
        return DeepTargetSpec(
            name=f"geometry_{dimension}",
            dataset_name=DATASET_NAME,
            target_column=TARGET_COLUMN,
            weight_column=WEIGHT_COLUMN,
            kind="multiclass",
            proposal_dimension=dimension,
            class_universe_source="configured",
            configured_class_labels=TRAJECTORY_CLASS_LABELS,
            target_remap=TRAJECTORY_BUNT_REMAP,
            calibration_method="temperature",
            loss_type="focal",
            focal_gamma=2.0,
            fold_count=1,
            slice_columns=GEOMETRY_SLICE_COLUMNS,
            game_id_column=GAME_ID_COLUMN,
            split_column=SPLIT_COLUMN,
            filter_predicate=(
                "geometry_dimension = 'trajectory' "
                "AND observed_status IN ('observed', 'derived', 'unknown_code')"
            ),
            loss_mask_predicate="observed_status = 'observed'",
            pretrained_embeddings_artifact_id="event_universe",
        )
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
        pretrained_embeddings_artifact_id="event_universe",
    )


GEOMETRY_SPECS: tuple[DeepTargetSpec, ...] = tuple(
    _geometry_spec(d) for d in GEOMETRY_DIMENSIONS
)


def _register() -> None:
    validate_pre_event(GEOMETRY_LAYOUT)
    register_coverage_layout(DATASET_NAME, GEOMETRY_LAYOUT)
    for spec in GEOMETRY_SPECS:
        register_target(spec, sibling_manifest="dl_proposal_manifest")


_register()
