"""Pitch-summary deep proposal: single binary target on has_count.

Pitch-summary is a 1:1-grain dataset (one row per ``event_key``). PR4
ships a single binary spec on ``has_count`` to validate the "publish to
dl_proposal_manifest with a non-geometry dimension value" path. Other
pitch-summary outcomes (``has_pitch_sequence``, the raw count/sequence
classes) can register later under the same dimension via additional
specs without touching the JOIN side.
"""

from __future__ import annotations

from python_models.ml.features import FeatureLayout
from python_models.statistical.deep.feature_layout import (
    register_coverage_layout,
    validate_pre_event,
)
from python_models.statistical.deep.registry import register_target
from python_models.statistical.deep.target_spec import DeepTargetSpec

DATASET_NAME: str = "model_input_pitch_summary"

HIGH_CARD_COLUMNS: tuple[str, ...] = (
    "batter_id",
    "pitcher_id",
    "park_id",
)

PRE_EVENT_LOW_CARD: tuple[str, ...] = (
    "league",
    "game_type",
    "frame_start",
    "batter_hand",
    "pitcher_hand",
    "base_state_start",
    "leverage_bucket",
    "personnel_confidence",
    "context_confidence",
)

LOW_CARD_COLUMNS: tuple[str, ...] = PRE_EVENT_LOW_CARD

NUMERIC_COLUMNS: tuple[str, ...] = (
    "season",
    "inning_start",
    "outs_start",
    "score_margin",
    "leverage_index",
)

PITCH_SUMMARY_LAYOUT: FeatureLayout = FeatureLayout(
    high_card_columns=HIGH_CARD_COLUMNS,
    low_card_columns=LOW_CARD_COLUMNS,
    numeric_columns=NUMERIC_COLUMNS,
    grain_column="event_key",
    split_column="primary_fold",
)

HAS_COUNT_SPEC: DeepTargetSpec = DeepTargetSpec(
    name="pitch_summary_has_count",
    dataset_name=DATASET_NAME,
    target_column="has_count",
    weight_column="training_weight",
    kind="binary",
    proposal_dimension="pitch_summary",
    calibration_method="temperature",
    fold_count=5,
    slice_columns=("season", "league", "source_family"),
    game_id_column="game_id",
    split_column="primary_fold",
    pretrained_embeddings_artifact_id="event_universe",
)

PITCH_SUMMARY_SPECS: tuple[DeepTargetSpec, ...] = (HAS_COUNT_SPEC,)


def _register() -> None:
    validate_pre_event(PITCH_SUMMARY_LAYOUT)
    register_coverage_layout(DATASET_NAME, PITCH_SUMMARY_LAYOUT)
    for spec in PITCH_SUMMARY_SPECS:
        register_target(spec, sibling_manifest="dl_proposal_manifest")


_register()
