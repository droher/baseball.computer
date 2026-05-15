"""Fielding-credit deep proposals: one binary spec per credit_type.

The full fold runner work for fielding credit (personnel-eligibility
mask applied before calibration, multi-output model per
fielding_position, zero-credit ineligible rows force ``dl_p_class =
ZEROES``) lands in a follow-up. PR5 ships the per-credit_type spec
registration so ``dl_credit_proposal_manifest`` knows what targets to
look for and ``fit-deep`` accepts the target names.

Each spec publishes to ``dl_credit_proposal_manifest`` (not the default
``dl_proposal_manifest``) — that sibling manifest already carries the
correct (event_key, player_id, fielding_position, credit_type) grain
that ``model_input_fielding_credit`` joins on.
"""

from __future__ import annotations

from python_models.ml.features import FeatureLayout
from python_models.statistical.deep.feature_layout import register_coverage_layout
from python_models.statistical.deep.registry import register_target
from python_models.statistical.deep.target_spec import DeepTargetSpec

DATASET_NAME: str = "model_input_fielding_credit"

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
    "personnel_confidence",
    "context_confidence",
    "fielding_evidence_status",
    "gap_class",
)

NUMERIC_COLUMNS: tuple[str, ...] = (
    "season",
    "inning_start",
    "outs_start",
    "score_margin",
    "leverage_index",
)

FIELDING_CREDIT_LAYOUT: FeatureLayout = FeatureLayout(
    high_card_columns=HIGH_CARD_COLUMNS,
    low_card_columns=LOW_CARD_COLUMNS,
    numeric_columns=NUMERIC_COLUMNS,
    grain_column="event_key",
    split_column="primary_fold",
)

CREDIT_TYPES: tuple[str, ...] = ("putout", "assist", "error")


def _credit_spec(credit_type: str) -> DeepTargetSpec:
    return DeepTargetSpec(
        name=f"fielding_credit_{credit_type}",
        dataset_name=DATASET_NAME,
        target_column="known_credit",
        weight_column="training_weight",
        kind="binary",
        proposal_dimension=credit_type,
        calibration_method="temperature",
        fold_count=5,
        slice_columns=("season", "league", "source_family", "fielding_evidence_status"),
        game_id_column="game_id",
        split_column="primary_fold",
        filter_predicate=(
            f"credit_type = '{credit_type}' AND eligible_for_allocation"
        ),
    )


FIELDING_CREDIT_SPECS: tuple[DeepTargetSpec, ...] = tuple(
    _credit_spec(c) for c in CREDIT_TYPES
)


def _register() -> None:
    register_coverage_layout(DATASET_NAME, FIELDING_CREDIT_LAYOUT)
    for spec in FIELDING_CREDIT_SPECS:
        register_target(spec, sibling_manifest="dl_credit_proposal_manifest")


_register()
