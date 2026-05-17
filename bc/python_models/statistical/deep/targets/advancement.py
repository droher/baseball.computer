"""Advancement deep proposals: one spec per non-batter baserunner.

Defines (does NOT register) ``advancement_r1`` / ``advancement_r2`` /
``advancement_r3`` against ``model_input_advancement`` (grain
``event_key, baserunner``). Each spec filters to a single ``baserunner``
value and predicts the 7-class advancement outcome
(``Stayed | Advanced1 | Advanced2 | Scored | OutAdvancing |
OutCaughtStealing | OutPickoff``).

Inputs are pre-event only. The dataset also carries trajectory /
location_depth / ball_handler_position observations — those are
post-event leak (correlated NULLs at inference) and are excluded from
the layout. ``validate_pre_event`` enforces it.

Module-level ``_register()`` is NOT invoked on import: the SQL view
``model_input_advancement`` does not yet emit ``advancement_class``
(the target column) or ``time_forward_fold``. Registering would break
any production fit-deep call that tried to resolve these targets. The
specs sit here as planning artifacts; call ``_register()`` explicitly
once the SQL gap closes. See ``notes/followups.md`` for the gap.
"""

from __future__ import annotations

from python_models.ml.features import FeatureLayout
from python_models.statistical.deep.feature_layout import (
    register_coverage_layout,
    validate_pre_event,
)
from python_models.statistical.deep.registry import register_target
from python_models.statistical.deep.target_spec import DeepTargetSpec

DATASET_NAME: str = "model_input_advancement"
GAME_ID_COLUMN: str = "game_id"
SPLIT_COLUMN: str = "primary_fold"
WEIGHT_COLUMN: str = "training_weight"
TARGET_COLUMN: str = "advancement_class"

PRE_EVENT_HIGH_CARD: tuple[str, ...] = (
    "batter_id",
    "pitcher_id",
    "runner_id",
    "park_id",
    "scorer",
)

PRE_EVENT_LOW_CARD: tuple[str, ...] = (
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
    "baserunner",
)

PRE_EVENT_NUMERIC: tuple[str, ...] = (
    "season",
    "inning_start",
    "outs_start",
    "score_margin",
    "leverage_index",
    "base_start",
)

ADVANCEMENT_LAYOUT: FeatureLayout = FeatureLayout(
    high_card_columns=PRE_EVENT_HIGH_CARD,
    low_card_columns=PRE_EVENT_LOW_CARD,
    numeric_columns=PRE_EVENT_NUMERIC,
    grain_column="event_key",
    split_column=SPLIT_COLUMN,
)

ADVANCEMENT_CLASS_LABELS: tuple[str, ...] = (
    "Stayed",
    "Advanced1",
    "Advanced2",
    "Scored",
    "OutAdvancing",
    "OutCaughtStealing",
    "OutPickoff",
)

ADVANCEMENT_SLICE_COLUMNS: tuple[str, ...] = (
    "season",
    "league",
    "source_family",
)

_BASERUNNERS: tuple[tuple[str, str], ...] = (
    ("advancement_r1", "First"),
    ("advancement_r2", "Second"),
    ("advancement_r3", "Third"),
)


def _advancement_spec(name: str, baserunner: str) -> DeepTargetSpec:
    return DeepTargetSpec(
        name=name,
        dataset_name=DATASET_NAME,
        target_column=TARGET_COLUMN,
        weight_column=WEIGHT_COLUMN,
        kind="multiclass",
        proposal_dimension=name,
        class_universe_source="configured",
        configured_class_labels=ADVANCEMENT_CLASS_LABELS,
        calibration_method="temperature",
        fold_count=5,
        slice_columns=ADVANCEMENT_SLICE_COLUMNS,
        game_id_column=GAME_ID_COLUMN,
        split_column=SPLIT_COLUMN,
        filter_predicate=f"baserunner = '{baserunner}'",
        pretrained_embeddings_artifact_id="event_universe",
    )


ADVANCEMENT_SPECS: tuple[DeepTargetSpec, ...] = tuple(
    _advancement_spec(name, br) for name, br in _BASERUNNERS
)


def _register() -> None:
    validate_pre_event(ADVANCEMENT_LAYOUT)
    register_coverage_layout(DATASET_NAME, ADVANCEMENT_LAYOUT)
    for spec in ADVANCEMENT_SPECS:
        register_target(spec, sibling_manifest="dl_advancement_proposal_manifest")
