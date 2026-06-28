"""Advancement deep target spec contract.

Advancement specs are intentionally NOT registered at import time
(`model_input_advancement` does not yet emit `advancement_class` /
`time_forward_fold`). These tests exercise only the spec definitions —
registry-dependent assertions return once the SQL gap closes and
`_register()` is invoked on import.
"""

from __future__ import annotations

from python_models.statistical.deep import targets as _targets  # noqa: F401
from python_models.statistical.deep.feature_layout import (
    validate_pre_event,
)
from python_models.statistical.deep.targets.advancement import (
    ADVANCEMENT_CLASS_LABELS,
    ADVANCEMENT_LAYOUT,
    ADVANCEMENT_SPECS,
)


def test_advancement_layout_is_pre_event() -> None:
    validate_pre_event(ADVANCEMENT_LAYOUT)


def test_advancement_layout_excludes_geometry_observations() -> None:
    all_cols = (
        tuple(ADVANCEMENT_LAYOUT.high_card_columns)
        + tuple(ADVANCEMENT_LAYOUT.low_card_columns)
        + tuple(ADVANCEMENT_LAYOUT.numeric_columns)
    )
    assert "trajectory_class" not in all_cols
    assert "location_depth_class" not in all_cols
    assert "ball_handler_position_class" not in all_cols
    assert "hit_or_out" not in all_cols
    assert "runs_on_play" not in all_cols
    assert "result_family" not in all_cols


def test_runner_id_in_high_card() -> None:
    assert "runner_id" in ADVANCEMENT_LAYOUT.high_card_columns


def test_three_specs_per_runner() -> None:
    assert len(ADVANCEMENT_SPECS) == 3
    names = {s.name for s in ADVANCEMENT_SPECS}
    assert names == {"advancement_r1", "advancement_r2", "advancement_r3"}


def test_specs_filter_per_baserunner() -> None:
    baserunners = {s.filter_predicate for s in ADVANCEMENT_SPECS}
    assert baserunners == {
        "baserunner = 'First'",
        "baserunner = 'Second'",
        "baserunner = 'Third'",
    }


def test_specs_use_seven_class_advancement() -> None:
    assert len(ADVANCEMENT_CLASS_LABELS) == 7
    for spec in ADVANCEMENT_SPECS:
        assert spec.kind == "multiclass"
        assert spec.configured_class_labels == ADVANCEMENT_CLASS_LABELS
        assert spec.class_universe_source == "configured"


def test_specs_wire_pretrain_artifact() -> None:
    for spec in ADVANCEMENT_SPECS:
        assert spec.pretrained_embeddings_artifact_id == "event_universe"


def test_published_manifest_name_per_spec() -> None:
    for spec in ADVANCEMENT_SPECS:
        assert spec.published_manifest_name() == f"dl_proposal_{spec.name}"
