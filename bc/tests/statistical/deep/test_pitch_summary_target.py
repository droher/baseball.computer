"""Pitch-summary deep target registry contract."""

from __future__ import annotations

from python_models.statistical.deep import targets as _targets  # noqa: F401
from python_models.statistical.deep.feature_layout import (
    coverage_layout_for,
    registered_datasets,
)
from python_models.statistical.deep.registry import get_target, sibling_manifest_for
from python_models.statistical.deep.targets.pitch_summary import (
    DATASET_NAME,
    HAS_COUNT_SPEC,
    PITCH_SUMMARY_SPECS,
)


def test_pitch_summary_layout_registered() -> None:
    assert DATASET_NAME in registered_datasets()
    layout = coverage_layout_for(DATASET_NAME)
    assert layout.grain_column == "event_key"
    assert layout.split_column == "primary_fold"


def test_has_count_spec_is_binary() -> None:
    assert HAS_COUNT_SPEC.kind == "binary"
    assert HAS_COUNT_SPEC.target_column == "has_count"
    assert HAS_COUNT_SPEC.proposal_dimension == "pitch_summary"
    assert HAS_COUNT_SPEC.weight_column == "training_weight"


def test_has_count_spec_publishes_to_proposal_manifest() -> None:
    assert sibling_manifest_for(HAS_COUNT_SPEC.name) == "dl_proposal_manifest"


def test_has_count_published_name_uses_dimension() -> None:
    assert HAS_COUNT_SPEC.published_manifest_name() == "dl_proposal_pitch_summary"


def test_spec_resolves_via_get_target() -> None:
    spec = get_target("pitch_summary_has_count")
    assert spec is HAS_COUNT_SPEC


def test_pitch_summary_specs_tuple_contains_has_count() -> None:
    assert HAS_COUNT_SPEC in PITCH_SUMMARY_SPECS
