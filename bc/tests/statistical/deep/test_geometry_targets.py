"""Geometry deep target registry + spec contract."""

from __future__ import annotations

import pytest

from python_models.statistical.deep import targets as _targets  # noqa: F401
from python_models.statistical.deep.feature_layout import (
    coverage_layout_for,
    registered_datasets,
)
from python_models.statistical.deep.registry import (
    all_target_names,
    get_target,
    sibling_manifest_for,
)
from python_models.statistical.deep.targets.geometry import (
    DATASET_NAME,
    GEOMETRY_DIMENSIONS,
    GEOMETRY_SPECS,
)


def test_five_dimensions_registered() -> None:
    geometry_names = [n for n in all_target_names() if n.startswith("geometry_")]
    assert len(geometry_names) == 5
    assert set(geometry_names) == {f"geometry_{d}" for d in GEOMETRY_DIMENSIONS}


def test_each_dimension_publishes_to_proposal_manifest() -> None:
    for spec in GEOMETRY_SPECS:
        assert sibling_manifest_for(spec.name) == "dl_proposal_manifest"


def test_published_manifest_name_uses_raw_dimension() -> None:
    for spec in GEOMETRY_SPECS:
        assert spec.published_manifest_name() == f"dl_proposal_{spec.proposal_dimension}"


def test_filter_predicate_matches_dimension() -> None:
    for spec in GEOMETRY_SPECS:
        assert spec.filter_predicate is not None
        assert spec.proposal_dimension in spec.filter_predicate
        assert "is_observed_class" in spec.filter_predicate


def test_target_column_and_kind_consistent() -> None:
    for spec in GEOMETRY_SPECS:
        assert spec.target_column == "class"
        assert spec.kind == "multiclass"


@pytest.mark.parametrize("dimension", GEOMETRY_DIMENSIONS)
def test_each_dimension_resolves_via_get_target(dimension: str) -> None:
    spec = get_target(f"geometry_{dimension}")
    assert spec.proposal_dimension == dimension


def test_geometry_layout_registered() -> None:
    assert DATASET_NAME in registered_datasets()
    layout = coverage_layout_for(DATASET_NAME)
    assert layout.grain_column == "event_key"
    assert layout.split_column == "primary_fold"


def test_layout_columns_not_empty() -> None:
    layout = coverage_layout_for(DATASET_NAME)
    assert len(layout.high_card_columns) > 0
    assert len(layout.low_card_columns) > 0
    assert len(layout.numeric_columns) > 0


def test_layout_columns_distinct() -> None:
    layout = coverage_layout_for(DATASET_NAME)
    all_cols = (
        *layout.high_card_columns,
        *layout.low_card_columns,
        *layout.numeric_columns,
    )
    assert len(all_cols) == len(set(all_cols)), "feature columns must be distinct"
