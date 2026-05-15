"""Fielding-credit deep target registry contract."""

from __future__ import annotations

import pytest

from python_models.statistical.deep import targets as _targets  # noqa: F401
from python_models.statistical.deep.feature_layout import (
    coverage_layout_for,
    registered_datasets,
)
from python_models.statistical.deep.registry import get_target, sibling_manifest_for
from python_models.statistical.deep.targets.fielding_credit import (
    CREDIT_TYPES,
    DATASET_NAME,
    FIELDING_CREDIT_SPECS,
)


def test_fielding_credit_layout_registered() -> None:
    assert DATASET_NAME in registered_datasets()
    layout = coverage_layout_for(DATASET_NAME)
    assert layout.grain_column == "event_key"
    assert layout.split_column == "primary_fold"


def test_three_credit_specs_registered() -> None:
    names = {spec.name for spec in FIELDING_CREDIT_SPECS}
    assert names == {f"fielding_credit_{c}" for c in CREDIT_TYPES}


def test_each_spec_publishes_to_credit_manifest() -> None:
    for spec in FIELDING_CREDIT_SPECS:
        assert sibling_manifest_for(spec.name) == "dl_credit_proposal_manifest"


def test_each_spec_filters_to_one_credit_type_and_eligibility() -> None:
    for spec in FIELDING_CREDIT_SPECS:
        assert spec.filter_predicate is not None
        assert f"credit_type = '{spec.proposal_dimension}'" in spec.filter_predicate
        assert "eligible_for_allocation" in spec.filter_predicate


@pytest.mark.parametrize("credit_type", CREDIT_TYPES)
def test_each_spec_resolves_via_get_target(credit_type: str) -> None:
    spec = get_target(f"fielding_credit_{credit_type}")
    assert spec.proposal_dimension == credit_type
    assert spec.kind == "binary"


def test_published_name_uses_credit_type() -> None:
    for spec in FIELDING_CREDIT_SPECS:
        assert spec.published_manifest_name() == f"dl_proposal_{spec.proposal_dimension}"
