"""DeepTargetSpec construction contract: fold_count must support real OOF."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from python_models.statistical.deep.target_spec import DeepTargetSpec

MINIMAL_KWARGS: dict[str, Any] = {
    "name": "spec_under_test",
    "dataset_name": "synthetic_dataset",
    "target_column": "target_class",
    "weight_column": "weight",
    "kind": "multiclass",
    "proposal_dimension": "synthetic",
}


def test_minimal_kwargs_construct() -> None:
    spec = DeepTargetSpec(**MINIMAL_KWARGS)
    assert spec.name == "spec_under_test"


def test_default_fold_count_supports_oof() -> None:
    spec = DeepTargetSpec(**MINIMAL_KWARGS)
    assert spec.fold_count >= 2


@pytest.mark.parametrize("fold_count", [1, 0, -1])
def test_fold_count_below_two_rejected(fold_count: int) -> None:
    with pytest.raises(ValidationError, match="fold_count"):
        DeepTargetSpec(**MINIMAL_KWARGS, fold_count=fold_count)


def test_fold_count_two_accepted() -> None:
    spec = DeepTargetSpec(**MINIMAL_KWARGS, fold_count=2)
    assert spec.fold_count == 2
