"""The Bayes geometry class vocab is bound to the DL proposal's softmax order."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from python_models.statistical.deep.targets.geometry import (
    GEOMETRY_DIMENSIONS as DL_GEOMETRY_DIMENSIONS,
)
from python_models.statistical.deep.targets.geometry import (
    GEOMETRY_SPECS,
    TRAJECTORY_BUNT_REMAP,
    TRAJECTORY_CLASS_LABELS,
)
from python_models.statistical.models._geometry_data import (
    _BUNT_VARIANTS,
    GEOMETRY_DIMENSIONS,
)


def test_trajectory_class_order_matches_dl_constant() -> None:
    assert GEOMETRY_DIMENSIONS["trajectory"].class_labels == TRAJECTORY_CLASS_LABELS


def test_trajectory_bunt_remap_binds_to_bayes_bunt_variants() -> None:
    assert tuple(source for source, _ in TRAJECTORY_BUNT_REMAP) == _BUNT_VARIANTS
    assert dict(TRAJECTORY_BUNT_REMAP) == GEOMETRY_DIMENSIONS["trajectory"].remap
    assert {target for _, target in TRAJECTORY_BUNT_REMAP} == {"Bunt"}
    assert "Bunt" in TRAJECTORY_CLASS_LABELS


def test_dl_backed_dimensions_are_exactly_the_dl_specs() -> None:
    dl_active = tuple(
        name for name, spec in GEOMETRY_DIMENSIONS.items() if spec.dl_active
    )
    assert dl_active == DL_GEOMETRY_DIMENSIONS
    by_dimension = {spec.proposal_dimension: spec for spec in GEOMETRY_SPECS}
    assert tuple(by_dimension) == DL_GEOMETRY_DIMENSIONS
    for dimension in dl_active:
        bayes = GEOMETRY_DIMENSIONS[dimension]
        assert bayes.class_labels is not None
        deep = by_dimension[dimension]
        if deep.class_universe_source == "configured":
            assert deep.configured_class_labels == bayes.class_labels
            assert dict(deep.target_remap) == bayes.remap
        else:
            assert deep.configured_class_labels == ()
            assert deep.target_remap == ()
            assert bayes.remap == {}


def test_non_dl_dimensions_carry_no_static_vocab() -> None:
    for name, spec in GEOMETRY_DIMENSIONS.items():
        if not spec.dl_active:
            assert spec.class_labels is None, name
            assert name not in DL_GEOMETRY_DIMENSIONS
