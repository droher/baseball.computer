"""Registry sanity for the Phase-4 observation propensity registry."""

from __future__ import annotations

import pytest

from python_models.statistical.bayes import targets as _targets  # noqa: F401  # pyright: ignore[reportUnusedImport]
from python_models.statistical.bayes.registry import all_targets


def test_at_least_one_target_registered() -> None:
    targets = all_targets()
    assert targets, "no bayes targets registered"


def test_observation_targets_share_dataset() -> None:
    targets = all_targets()
    datasets = {spec.dataset_name for spec in targets}
    assert datasets == {"model_input_observation_batted_ball"}


def test_trajectory_observedness_registered() -> None:
    from python_models.statistical.bayes.registry import get_target

    spec = get_target("trajectory_observedness")
    assert spec.dataset_dimension_filter == "trajectory"
    assert spec.dimension == "trajectory"
    assert spec.published_manifest_name() == "trajectory_observedness"


def test_unknown_target_raises() -> None:
    from python_models.statistical.bayes.registry import get_target

    with pytest.raises(KeyError):
        _ = get_target("does_not_exist")


def test_every_obs_spec_pins_a_sample_size() -> None:
    for spec in all_targets():
        assert (
            spec.sample_size is not None and spec.sample_size > 0
        ), f"{spec.name} must declare a positive sample_size budget"


def test_every_obs_spec_filter_matches_dimension() -> None:
    for spec in all_targets():
        assert spec.dataset_dimension_filter == spec.dimension, (
            f"{spec.name} dataset_dimension_filter "
            f"{spec.dataset_dimension_filter!r} must match dimension "
            f"{spec.dimension!r} for the v1 prep contract"
        )


def test_obs_specs_dimensions_are_unique() -> None:
    dims = [spec.dimension for spec in all_targets()]
    assert len(dims) == len(set(dims)), (
        f"duplicate dimension across obs specs: {sorted(dims)}"
    )
