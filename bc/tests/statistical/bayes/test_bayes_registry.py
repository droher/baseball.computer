"""Registry sanity for the Phase-4 observation propensity registry."""

from __future__ import annotations

import pytest

from python_models.statistical.bayes import targets as _targets  # noqa: F401  # pyright: ignore[reportUnusedImport]
from python_models.statistical.bayes.registry import all_targets


def test_at_least_one_target_registered() -> None:
    targets = all_targets()
    assert targets, "no bayes targets registered"


def test_observation_targets_share_dataset() -> None:
    targets = [t for t in all_targets() if t.outcome_kind == "bernoulli"]
    datasets = {spec.dataset_name for spec in targets}
    assert datasets == {"model_input_observation_batted_ball"}


def test_credit_targets_share_dataset() -> None:
    targets = [t for t in all_targets() if t.multinomial_export == "credit"]
    if not targets:
        return
    datasets = {spec.dataset_name for spec in targets}
    assert datasets == {"model_input_fielding_credit"}


def test_ball_handler_imputation_reads_observation_dataset() -> None:
    from python_models.statistical.bayes.registry import get_target

    spec = get_target("ball_handler_imputation")
    assert spec.outcome_kind == "multinomial"
    assert spec.multinomial_export == "ball_handler"
    assert spec.dataset_name == "model_input_observation_batted_ball"
    assert spec.dataset_dimension_filter == "ball_handler_position"
    assert spec.dimension == "ball_handler_position"


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
        assert spec.sample_size is not None and spec.sample_size > 0, (
            f"{spec.name} must declare a positive sample_size budget"
        )


def test_every_obs_spec_filter_matches_dimension() -> None:
    for spec in all_targets():
        assert spec.dataset_dimension_filter == spec.dimension, (
            f"{spec.name} dataset_dimension_filter "
            f"{spec.dataset_dimension_filter!r} must match dimension "
            f"{spec.dimension!r} for the v1 prep contract"
        )


def test_target_names_are_globally_unique() -> None:
    names = [spec.name for spec in all_targets()]
    assert len(names) == len(set(names)), (
        f"duplicate target name in registry: {sorted(names)}"
    )


def test_dimensions_unique_within_outcome_kind() -> None:
    by_kind: dict[str, list[str]] = {}
    for spec in all_targets():
        by_kind.setdefault(spec.outcome_kind, []).append(spec.dimension)
    for kind, dims in by_kind.items():
        assert len(dims) == len(set(dims)), (
            f"duplicate dimension within outcome_kind={kind!r}: {sorted(dims)}"
        )


def test_ball_handler_dimension_shared_across_outcome_kinds() -> None:
    from python_models.statistical.bayes.registry import get_target

    obs = get_target("ball_handler_position_observedness")
    imputation = get_target("ball_handler_imputation")
    assert obs.dimension == imputation.dimension == "ball_handler_position"
    assert obs.outcome_kind == "bernoulli"
    assert imputation.outcome_kind == "multinomial"


def test_putout_credit_allocation_registered() -> None:
    from python_models.statistical.bayes.registry import get_target

    spec = get_target("putout_credit_allocation")
    assert spec.outcome_kind == "multinomial"
    assert spec.dataset_name == "model_input_fielding_credit"
    assert spec.dataset_dimension_filter == "putout"


def test_assist_credit_allocation_registered() -> None:
    from python_models.statistical.bayes.registry import get_target

    spec = get_target("assist_credit_allocation")
    assert spec.outcome_kind == "multinomial"
    assert spec.dataset_name == "model_input_fielding_credit"
    assert spec.dataset_dimension_filter == "assist"
    assert spec.dimension == "assist"
