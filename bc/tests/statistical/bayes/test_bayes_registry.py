"""Registry sanity for the Phase-4 observation propensity registry."""

from __future__ import annotations

import pytest

from python_models.statistical.bayes import targets as _targets  # noqa: F401  # pyright: ignore[reportUnusedImport]
from python_models.statistical.bayes.registry import all_targets


def test_at_least_one_target_registered() -> None:
    targets = all_targets()
    assert targets, "no bayes targets registered"


def test_observation_targets_share_dataset() -> None:
    targets = [
        t
        for t in all_targets()
        if t.outcome_kind == "bernoulli"
        and t.dataset_name == "model_input_observation_batted_ball"
    ]
    assert targets, "no observation-propensity bernoulli targets registered"
    datasets = {spec.dataset_name for spec in targets}
    assert datasets == {"model_input_observation_batted_ball"}


def test_pitch_count_observedness_registered() -> None:
    from python_models.statistical.bayes.registry import get_target

    spec = get_target("pitch_count_observedness")
    assert spec.outcome_kind == "bernoulli"
    assert spec.multinomial_export is None
    assert spec.count_export is None
    assert spec.dataset_name == "model_input_pitch_summary"
    assert spec.dataset_dimension_filter == "has_count"
    assert spec.dimension == "has_count"
    assert spec.sample_size == 10_000
    assert spec.published_manifest_name() == "pitch_count_observedness"


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


def test_no_dl_source_restricts_default_flavors() -> None:
    for spec in all_targets():
        if spec.dl_proposal_dimension is None:
            assert spec.default_flavors == ("gamma_dl_zero",), (
                f"{spec.name} has no dl_proposal_dimension but "
                f"default_flavors={spec.default_flavors!r}"
            )


GEOMETRY_DL_DIMENSIONS = (
    "trajectory",
    "location_depth",
    "location_edge",
)
GEOMETRY_ZERO_FLAVOR_DIMENSIONS = ("general_location", "location_side")


def test_geometry_targets_registered() -> None:
    from python_models.statistical.bayes.registry import get_target

    expected = {*GEOMETRY_DL_DIMENSIONS, *GEOMETRY_ZERO_FLAVOR_DIMENSIONS}
    for dimension in expected:
        spec = get_target(f"geometry_{dimension}")
        assert spec.outcome_kind == "multinomial"
        assert spec.multinomial_export == "geometry"
        assert spec.dataset_name == "model_input_geometry"
        assert spec.dataset_dimension_filter == dimension
        assert spec.dimension == dimension
        assert spec.sample_size == 10_000


def test_geometry_dl_dimensions_carry_both_flavors() -> None:
    from python_models.statistical.bayes.registry import get_target

    for dimension in GEOMETRY_DL_DIMENSIONS:
        spec = get_target(f"geometry_{dimension}")
        assert spec.dl_proposal_dimension == dimension
        assert spec.default_flavors == ("gamma_dl_zero", "gamma_dl_shrunk")


def test_geometry_dimensions_without_validated_dl_are_zero_flavor_only() -> None:
    from python_models.statistical.bayes.registry import get_target

    for dimension in GEOMETRY_ZERO_FLAVOR_DIMENSIONS:
        spec = get_target(f"geometry_{dimension}")
        assert spec.dl_proposal_dimension is None
        assert spec.default_flavors == ("gamma_dl_zero",)


def test_geometry_global_side_disables_learned_supplements() -> None:
    from python_models.statistical.bayes.registry import get_target

    spec = get_target("geometry_location_side")
    assert spec.dl_proposal_dimension is None
    assert spec.default_flavors == ("gamma_dl_zero",)
    assert spec.propensity_dimension == "location_side"
    assert spec.default_propensity_flavors == ("gamma_propensity_zero",)


def test_five_geometry_targets_registered() -> None:
    geometry = [t for t in all_targets() if t.multinomial_export == "geometry"]
    assert len(geometry) == 5
    names = {spec.name for spec in geometry}
    expected = {
        f"geometry_{dimension}"
        for dimension in (*GEOMETRY_DL_DIMENSIONS, *GEOMETRY_ZERO_FLAVOR_DIMENSIONS)
    }
    assert names == expected


def test_state_transition_registered() -> None:
    from python_models.statistical.bayes.registry import get_target

    spec = get_target("state_transition")
    assert spec.outcome_kind == "multinomial"
    assert spec.multinomial_export == "state_transition"
    assert spec.count_export is None
    assert spec.dataset_name == "model_input_run_values"
    assert spec.dataset_dimension_filter == "state_transition"
    assert spec.dimension == "state_transition"
    assert spec.dl_proposal_dimension is None
    assert spec.default_flavors == ("gamma_dl_zero",)
    assert spec.sample_size is not None and spec.sample_size > 0
    assert spec.published_manifest_name() == "state_transition"


def test_no_propensity_dimension_restricts_propensity_flavors() -> None:
    for spec in all_targets():
        if spec.propensity_dimension is None:
            assert spec.default_propensity_flavors == ("gamma_propensity_zero",), (
                f"{spec.name} has no propensity_dimension but "
                f"default_propensity_flavors={spec.default_propensity_flavors!r}"
            )


def test_propensity_class_flavor_without_dimension_rejected() -> None:
    from python_models.statistical.bayes.registry import get_target
    from python_models.statistical.bayes.specs import BayesTargetSpec

    template = get_target("ball_handler_imputation")
    with pytest.raises(ValueError, match="gamma_propensity_class"):
        _ = BayesTargetSpec(
            name="propensity_validator_probe",
            dimension=template.dimension,
            dataset_name=template.dataset_name,
            dataset_dimension_filter=template.dataset_dimension_filter,
            prep_fn=template.prep_fn,
            builder=template.builder,
            propensity_dimension=None,
            default_propensity_flavors=(
                "gamma_propensity_zero",
                "gamma_propensity_class",
            ),
        )


def test_propensity_offset_flavor_allowed_without_dimension() -> None:
    from python_models.statistical.bayes.registry import get_target
    from python_models.statistical.bayes.specs import BayesTargetSpec

    template = get_target("ball_handler_imputation")
    spec = BayesTargetSpec(
        name="propensity_offset_probe",
        dimension=template.dimension,
        dataset_name=template.dataset_name,
        dataset_dimension_filter=template.dataset_dimension_filter,
        prep_fn=template.prep_fn,
        builder=template.builder,
        propensity_dimension=None,
        default_propensity_flavors=(
            "gamma_propensity_zero",
            "gamma_propensity_offset",
        ),
    )
    assert "gamma_propensity_offset" in spec.default_propensity_flavors


def test_imputation_targets_carry_configured_propensity_contract() -> None:
    imputation = [
        spec
        for spec in all_targets()
        if spec.multinomial_export in ("geometry", "ball_handler")
    ]
    assert len(imputation) == 6
    for spec in imputation:
        assert spec.propensity_dimension == spec.dataset_dimension_filter, (
            f"{spec.name} propensity_dimension {spec.propensity_dimension!r} must "
            f"match dataset_dimension_filter {spec.dataset_dimension_filter!r}"
        )
        if spec.name == "geometry_location_side":
            assert spec.default_propensity_flavors == ("gamma_propensity_zero",)
        else:
            assert spec.default_propensity_flavors == (
                "gamma_propensity_zero",
                "gamma_propensity_class",
            ), (
                f"{spec.name} must register both propensity flavors with "
                f"gamma_propensity_zero first (the run_bayes_model default)"
            )


def test_responsibility_name_is_reserved_not_registered() -> None:
    from typing import get_args

    from python_models.statistical.bayes.registry import get_target
    from python_models.statistical.bayes.specs import BayesTargetSpec

    names = {spec.name for spec in all_targets()}
    assert "responsibility" not in names
    with pytest.raises(KeyError):
        _ = get_target("responsibility")

    annotation = BayesTargetSpec.model_fields["multinomial_export"].annotation
    literal_values = {
        value for member in get_args(annotation) for value in get_args(member)
    }
    assert literal_values, "multinomial_export Literal values not extracted"
    assert "responsibility" not in literal_values
