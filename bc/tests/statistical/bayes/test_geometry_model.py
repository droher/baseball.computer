"""Builder test for the geometry model (Model E)."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

import numpy as np

from python_models.statistical.models._credit_data import (
    FixedEffectDesign,
    HeldOutSet,
)
from python_models.statistical.models._geometry_data import GeometryInputs
from python_models.statistical.models.geometry import build_geometry_model

FIXED_EFFECT_COLUMNS = ("result_family", "outs_start")


def _empty_held_out() -> HeldOutSet:
    empty = np.zeros(0, dtype=np.int64)
    return HeldOutSet(
        event_keys=empty,
        true_position=empty,
        U=empty,
        season_idx=empty,
        scorer_idx=empty,
        park_idx=empty,
        source_idx=empty,
        fixed_effects={},
    )


def _synthetic_inputs(
    *,
    n_classes: int = 4,
    n_events: int = 24,
    dl_active: bool = True,
    propensity_active: bool = False,
    handler_active: bool = False,
    seed: int = 20260525,
) -> GeometryInputs:
    rng = np.random.default_rng(seed)
    class_labels = [f"class_{k}" for k in range(n_classes)]

    class_idx = rng.integers(0, n_classes, size=n_events)
    counts = np.zeros((n_events, n_classes), dtype=np.int64)
    counts[np.arange(n_events), class_idx] = 1

    dl_logit_per_class = (
        rng.normal(size=(n_events, n_classes)).astype(np.float64)
        if dl_active
        else np.zeros((n_events, n_classes), dtype=np.float64)
    )
    dl_logit_class_means = dl_logit_per_class.mean(axis=0).astype(np.float64)
    dl_logit_per_class = dl_logit_per_class - dl_logit_class_means[None, :]

    season_league_labels = ["1925|AL", "1925|NL", "1926|AL"]
    scorer_labels = ["scorerA", "scorerB"]
    season_league_idx = rng.integers(
        0, len(season_league_labels), size=n_events
    ).astype(np.int64)
    scorer_idx = rng.integers(0, len(scorer_labels), size=n_events).astype(np.int64)

    fe_levels: dict[str, tuple[str, ...]] = {
        "result_family": ("out_in_play", "hit_in_play"),
        "outs_start": ("0", "1", "2"),
    }
    fixed_effects: dict[str, FixedEffectDesign] = {}
    coords: dict[str, list[str]] = {
        "season_league": list(season_league_labels),
        "scorer": list(scorer_labels),
    }
    for column in FIXED_EFFECT_COLUMNS:
        levels = fe_levels[column]
        codes = rng.integers(0, len(levels), size=n_events).astype(np.int64)
        fixed_effects[column] = FixedEffectDesign(levels=levels, codes=codes)
        coords[f"{column}_levels"] = list(levels)

    propensity_z = (
        rng.normal(size=n_events).astype(np.float64)
        if propensity_active
        else np.zeros(n_events, dtype=np.float64)
    )
    handler_z = (
        rng.normal(size=n_events).astype(np.float64)
        if handler_active
        else np.zeros(n_events, dtype=np.float64)
    )

    return GeometryInputs(
        event_keys=np.arange(n_events, dtype=np.int64),
        dimension="trajectory",
        class_labels=class_labels,
        n_classes=n_classes,
        dl_active=dl_active,
        counts=counts,
        dl_logit_per_class=dl_logit_per_class,
        dl_logit_class_means=dl_logit_class_means,
        season_league_idx=season_league_idx,
        scorer_idx=scorer_idx,
        fixed_effects=fixed_effects,
        coords=coords,
        held_out=_empty_held_out(),
        held_out_dl_logit_per_class=np.zeros((0, n_classes), dtype=np.float64),
        propensity_z=propensity_z,
        held_out_propensity_z=np.zeros(0, dtype=np.float64),
        propensity_active=propensity_active,
        handler_z=handler_z,
        held_out_handler_z=np.zeros(0, dtype=np.float64),
        handler_active=handler_active,
    )


def _rv_names(model: object) -> set[str]:
    return {rv.name for rv in model.unobserved_RVs}


def test_builds_valid_graph_with_expected_rvs() -> None:
    inputs = _synthetic_inputs(n_classes=5)
    model = build_geometry_model(inputs)

    rv_names = _rv_names(model)
    det_names = {d.name for d in model.deterministics}
    observed_names = {rv.name for rv in model.observed_RVs}
    data_names = {d.name for d in model.data_vars}

    assert "alpha_class" in rv_names
    assert {
        "sigma_season_league",
        "sigma_scorer",
        "z_season_league",
        "z_scorer",
    }.isdisjoint(rv_names), (
        "scalar-per-event REs cancel in the softmax and must not be in the model"
    )
    assert {"beta_season_league", "beta_scorer"}.isdisjoint(det_names)
    for column in FIXED_EFFECT_COLUMNS:
        assert f"delta_{column}" in rv_names
    assert "G_observed" in observed_names
    assert "dl_logit_per_class" in data_names


def test_no_event_sized_deterministic() -> None:
    inputs = _synthetic_inputs()
    model = build_geometry_model(inputs)
    det_names = {d.name for d in model.deterministics}
    assert "pi" not in det_names and "eta" not in det_names, (
        "per-event pi / eta must not be Deterministics — they blow up posterior.nc"
    )
    for det in model.deterministics:
        shape = tuple(model.named_vars_to_dims.get(det.name, ()))
        assert "event" not in shape, f"{det.name} is event-sized"


def test_gamma_dl_absent_under_zero_flavor() -> None:
    inputs = _synthetic_inputs(dl_active=True)
    model = build_geometry_model(inputs, gamma_dl_flavor="gamma_dl_zero")
    assert "gamma_dl" not in _rv_names(model)
    assert "dl_logit_per_class" in {d.name for d in model.data_vars}


def test_gamma_dl_present_under_shrunk_when_dl_active() -> None:
    inputs = _synthetic_inputs(dl_active=True)
    model = build_geometry_model(inputs, gamma_dl_flavor="gamma_dl_shrunk")
    assert "gamma_dl" in _rv_names(model)
    assert "dl_logit_per_class" in {d.name for d in model.data_vars}


def test_gamma_dl_absent_under_shrunk_when_dl_inactive() -> None:
    inputs = _synthetic_inputs(dl_active=False)
    model = build_geometry_model(inputs, gamma_dl_flavor="gamma_dl_shrunk")
    assert "gamma_dl" not in _rv_names(model)
    assert "dl_logit_per_class" in {d.name for d in model.data_vars}


def test_gamma_propensity_absent_under_zero_flavor() -> None:
    inputs = _synthetic_inputs(propensity_active=True)
    model = build_geometry_model(
        inputs, gamma_propensity_flavor="gamma_propensity_zero"
    )
    assert "gamma_propensity" not in _rv_names(model)
    assert "propensity_z" in {d.name for d in model.data_vars}


def test_gamma_propensity_present_under_class_flavor_when_active() -> None:
    import pymc as pm

    inputs = _synthetic_inputs(propensity_active=True)
    model = build_geometry_model(
        inputs, gamma_propensity_flavor="gamma_propensity_class"
    )
    assert "gamma_propensity" in _rv_names(model)
    assert "propensity_z" in {d.name for d in model.data_vars}
    assert tuple(model.named_vars_to_dims["gamma_propensity"]) == ("class",)
    draw = pm.draw(model["gamma_propensity"], random_seed=20260610)
    assert draw.shape == (inputs.n_classes,)
    assert float(np.abs(draw.sum())) < 1e-8, (
        "gamma_propensity must be zero-sum over the class dim — a constant "
        "component cancels in the softmax and is unidentified"
    )


def test_gamma_propensity_absent_under_class_flavor_when_inactive() -> None:
    inputs = _synthetic_inputs(propensity_active=False)
    model = build_geometry_model(
        inputs, gamma_propensity_flavor="gamma_propensity_class"
    )
    assert "gamma_propensity" not in _rv_names(model)
    assert "propensity_z" in {d.name for d in model.data_vars}


def test_propensity_z_data_absent_from_inputs_defaults_to_zeros() -> None:
    inputs = _synthetic_inputs(propensity_active=False).model_copy(
        update={"propensity_z": None}
    )
    model = build_geometry_model(inputs)
    np.testing.assert_allclose(
        model["propensity_z"].eval(), np.zeros(inputs.n_events, dtype=np.float64)
    )


def test_gamma_handler_present_when_handler_active() -> None:
    import pymc as pm

    inputs = _synthetic_inputs(handler_active=True)
    model = build_geometry_model(inputs)
    assert "gamma_handler" in _rv_names(model)
    assert "handler_z" in {d.name for d in model.data_vars}
    assert tuple(model.named_vars_to_dims["gamma_handler"]) == ("class",)
    draw = pm.draw(model["gamma_handler"], random_seed=20260624)
    assert draw.shape == (inputs.n_classes,)
    assert float(np.abs(draw.sum())) < 1e-8, (
        "gamma_handler must be zero-sum over the class dim — a constant "
        "component cancels in the softmax and is unidentified"
    )


def test_gamma_handler_absent_when_handler_inactive() -> None:
    inputs = _synthetic_inputs(handler_active=False)
    model = build_geometry_model(inputs)
    assert "gamma_handler" not in _rv_names(model)
    assert "handler_z" in {d.name for d in model.data_vars}


def test_handler_z_data_absent_from_inputs_defaults_to_zeros() -> None:
    inputs = _synthetic_inputs(handler_active=False).model_copy(
        update={"handler_z": None}
    )
    model = build_geometry_model(inputs)
    np.testing.assert_allclose(
        model["handler_z"].eval(), np.zeros(inputs.n_events, dtype=np.float64)
    )


def test_handler_inert_for_advancement_style_inputs_without_handler() -> None:
    inputs = _synthetic_inputs(handler_active=False, dl_active=False)
    model = build_geometry_model(inputs)
    assert "gamma_handler" not in _rv_names(model)
    assert "G_observed" in {rv.name for rv in model.observed_RVs}
