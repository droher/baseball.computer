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
