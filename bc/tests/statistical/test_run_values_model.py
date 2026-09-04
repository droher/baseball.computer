"""Builder tests for the run-expectancy count model."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

import numpy as np
import pymc as pm
import pytest
from scipy.stats import nbinom

from python_models.statistical.models._run_values_data import (
    ERA_REGIME_LABELS,
    RunExpectancyHeldOutSet,
    RunExpectancyInputs,
    _build_era_regime_design,
)
from python_models.statistical.models.run_values import build_run_expectancy_model


def _tiny_inputs() -> RunExpectancyInputs:
    cell_labels = [
        "1972|AL|0_0",
        "1974|AL|0_0",
        "1972|AL|1_3",
        "1974|AL|1_3",
    ]
    state_labels = ["0_0", "1_3"]
    n = len(cell_labels)
    rng = np.random.default_rng(20260513)
    season_by_cell = [1972, 1974, 1972, 1974]
    league_by_cell = ["AL", "AL", "AL", "AL"]
    cell_era_design, era_labels = _build_era_regime_design(
        season_by_cell, league_by_cell
    )
    return RunExpectancyInputs(
        sum_runs=rng.integers(0, 12, size=n).astype(np.int64),
        cell_event_count=rng.integers(30, 60, size=n).astype(np.int64),
        cell_state_idx=np.array([0, 0, 1, 1], dtype=np.int64),
        cell_era_design=cell_era_design,
        cell_labels=list(cell_labels),
        era_labels=list(era_labels),
        state_by_cell=[0, 0, 11, 11],
        outs_by_cell=[0, 0, 1, 1],
        base_state_by_cell=[0, 0, 3, 3],
        season_by_cell=season_by_cell,
        league_by_cell=league_by_cell,
        state_labels=list(state_labels),
        outcome="runs_to_end",
        coords={
            "source": ["__single__"],
            "state": list(state_labels),
            "cell": list(cell_labels),
            "era_regime": list(era_labels),
        },
        held_out=RunExpectancyHeldOutSet(
            sum_runs=np.zeros(0, dtype=np.int64),
            cell_event_count=np.zeros(0, dtype=np.int64),
            cell_idx=np.zeros(0, dtype=np.int64),
            cell_state_idx=np.zeros(0, dtype=np.int64),
        ),
    )


def test_builds_model_with_re_value() -> None:
    inputs = _tiny_inputs()
    model = build_run_expectancy_model(inputs)
    assert isinstance(model, pm.Model)
    assert "re_value" in model.named_vars
    assert "mu_state" in model.named_vars
    assert "sum_runs_obs" in {rv.name for rv in model.observed_RVs}


def test_re_value_is_the_only_cell_sized_deterministic() -> None:
    inputs = _tiny_inputs()
    model = build_run_expectancy_model(inputs)

    n_cell = inputs.n_cells
    assert n_cell == len(inputs.coords["cell"])

    re_dims = tuple(model.named_vars_to_dims.get("re_value", ()))
    assert re_dims == ("cell",)

    mu_dims = tuple(model.named_vars_to_dims.get("mu_state", ()))
    assert mu_dims == ("state",)

    coord_lengths = {name: len(vals) for name, vals in inputs.coords.items()}
    cell_sized = [
        det.name
        for det in model.deterministics
        if tuple(
            coord_lengths.get(d, -1) for d in model.named_vars_to_dims.get(det.name, ())
        )
        and n_cell
        in tuple(
            coord_lengths.get(d, -1) for d in model.named_vars_to_dims.get(det.name, ())
        )
    ]
    assert cell_sized == ["re_value"]

    for det in model.deterministics:
        dims = tuple(model.named_vars_to_dims.get(det.name, ()))
        assert dims, f"{det.name} carries no named dims"


def test_phi_is_per_state() -> None:
    inputs = _tiny_inputs()
    model = build_run_expectancy_model(inputs)
    assert tuple(model.named_vars_to_dims["phi"]) == ("state",)
    drawn = np.asarray(pm.draw(model["phi"], draws=1, random_seed=0))
    assert drawn.shape == (len(inputs.state_labels),)
    assert np.all(drawn > 0)


def test_nb_alpha_uses_each_cell_state_phi() -> None:
    inputs = _tiny_inputs()
    model = build_run_expectancy_model(inputs)
    logp_obs = model.compile_fn(
        model.logp(vars=[model["sum_runs_obs"]], sum=False),
        inputs=model.value_vars,
        on_unused_input="ignore",
    )

    point = model.initial_point()
    phi_value_name = model.rvs_to_values[model["phi"]].name
    theta_value_name = model.rvs_to_values[model["theta_cell"]].name
    assert phi_value_name in point and theta_value_name in point

    rng = np.random.default_rng(3)
    phi_by_state = rng.uniform(0.5, 4.0, size=len(inputs.state_labels))
    theta = rng.normal(0.0, 0.3, size=inputs.n_cells)
    point[phi_value_name] = np.log(phi_by_state)
    point[theta_value_name] = theta

    per_cell = np.asarray(logp_obs(point)[0], dtype=np.float64)

    n = inputs.cell_event_count.astype(np.float64)
    mu = n * np.exp(theta)
    alpha = n * phi_by_state[inputs.cell_state_idx]
    expected = nbinom.logpmf(inputs.sum_runs, alpha, alpha / (alpha + mu))
    assert np.allclose(per_cell, expected, atol=1e-8)

    shared = n * phi_by_state.mean()
    with_global_phi = nbinom.logpmf(inputs.sum_runs, shared, shared / (shared + mu))
    assert not np.allclose(per_cell, with_global_phi, atol=1e-6)


def test_era_design_is_multi_hot_and_pruned() -> None:
    seasons = [1950, 1980, 1980, 2023, 2021]
    leagues = ["NL", "AL", "NL", "AL", "AL"]
    design, labels = _build_era_regime_design(seasons, leagues)

    assert design.shape == (5, len(labels))
    assert set(design.flatten().tolist()) <= {0.0, 1.0}

    full_dh = "full_DH" in labels
    dup = "extra_inning_ghost_plus_expanded_DH" in labels
    assert full_dh and not dup

    pre_dh_col = design[:, labels.index("pre_DH")]
    assert pre_dh_col.tolist() == [1.0, 0.0, 0.0, 0.0, 0.0]
    al_only_col = design[:, labels.index("DH_AL_only")]
    assert al_only_col.tolist() == [0.0, 1.0, 0.0, 0.0, 1.0]


def test_era_design_drops_all_zero_and_duplicate_columns() -> None:
    seasons = [1960, 1965]
    leagues = ["NL", "AL"]
    _, labels = _build_era_regime_design(seasons, leagues)
    assert labels == ["pre_DH"]


def test_builder_includes_era_hierarchy_when_active() -> None:
    inputs = _tiny_inputs()
    assert inputs.era_labels == ["pre_DH", "DH_AL_only"]
    model = build_run_expectancy_model(inputs)
    assert "a_era" in model.named_vars
    assert "sigma_era" in model.named_vars
    assert tuple(model.named_vars_to_dims["a_era"]) == ("state", "era_regime")


def test_builder_drops_era_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BC_RUN_VALUES_DISABLE_ERA_REGIME", "1")
    inputs = _tiny_inputs()
    model = build_run_expectancy_model(inputs)
    assert "a_era" not in model.named_vars
    assert "re_value" in model.named_vars


def test_era_labels_subset_of_known_regimes() -> None:
    inputs = _tiny_inputs()
    assert set(inputs.era_labels) <= set(ERA_REGIME_LABELS)


@pytest.mark.slow
def test_prior_predictive_runs() -> None:
    inputs = _tiny_inputs()
    model = build_run_expectancy_model(inputs)
    idata = pm.sample_prior_predictive(draws=2, model=model)
    assert "sum_runs_obs" in idata.prior_predictive
