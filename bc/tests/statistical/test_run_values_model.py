"""Builder tests for the run-expectancy count model."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

import numpy as np
import pymc as pm
import pytest

from python_models.statistical.models._run_values_data import (
    RunExpectancyHeldOutSet,
    RunExpectancyInputs,
)
from python_models.statistical.models.run_values import build_run_expectancy_model


def _tiny_inputs() -> RunExpectancyInputs:
    cell_labels = [
        "1933_AL_0_0",
        "1934_AL_0_0",
        "1933_AL_1_3",
        "1934_AL_1_3",
    ]
    state_labels = ["0_0", "1_3"]
    n = len(cell_labels)
    rng = np.random.default_rng(20260513)
    return RunExpectancyInputs(
        sum_runs=rng.integers(0, 12, size=n).astype(np.int64),
        cell_event_count=rng.integers(30, 60, size=n).astype(np.int64),
        cell_state_idx=np.array([0, 0, 1, 1], dtype=np.int64),
        cell_labels=list(cell_labels),
        state_by_cell=[0, 0, 11, 11],
        outs_by_cell=[0, 0, 1, 1],
        base_state_by_cell=[0, 0, 3, 3],
        season_by_cell=[1933, 1934, 1933, 1934],
        league_by_cell=["AL", "AL", "AL", "AL"],
        state_labels=list(state_labels),
        outcome="runs_to_end",
        coords={
            "source": ["__single__"],
            "state": list(state_labels),
            "cell": list(cell_labels),
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


@pytest.mark.slow
def test_prior_predictive_runs() -> None:
    inputs = _tiny_inputs()
    model = build_run_expectancy_model(inputs)
    idata = pm.sample_prior_predictive(draws=2, model=model)
    assert "sum_runs_obs" in idata.prior_predictive
