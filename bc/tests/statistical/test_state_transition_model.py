"""Builder + vocab tests for the base-out transition submodel."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

import numpy as np
import pymc as pm
import pytest

from python_models.statistical.models._state_transition_data import (
    INNING_END_INDEX,
    INNING_END_LABEL,
    N_END_CLASSES,
    N_START_STATES,
    StateTransitionHeldOutSet,
    StateTransitionInputs,
)
from python_models.statistical.models.state_transition import (
    build_state_transition_model,
)


def _tiny_inputs() -> StateTransitionInputs:
    start_state_labels = [
        f"{outs}_{base}" for outs in range(3) for base in range(8)
    ]
    end_class_labels = [*start_state_labels, INNING_END_LABEL]
    cell_labels = ["2019_NL|0_0", "2019_AL|0_0", "2019_NL|2_0"]
    cell_start_idx = np.array([0, 0, 16], dtype=np.int64)
    rng = np.random.default_rng(20260513)
    counts = rng.integers(0, 30, size=(len(cell_labels), N_END_CLASSES)).astype(
        np.int64
    )
    coords = {
        "source": ["__single__"],
        "start_state": list(start_state_labels),
        "end_class": list(end_class_labels),
        "end_class_nonref": list(end_class_labels[1:]),
        "cell": list(cell_labels),
    }
    return StateTransitionInputs(
        counts=counts,
        cell_start_idx=cell_start_idx,
        cell_labels=list(cell_labels),
        season_by_cell=[2019, 2019, 2019],
        league_by_cell=["NL", "AL", "NL"],
        start_state_by_cell=[0, 0, 16],
        start_state_labels=list(start_state_labels),
        end_class_labels=list(end_class_labels),
        outcome="end_state",
        coords=coords,
        held_out=StateTransitionHeldOutSet(
            counts=np.zeros((0, N_END_CLASSES), dtype=np.int64),
            cell_idx=np.zeros(0, dtype=np.int64),
            cell_start_idx=np.zeros(0, dtype=np.int64),
        ),
    )


def test_builds_model_with_cell_class_prob() -> None:
    inputs = _tiny_inputs()
    model = build_state_transition_model(inputs)
    assert isinstance(model, pm.Model)
    assert "cell_class_prob" in model.named_vars
    assert "alpha_trans" in model.named_vars
    assert "beta0" in model.named_vars
    assert "end_state_obs" in {rv.name for rv in model.observed_RVs}


def test_vocab_sizes_are_25_end_classes_and_24_start_states() -> None:
    inputs = _tiny_inputs()
    assert N_START_STATES == 24
    assert N_END_CLASSES == 25
    assert INNING_END_INDEX == N_START_STATES
    assert len(inputs.coords["start_state"]) == 24
    assert len(inputs.coords["end_class"]) == 25
    assert inputs.coords["end_class"][INNING_END_INDEX] == INNING_END_LABEL
    assert inputs.n_end_classes == 25
    assert inputs.n_start_states == 24


def test_cell_class_prob_dims_and_reference_class() -> None:
    inputs = _tiny_inputs()
    model = build_state_transition_model(inputs)
    dims = tuple(model.named_vars_to_dims.get("cell_class_prob", ()))
    assert dims == ("cell", "end_class")
    cell_dims = tuple(model.named_vars_to_dims.get("cell_logodds", ()))
    assert cell_dims == ("cell", "end_class_nonref")


def test_reference_class_softmax_sums_to_one() -> None:
    inputs = _tiny_inputs()
    n_cells = inputs.n_cells
    n_nonref = N_END_CLASSES - 1
    rng = np.random.default_rng(7)
    cell_logodds = rng.normal(0.0, 1.5, size=(n_cells, n_nonref))
    eta = np.concatenate([np.zeros((n_cells, 1)), cell_logodds], axis=1)
    eta = eta - eta.max(axis=1, keepdims=True)
    probs = np.exp(eta)
    probs = probs / probs.sum(axis=1, keepdims=True)
    row_sums = probs.sum(axis=1)
    assert probs.shape == (n_cells, N_END_CLASSES)
    assert np.allclose(row_sums, 1.0, atol=1e-6)


@pytest.mark.slow
def test_prior_predictive_runs() -> None:
    inputs = _tiny_inputs()
    model = build_state_transition_model(inputs)
    idata = pm.sample_prior_predictive(draws=2, model=model)
    assert "end_state_obs" in idata.prior_predictive
