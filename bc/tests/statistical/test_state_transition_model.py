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
    _reachable_mask,
    _reference_by_start,
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
    reachable = _reachable_mask()
    rng = np.random.default_rng(20260513)
    counts = (
        rng.integers(1, 30, size=(len(cell_labels), N_END_CLASSES))
        * reachable[cell_start_idx]
    ).astype(np.int64)
    ref_class_by_start = _reference_by_start(counts, cell_start_idx, reachable)
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
        reachable_mask=reachable,
        ref_class_by_start=ref_class_by_start,
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


def test_builds_model_with_expected_vars() -> None:
    inputs = _tiny_inputs()
    model = build_state_transition_model(inputs)
    assert isinstance(model, pm.Model)
    assert "cell_class_prob" in model.named_vars
    assert "start_state_prob" in model.named_vars
    assert "alpha_trans" in model.named_vars
    assert "z_cell" in model.named_vars
    assert "sigma_cell" in model.named_vars
    assert "beta0" not in model.named_vars
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


def test_cell_class_prob_dims() -> None:
    inputs = _tiny_inputs()
    model = build_state_transition_model(inputs)
    dims = tuple(model.named_vars_to_dims.get("cell_class_prob", ()))
    assert dims == ("cell", "end_class")
    start_dims = tuple(model.named_vars_to_dims.get("start_state_prob", ()))
    assert start_dims == ("start_state", "end_class")


def test_softmax_sums_to_one_and_masks_unreachable() -> None:
    inputs = _tiny_inputs()
    model = build_state_transition_model(inputs)
    prob = np.asarray(
        pm.draw(model["cell_class_prob"], draws=1, random_seed=0), dtype=np.float64
    )
    assert prob.shape == (inputs.n_cells, N_END_CLASSES)
    assert np.allclose(prob.sum(axis=1), 1.0, atol=1e-9)
    cell_reachable = inputs.reachable_mask[inputs.cell_start_idx]
    assert float(prob[~cell_reachable].max()) < 1e-10
    assert float(prob[cell_reachable].min()) > 0.0


def test_observed_count_in_unreachable_entry_raises() -> None:
    inputs = _tiny_inputs()
    bad_counts = inputs.counts.copy()
    bad_counts[2, 0] = 5
    bad = inputs.model_copy(update={"counts": bad_counts})
    with pytest.raises(AssertionError, match="unreachable"):
        build_state_transition_model(bad)


@pytest.mark.slow
def test_prior_predictive_runs() -> None:
    inputs = _tiny_inputs()
    model = build_state_transition_model(inputs)
    idata = pm.sample_prior_predictive(draws=2, model=model)
    assert "end_state_obs" in idata.prior_predictive
