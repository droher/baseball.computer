"""Builder tests for the pitch-summary multinomial model."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

import numpy as np
import pymc as pm
import pytest

from python_models.statistical.models._pitch_summary_data import (
    PitchSummaryHeldOutSet,
    PitchSummaryInputs,
)
from python_models.statistical.models.pitch_summary import build_pitch_summary_model

N_CLASSES = 12


def _class_axes() -> tuple[list[str], list[int], list[int]]:
    labels: list[str] = []
    balls: list[int] = []
    strikes: list[int] = []
    for b in range(4):
        for s in range(3):
            labels.append(f"b{b}_s{s}")
            balls.append(b)
            strikes.append(s)
    return labels, balls, strikes


def _tiny_inputs() -> PitchSummaryInputs:
    cell_labels = [
        "out_in_play|2023|AL",
        "strikeout|2023|AL",
        "strikeout|2023|NL",
    ]
    result_by_cell = ["out_in_play", "strikeout", "strikeout"]
    season_by_cell = [2023, 2023, 2023]
    league_by_cell = ["AL", "AL", "NL"]
    result_family_labels = ["out_in_play", "strikeout"]
    result_to_idx = {r: i for i, r in enumerate(result_family_labels)}
    cell_result_idx = np.array(
        [result_to_idx[r] for r in result_by_cell], dtype=np.int64
    )
    class_labels, balls_by_class, strikes_by_class = _class_axes()

    rng = np.random.default_rng(20260513)
    counts = rng.integers(2, 9, size=(len(cell_labels), N_CLASSES)).astype(np.int64)

    return PitchSummaryInputs(
        counts=counts,
        cell_result_idx=cell_result_idx,
        cell_labels=list(cell_labels),
        result_by_cell=result_by_cell,
        season_by_cell=season_by_cell,
        league_by_cell=league_by_cell,
        class_labels=class_labels,
        balls_by_class=balls_by_class,
        strikes_by_class=strikes_by_class,
        result_family_labels=result_family_labels,
        outcome="final_count",
        coords={
            "source": ["__single__"],
            "class": list(class_labels),
            "class_nonref": list(class_labels[1:]),
            "result_family": list(result_family_labels),
            "cell": list(cell_labels),
        },
        held_out=PitchSummaryHeldOutSet(
            counts=np.zeros((0, N_CLASSES), dtype=np.int64),
            cell_idx=np.zeros(0, dtype=np.int64),
            cell_result_idx=np.zeros(0, dtype=np.int64),
        ),
    )


def test_builds_model_with_cell_class_prob() -> None:
    inputs = _tiny_inputs()
    model = build_pitch_summary_model(inputs)
    assert isinstance(model, pm.Model)
    assert "cell_class_prob" in model.named_vars
    assert "beta0" in model.named_vars
    assert "result_logodds" in model.named_vars
    assert "cell_logodds" in model.named_vars
    assert "final_count_obs" in {rv.name for rv in model.observed_RVs}


def test_cell_class_prob_is_the_only_cell_sized_deterministic() -> None:
    inputs = _tiny_inputs()
    model = build_pitch_summary_model(inputs)

    n_cell = inputs.n_cells
    assert n_cell == len(inputs.coords["cell"])

    prob_dims = tuple(model.named_vars_to_dims.get("cell_class_prob", ()))
    assert prob_dims == ("cell", "class")

    coord_lengths = {name: len(vals) for name, vals in inputs.coords.items()}
    cell_sized = [
        det.name
        for det in model.deterministics
        if n_cell
        in tuple(
            coord_lengths.get(d, -1) for d in model.named_vars_to_dims.get(det.name, ())
        )
    ]
    assert cell_sized == ["cell_class_prob"]

    for det in model.deterministics:
        dims = tuple(model.named_vars_to_dims.get(det.name, ()))
        assert dims, f"{det.name} carries no named dims"


@pytest.mark.slow
def test_prior_predictive_runs() -> None:
    inputs = _tiny_inputs()
    model = build_pitch_summary_model(inputs)
    idata = pm.sample_prior_predictive(draws=2, model=model)
    assert "final_count_obs" in idata.prior_predictive
