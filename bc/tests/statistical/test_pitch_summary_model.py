"""Builder tests for the pitch-summary multinomial + coverage models."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import pymc as pm
import pytest
from pytensor.graph.basic import ancestors

from python_models.statistical.models._credit_data import FixedEffectDesign
from python_models.statistical.models._pitch_coverage_data import (
    CONTEXT_FIXED_EFFECT_COLUMNS,
    PitchCoverageHeldOutSet,
    PitchCoverageInputs,
    prepare_pitch_coverage_inputs,
)
from python_models.statistical.models._pitch_summary_data import (
    PitchSummaryHeldOutSet,
    PitchSummaryInputs,
)
from python_models.statistical.models.pitch_coverage import build_pitch_coverage_model
from python_models.statistical.models.pitch_summary import build_pitch_summary_model

N_CLASSES = 12

_COVERAGE_PARQUET = Path(
    "artifacts/statistical/datasets/model_input_pitch_summary/ps-v1/dataset.parquet"
)


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


def _tiny_coverage_inputs() -> PitchCoverageInputs:
    cell_labels = ["2023|AL", "2023|NL", "2024|AL"]
    cell_vocab = {c: i for i, c in enumerate(cell_labels)}
    scorer_labels = ["s0", "s1"]
    scorer_vocab = {s: i for i, s in enumerate(scorer_labels)}

    rows = [
        ("2023|AL", "s0", "strikeout", "pre_shift_era", 1),
        ("2023|NL", "s1", "single", "pre_shift_era", 0),
        ("2024|AL", "s0", "out_in_play", "full_shift_era", 1),
        ("2023|AL", "s1", "single", "full_shift_era", 0),
        ("2024|AL", "s1", "strikeout", "pre_shift_era", 1),
        ("2023|NL", "s0", "out_in_play", "full_shift_era", 0),
    ]
    cell_idx = np.array([cell_vocab[r[0]] for r in rows], dtype=np.int64)
    scorer_idx = np.array([scorer_vocab[r[1]] for r in rows], dtype=np.int64)
    y = np.array([r[4] for r in rows], dtype=np.int64)

    fixed_effects: dict[str, FixedEffectDesign] = {}
    fe_levels: dict[str, list[str]] = {}
    for pos, column in enumerate(CONTEXT_FIXED_EFFECT_COLUMNS):
        levels = sorted({r[2 + pos] for r in rows})
        vocab = {lvl: i for i, lvl in enumerate(levels)}
        codes = np.array([vocab[r[2 + pos]] for r in rows], dtype=np.int64)
        fixed_effects[column] = FixedEffectDesign(codes=codes, levels=tuple(levels))
        fe_levels[column] = levels

    coords: dict[str, list[str]] = {
        "source": ["__single__"],
        "season_league": list(cell_labels),
        "scorer": list(scorer_labels),
    }
    for column, levels in fe_levels.items():
        coords[f"{column}_levels"] = levels

    empty = np.zeros(0, dtype=np.int64)
    return PitchCoverageInputs(
        y=y,
        cell_idx=cell_idx,
        scorer_idx=scorer_idx,
        fixed_effects=fixed_effects,
        cell_labels=list(cell_labels),
        scorer_labels=list(scorer_labels),
        coords=coords,
        outcome="has_count",
        dimension="has_count",
        held_out=PitchCoverageHeldOutSet(
            y=empty,
            cell_idx=empty,
            scorer_idx=empty,
            fixed_effect_codes={c: empty for c in CONTEXT_FIXED_EFFECT_COLUMNS},
        ),
    )


def test_coverage_model_builds_with_context_terms() -> None:
    inputs = _tiny_coverage_inputs()
    model = build_pitch_coverage_model(inputs)
    assert isinstance(model, pm.Model)
    assert "count_observed" in {rv.name for rv in model.observed_RVs}
    assert "beta_cell" in model.named_vars
    assert "beta_scorer" in model.named_vars
    for column in CONTEXT_FIXED_EFFECT_COLUMNS:
        assert f"delta_{column}" in model.named_vars, column


def test_coverage_logit_predictor_includes_every_input_term() -> None:
    inputs = _tiny_coverage_inputs()
    model = build_pitch_coverage_model(inputs)
    observed = next(rv for rv in model.observed_RVs if rv.name == "count_observed")
    contributors = {var.name for var in ancestors([observed]) if var.name}
    expected = {"alpha", "beta_cell", "beta_scorer"} | {
        f"delta_{c}" for c in CONTEXT_FIXED_EFFECT_COLUMNS
    }
    missing = expected - contributors
    assert not missing, f"coverage logit missing terms: {missing}"


def test_coverage_outcome_is_bernoulli_observed() -> None:
    inputs = _tiny_coverage_inputs()
    model = build_pitch_coverage_model(inputs)
    observed = next(rv for rv in model.observed_RVs if rv.name == "count_observed")
    assert observed.owner.op.name == "bernoulli"
    obs_values = model.rvs_to_values[observed].eval()
    assert set(np.unique(obs_values)).issubset({0, 1})


def test_coverage_single_level_fixed_effect_omitted() -> None:
    inputs = _tiny_coverage_inputs()
    degenerate = dict(inputs.fixed_effects)
    column = CONTEXT_FIXED_EFFECT_COLUMNS[0]
    degenerate[column] = FixedEffectDesign(
        codes=np.zeros_like(inputs.fixed_effects[column].codes),
        levels=("only",),
    )
    coords = dict(inputs.coords)
    coords[f"{column}_levels"] = ["only"]
    patched = inputs.model_copy(
        update={"fixed_effects": degenerate, "coords": coords}
    )
    model = build_pitch_coverage_model(patched)
    assert f"delta_{column}" not in model.named_vars


@pytest.mark.skipif(
    not _COVERAGE_PARQUET.exists(), reason="pitch-summary dataset artifact absent"
)
def test_coverage_prep_populates_inputs_from_real_parquet() -> None:
    inputs = prepare_pitch_coverage_inputs(_COVERAGE_PARQUET, smoke_limit=20_000)
    assert inputs.outcome == "has_count"
    assert inputs.n_events == 20_000
    assert inputs.n_cells == len(inputs.coords["season_league"])
    assert set(np.unique(inputs.y)).issubset({0, 1})
    assert 0.0 < float(inputs.y.mean()) < 1.0
    assert inputs.cell_idx.shape == inputs.y.shape
    assert inputs.scorer_idx.shape == inputs.y.shape
    assert int(inputs.cell_idx.max()) < inputs.n_cells
    assert int(inputs.scorer_idx.max()) < len(inputs.coords["scorer"])
    for column in CONTEXT_FIXED_EFFECT_COLUMNS:
        design = inputs.fixed_effects[column]
        assert design.codes.shape == inputs.y.shape
        assert int(design.codes.max()) < len(design.levels)
        assert list(design.levels) == inputs.coords[f"{column}_levels"]


@pytest.mark.skipif(
    not _COVERAGE_PARQUET.exists(), reason="pitch-summary dataset artifact absent"
)
def test_coverage_held_out_disjoint_and_unseen_levels_sentinel() -> None:
    inputs = prepare_pitch_coverage_inputs(_COVERAGE_PARQUET, smoke_limit=20_000)
    held = inputs.held_out
    assert held.n_events > 0
    assert set(np.unique(held.y)).issubset({0, 1})
    assert int(held.cell_idx.min()) >= -1
    assert int(held.cell_idx.max()) < inputs.n_cells
    assert int(held.scorer_idx.min()) >= -1
