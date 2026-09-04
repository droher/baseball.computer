"""Posterior-summary export invariants shared by every aggregate export helper.

Each helper reduces a synthetic posterior to ``mean`` / ``sd`` / 94% HDI
columns; on every published row the HDI must contain the mean and the sd
must be non-negative. The held-out evaluators for the two summary
multinomials get a shape + self-consistency check on the same fixtures.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false, reportPrivateUsage=false

from __future__ import annotations

from pathlib import Path

import arviz as az
import numpy as np
import polars as pl
import pytest
from scipy.special import softmax

from python_models.statistical.bayes.training import (
    _evaluate_assist_count_held_out,
    _evaluate_state_transition_held_out,
    _export_assist_count_summary,
    _export_park_factor_summary,
    _export_pitch_summary_summary,
    _export_run_expectancy_summary,
    _export_state_transition_summary,
)
from python_models.statistical.linear_weights_estimated import (
    propagate_linear_weights_draws,
)
from python_models.statistical.models._assist_count_data import (
    ASSIST_COUNT_CLASSES,
    AssistCountHeldOutSet,
    AssistCountInputs,
)
from python_models.statistical.models._state_transition_data import (
    N_END_CLASSES,
    N_START_STATES,
    StateTransitionHeldOutSet,
    StateTransitionInputs,
)
from python_models.statistical.models.pitch_summary import MASK_LOGIT
from tests.statistical.coverage.test_linear_weights_estimated import (
    _re_draws_many,
    _transition_counts,
)
from tests.statistical.test_park_factor_export import _inputs as _park_factor_inputs
from tests.statistical.test_pitch_summary_export import _inputs as _pitch_summary_inputs
from tests.statistical.test_run_expectancy_export import (
    CELL_LABELS as RE_CELL_LABELS,
)
from tests.statistical.test_run_expectancy_export import (
    STATE_LABELS as RE_STATE_LABELS,
)
from tests.statistical.test_run_expectancy_export import _inputs as _run_expectancy_inputs
from tests.statistical.test_state_transition_model import (
    _tiny_inputs as _state_transition_inputs,
)

N_CHAIN = 2
N_DRAW = 300


def _f(metrics: dict[str, object], key: str) -> float:
    value = metrics[key]
    assert isinstance(value, (int, float))
    return float(value)


def _assert_hdi_contains_mean(
    df: pl.DataFrame, *, mean: str, sd: str, lower: str, upper: str
) -> None:
    assert df.height > 0
    mean_v = df.get_column(mean).to_numpy()
    sd_v = df.get_column(sd).to_numpy()
    lower_v = df.get_column(lower).to_numpy()
    upper_v = df.get_column(upper).to_numpy()
    assert np.all(np.isfinite(mean_v)) and np.all(np.isfinite(sd_v))
    assert np.all(sd_v >= 0.0)
    assert np.all(lower_v <= upper_v)
    assert np.all(lower_v <= mean_v), (lower_v - mean_v).max()
    assert np.all(mean_v <= upper_v), (mean_v - upper_v).max()


def _masked_softmax(
    rng: np.random.Generator, reachable_rows: np.ndarray, *, scale: float = 1.0
) -> np.ndarray:
    n_rows, n_class = reachable_rows.shape
    logits = rng.normal(scale=scale, size=(N_CHAIN, N_DRAW, n_rows, n_class))
    logits = np.where(reachable_rows[None, None, :, :], logits, MASK_LOGIT)
    return softmax(logits, axis=-1)


def test_run_expectancy_summary_hdi_contains_mean(tmp_path: Path) -> None:
    inputs = _run_expectancy_inputs()
    rng = np.random.default_rng(1)
    n_cell = len(RE_CELL_LABELS)
    idata = az.from_dict(
        posterior={
            "re_value": rng.gamma(2.0, 0.5, size=(N_CHAIN, N_DRAW, n_cell)),
        },
        coords={"cell": list(RE_CELL_LABELS), "state": list(RE_STATE_LABELS)},
        dims={"re_value": ["cell"]},
    )
    target = tmp_path / "run_expectancy_summary.parquet"
    _export_run_expectancy_summary(idata, inputs, target_path=target)
    df = pl.read_parquet(target)
    assert df.height == n_cell
    _assert_hdi_contains_mean(
        df,
        mean="re_value_mean",
        sd="re_value_sd",
        lower="re_value_hdi_lower",
        upper="re_value_hdi_upper",
    )


def _state_transition_idata(
    inputs: StateTransitionInputs, *, seed: int = 2, tie_to_start: bool = False
) -> az.InferenceData:
    rng = np.random.default_rng(seed)
    start_prob = _masked_softmax(rng, inputs.reachable_mask)
    if tie_to_start:
        cell_prob = start_prob[:, :, inputs.cell_start_idx, :]
    else:
        cell_prob = _masked_softmax(rng, inputs.reachable_mask[inputs.cell_start_idx])
    return az.from_dict(
        posterior={"cell_class_prob": cell_prob, "start_state_prob": start_prob},
        coords={
            "cell": list(inputs.cell_labels),
            "start_state": list(inputs.start_state_labels),
            "end_class": list(inputs.end_class_labels),
        },
        dims={
            "cell_class_prob": ["cell", "end_class"],
            "start_state_prob": ["start_state", "end_class"],
        },
    )


def _with_state_transition_held_out(
    inputs: StateTransitionInputs, *, seed: int = 5
) -> StateTransitionInputs:
    rng = np.random.default_rng(seed)
    cell_idx = np.array([0, 2], dtype=np.int64)
    cell_start_idx = inputs.cell_start_idx[cell_idx]
    counts = (
        rng.integers(1, 20, size=(cell_idx.shape[0], N_END_CLASSES))
        * inputs.reachable_mask[cell_start_idx]
    ).astype(np.int64)
    return inputs.model_copy(
        update={
            "held_out": StateTransitionHeldOutSet(
                counts=counts, cell_idx=cell_idx, cell_start_idx=cell_start_idx
            )
        }
    )


def test_state_transition_summary_shape_and_hdi_contains_mean(tmp_path: Path) -> None:
    inputs = _state_transition_inputs()
    idata = _state_transition_idata(inputs)
    target = tmp_path / "state_transition_summary.parquet"
    _export_state_transition_summary(idata, inputs, target_path=target)
    df = pl.read_parquet(target)
    assert df.height == inputs.n_cells * N_END_CLASSES
    assert df.select("start_state", "season", "league", "end_class").n_unique() == df.height
    assert N_START_STATES == len(inputs.start_state_labels)
    per_cell = df.group_by("start_state", "season", "league").agg(
        pl.col("prob_mean").sum().alias("total")
    )
    np.testing.assert_allclose(per_cell.get_column("total").to_numpy(), 1.0, atol=1e-9)
    _assert_hdi_contains_mean(
        df, mean="prob_mean", sd="prob_sd", lower="prob_hdi_lower", upper="prob_hdi_upper"
    )
    prob = np.asarray(idata.posterior["cell_class_prob"].values, dtype=np.float64)
    np.testing.assert_allclose(
        df.get_column("prob_mean").to_numpy(), prob.mean(axis=(0, 1)).reshape(-1)
    )


def test_state_transition_held_out_keys_and_self_consistency() -> None:
    inputs = _with_state_transition_held_out(_state_transition_inputs())
    idata = _state_transition_idata(inputs)
    metrics = _evaluate_state_transition_held_out(inputs, idata)
    assert metrics["n_cells"] == inputs.held_out.n_cells
    assert metrics["n_events"] == int(inputs.held_out.counts.sum())
    assert _f(metrics, "loglik_lift") == pytest.approx(
        _f(metrics, "loglik_per_event") - _f(metrics, "loglik_per_event_baseline")
    )
    assert _f(metrics, "tv_improvement") == pytest.approx(
        _f(metrics, "tv_distance_baseline") - _f(metrics, "tv_distance")
    )
    for key in ("tv_distance", "tv_distance_baseline"):
        assert 0.0 <= _f(metrics, key) <= 1.0
    assert _f(metrics, "loglik_per_event") <= 0.0

    tied = _evaluate_state_transition_held_out(
        inputs, _state_transition_idata(inputs, tie_to_start=True)
    )
    assert tied["loglik_lift"] == 0.0
    assert tied["tv_improvement"] == 0.0


def test_state_transition_held_out_empty_short_circuits() -> None:
    inputs = _state_transition_inputs()
    assert inputs.held_out.n_cells == 0
    assert _evaluate_state_transition_held_out(
        inputs, _state_transition_idata(inputs)
    ) == {"n_cells": 0}


def test_pitch_summary_summary_hdi_contains_mean_on_reachable_rows(
    tmp_path: Path,
) -> None:
    inputs = _pitch_summary_inputs()
    rng = np.random.default_rng(3)
    idata = az.from_dict(
        posterior={
            "cell_class_prob": _masked_softmax(rng, inputs.cell_reachable_mask),
            "result_class_prob": _masked_softmax(rng, inputs.reachable_mask),
        },
        coords={
            "cell": list(inputs.cell_labels),
            "class": list(inputs.class_labels),
            "result_family": list(inputs.result_family_labels),
        },
        dims={
            "cell_class_prob": ["cell", "class"],
            "result_class_prob": ["result_family", "class"],
        },
    )
    target = tmp_path / "pitch_summary_summary.parquet"
    _export_pitch_summary_summary(idata, inputs, target_path=target)
    df = pl.read_parquet(target)
    reachable = inputs.cell_reachable_mask.reshape(-1)
    assert df.height == reachable.shape[0]
    masked = df.filter(pl.Series(~reachable))
    assert masked.height > 0
    for column in ("prob_mean", "prob_sd", "prob_hdi_lower", "prob_hdi_upper"):
        assert masked.get_column(column).to_list() == [0.0] * masked.height
    _assert_hdi_contains_mean(
        df.filter(pl.Series(reachable)),
        mean="prob_mean",
        sd="prob_sd",
        lower="prob_hdi_lower",
        upper="prob_hdi_upper",
    )


AC_CELLS = ["out_in_play||0|0", "single||1|2", "out_in_play||3|1"]
AC_EVENT_CLASSES = ["out_in_play", "single"]
AC_CELL_EVENT_CLASS = np.array([0, 1, 0], dtype=np.int64)


def _assist_count_inputs(*, held_out_cells: int) -> AssistCountInputs:
    rng = np.random.default_rng(4)
    n_class = len(ASSIST_COUNT_CLASSES)
    counts = rng.integers(5, 40, size=(len(AC_CELLS), n_class)).astype(np.int64)
    held_cell_idx = np.arange(held_out_cells, dtype=np.int64)
    held = AssistCountHeldOutSet(
        counts=rng.integers(0, 10, size=(held_out_cells, n_class)).astype(np.int64),
        cell_idx=held_cell_idx,
        cell_event_class_idx=AC_CELL_EVENT_CLASS[held_cell_idx],
    )
    return AssistCountInputs(
        counts=counts,
        cell_event_class_idx=AC_CELL_EVENT_CLASS,
        coords={
            "source": ["__single__"],
            "cell": list(AC_CELLS),
            "event_class": list(AC_EVENT_CLASSES),
            "assist_count_class": list(ASSIST_COUNT_CLASSES),
            "assist_count_nonref": list(ASSIST_COUNT_CLASSES[1:]),
        },
        held_out=held,
    )


def _assist_count_idata(
    inputs: AssistCountInputs, *, seed: int = 6, tie_to_event_class: bool = False
) -> az.InferenceData:
    rng = np.random.default_rng(seed)
    n_class = inputs.n_classes
    n_event_class = len(inputs.coords["event_class"])
    event_class_prob = softmax(
        rng.normal(size=(N_CHAIN, N_DRAW, n_event_class, n_class)), axis=-1
    )
    if tie_to_event_class:
        cell_prob = event_class_prob[:, :, inputs.cell_event_class_idx, :]
    else:
        cell_prob = softmax(
            rng.normal(size=(N_CHAIN, N_DRAW, inputs.n_cells, n_class)), axis=-1
        )
    return az.from_dict(
        posterior={
            "cell_class_prob": cell_prob,
            "event_class_count_prob": event_class_prob,
        },
        coords={
            "cell": list(inputs.coords["cell"]),
            "event_class": list(inputs.coords["event_class"]),
            "assist_count_class": list(inputs.coords["assist_count_class"]),
        },
        dims={
            "cell_class_prob": ["cell", "assist_count_class"],
            "event_class_count_prob": ["event_class", "assist_count_class"],
        },
    )


def test_assist_count_summary_shape_and_hdi_contains_mean(tmp_path: Path) -> None:
    inputs = _assist_count_inputs(held_out_cells=0)
    idata = _assist_count_idata(inputs)
    target = tmp_path / "assist_count_summary.parquet"
    _export_assist_count_summary(idata, inputs, target_path=target)
    df = pl.read_parquet(target)
    assert df.height == inputs.n_cells * inputs.n_classes
    grain = ["result_family", "base_state_start", "outs_start"]
    assert df.select([*grain, "assist_count_class"]).n_unique() == df.height
    rebuilt = [
        f"{row['result_family']}||{row['base_state_start']}|{row['outs_start']}"
        for row in df.unique(subset=grain, maintain_order=True).iter_rows(named=True)
    ]
    assert rebuilt == AC_CELLS
    per_cell = df.group_by(grain).agg(pl.col("prob_mean").sum().alias("total"))
    np.testing.assert_allclose(per_cell.get_column("total").to_numpy(), 1.0, atol=1e-9)
    _assert_hdi_contains_mean(
        df, mean="prob_mean", sd="prob_sd", lower="prob_hdi_lower", upper="prob_hdi_upper"
    )


def test_assist_count_held_out_keys_and_self_consistency() -> None:
    inputs = _assist_count_inputs(held_out_cells=2)
    metrics = _evaluate_assist_count_held_out(inputs, _assist_count_idata(inputs))
    assert metrics["n_cells"] == inputs.held_out.n_cells
    assert metrics["n_events"] == int(inputs.held_out.counts.sum())
    assert _f(metrics, "loglik_lift") == pytest.approx(
        _f(metrics, "loglik_per_event") - _f(metrics, "loglik_per_event_baseline")
    )
    assert _f(metrics, "tv_improvement") == pytest.approx(
        _f(metrics, "tv_distance_baseline") - _f(metrics, "tv_distance")
    )
    for key in ("tv_distance", "tv_distance_baseline"):
        assert 0.0 <= _f(metrics, key) <= 1.0

    tied = _evaluate_assist_count_held_out(
        inputs, _assist_count_idata(inputs, tie_to_event_class=True)
    )
    assert tied["loglik_lift"] == 0.0
    assert tied["tv_improvement"] == 0.0


def test_assist_count_held_out_empty_short_circuits() -> None:
    inputs = _assist_count_inputs(held_out_cells=0)
    assert _evaluate_assist_count_held_out(inputs, _assist_count_idata(inputs)) == {
        "n_cells": 0
    }


def test_park_factor_summary_hdi_contains_mean(tmp_path: Path) -> None:
    inputs = _park_factor_inputs(held_out_games=0)
    rng = np.random.default_rng(8)
    cells = list(inputs.park_season_league_labels)
    idata = az.from_dict(
        posterior={
            "theta_park": rng.normal(scale=0.2, size=(N_CHAIN, N_DRAW, len(cells))),
        },
        coords={"park_season_league": cells},
        dims={"theta_park": ["park_season_league"]},
    )
    target = tmp_path / "park_factor_summary.parquet"
    _export_park_factor_summary(idata, inputs, target_path=target)
    df = pl.read_parquet(target)
    assert df.height == len(cells)
    _assert_hdi_contains_mean(
        df,
        mean="theta_mean",
        sd="theta_sd",
        lower="theta_hdi_lower",
        upper="theta_hdi_upper",
    )
    np.testing.assert_allclose(
        df.get_column("park_factor_mean").to_numpy(),
        np.exp(df.get_column("theta_mean").to_numpy()),
    )


@pytest.mark.parametrize("dirichlet_alpha", [None, 0.5])
def test_linear_weights_hdi_contains_mean(dirichlet_alpha: float | None) -> None:
    out = propagate_linear_weights_draws(
        _transition_counts(),
        _re_draws_many(N_DRAW, seed=9),
        dirichlet_alpha=dirichlet_alpha,
        min_events_per_play=0,
    )
    _assert_hdi_contains_mean(
        out,
        mean="run_value_mean",
        sd="run_value_sd",
        lower="run_value_hdi_lower",
        upper="run_value_hdi_upper",
    )
