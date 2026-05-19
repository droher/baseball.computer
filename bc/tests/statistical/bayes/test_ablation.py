"""Synthetic-posterior coverage for ``summarize_random_effect_shift``."""

# pyright: reportMissingTypeArgument=false, reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false

from __future__ import annotations

import numpy as np
import pytest

az = pytest.importorskip("arviz")


def _synth(means_zero: np.ndarray, means_shrunk: np.ndarray, var: str = "beta_scorer"):
    rng = np.random.default_rng(20260518)
    sd = 0.1
    coord_name = var.removeprefix("beta_")

    def build(means: np.ndarray):
        samples = rng.normal(
            loc=means, scale=sd, size=(2, 50, means.shape[0])
        ).astype(np.float64)
        return az.from_dict(
            posterior={var: samples},
            coords={
                coord_name: np.asarray(
                    [f"{coord_name}_{i}" for i in range(means.shape[0])]
                )
            },
            dims={var: [coord_name]},
        )

    return build(means_zero), build(means_shrunk)


def test_ablation_returns_decision_with_per_effect_rows() -> None:
    from python_models.statistical.bayes.ablation import (
        DEFAULT_THRESHOLD_SD,
        summarize_random_effect_shift,
    )

    zero, shrunk = _synth(
        means_zero=np.array([0.0, 0.1, 0.2, 0.3, 0.4]),
        means_shrunk=np.array([0.0, 0.1, 0.2, 0.3, 0.4]),
    )
    decision = summarize_random_effect_shift(zero, shrunk)
    assert decision.threshold_sd == DEFAULT_THRESHOLD_SD
    assert len(decision.per_effect) == 1
    assert decision.per_effect[0].effect == "beta_scorer"
    assert decision.per_effect[0].n_cells == 5
    assert decision.fraction_above_threshold < 0.5
    assert decision.decision == "gamma_dl_shrunk"


def test_ablation_switches_when_shrunk_shifts_majority() -> None:
    from python_models.statistical.bayes.ablation import summarize_random_effect_shift

    zero_means = np.zeros(10)
    shrunk_means = np.full(10, 2.0)
    zero, shrunk = _synth(zero_means, shrunk_means)
    decision = summarize_random_effect_shift(zero, shrunk, threshold_sd=0.25)
    assert decision.fraction_above_threshold > 0.5
    assert decision.decision == "gamma_dl_zero"
    assert decision.max_cell_shift_sd > 0.25


def test_ablation_handles_missing_effect_var() -> None:
    from python_models.statistical.bayes.ablation import summarize_random_effect_shift

    zero, shrunk = _synth(np.array([0.0, 0.1]), np.array([0.0, 0.1]), var="beta_scorer")
    decision = summarize_random_effect_shift(
        zero, shrunk, effect_vars=("beta_scorer", "beta_park")
    )
    assert len(decision.per_effect) == 1
    assert decision.per_effect[0].effect == "beta_scorer"
