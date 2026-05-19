"""Ablation selector for gamma_dl_zero vs gamma_dl_shrunk posteriors.

Per doc-03 §66: compare posterior random-effect cell-mean shifts between
the zero and shrunk fits. If most (season|scorer|source) cells move
more than ``threshold`` SD relative to the *zero-fit* (publication-tier)
posterior SD, prefer ``gamma_dl_zero``; otherwise prefer
``gamma_dl_shrunk``. Per-effect decisions combine via majority vote so a
single high-cardinality effect (e.g. scorer) doesn't drown out
low-cardinality ones (e.g. source).

PR2 ships the comparison utility only. Smoke posteriors are tiny so the
returned decision is informational; PR3 picks the published tier on the
full-fit posteriors.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAttributeAccessIssue=false, reportMissingTypeArgument=false, reportUnknownParameterType=false, reportOperatorIssue=false

from __future__ import annotations

import logging
from typing import ClassVar, Literal

import arviz as az
import numpy as np
from pydantic import BaseModel, ConfigDict, Field

_log = logging.getLogger(__name__)

DEFAULT_THRESHOLD_SD: float = 0.25
DEFAULT_RANDOM_EFFECT_VARS: tuple[str, ...] = (
    "beta_season",
    "beta_scorer",
    "beta_source",
)


class AblationEffectShift(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    effect: str
    n_cells: int
    max_cell_shift_sd: float
    fraction_above_threshold: float


class AblationDecision(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    threshold_sd: float = Field(default=DEFAULT_THRESHOLD_SD)
    max_cell_shift_sd: float
    fraction_above_threshold: float
    decision: Literal["gamma_dl_zero", "gamma_dl_shrunk"]
    per_effect: tuple[AblationEffectShift, ...]


def _effect_cell_means_and_sd(
    idata: az.InferenceData, var: str
) -> tuple[np.ndarray, np.ndarray]:
    arr = idata.posterior[var]
    means = np.asarray(arr.mean(dim=("chain", "draw")).values, dtype=np.float64).ravel()
    sds = np.asarray(arr.std(dim=("chain", "draw")).values, dtype=np.float64).ravel()
    return means, sds


def summarize_random_effect_shift(
    zero_idata: az.InferenceData,
    shrunk_idata: az.InferenceData,
    *,
    threshold_sd: float = DEFAULT_THRESHOLD_SD,
    effect_vars: tuple[str, ...] = DEFAULT_RANDOM_EFFECT_VARS,
) -> AblationDecision:
    """Compare random-effect cell means between zero and shrunk posteriors.

    For each named effect, compute per-cell shift in SD units
    ``|mean_shrunk - mean_zero| / zero_sd`` (the zero-fit posterior SD is
    the publication-tier reference per doc-03 §66; floored at ``1e-9`` to
    avoid divide-by-zero). The per-effect summary records the fraction
    of cells exceeding ``threshold_sd``. The overall decision is by
    majority vote across effects: if more than half the effects have a
    majority of cells above threshold, publish ``gamma_dl_zero``; else
    publish ``gamma_dl_shrunk``.
    """
    per_effect: list[AblationEffectShift] = []
    effects_above_majority = 0
    overall_max = 0.0
    weighted_frac_sum = 0.0
    for var in effect_vars:
        if var not in zero_idata.posterior or var not in shrunk_idata.posterior:
            _log.info(
                "ablation: skipping %s (missing in zero or shrunk posterior)", var
            )
            continue
        zero_mean, zero_sd = _effect_cell_means_and_sd(zero_idata, var)
        shrunk_mean, _shrunk_sd = _effect_cell_means_and_sd(shrunk_idata, var)
        if zero_mean.shape != shrunk_mean.shape:
            _log.warning(
                "ablation: %s cell counts differ zero=%s shrunk=%s",
                var,
                zero_mean.shape,
                shrunk_mean.shape,
            )
            continue
        denom = np.maximum(zero_sd, 1e-9)
        shift = np.abs(shrunk_mean - zero_mean) / denom
        n_cells = int(shift.shape[0])
        if n_cells == 0:
            continue
        above = float(np.mean(shift > threshold_sd))
        cell_max = float(np.max(shift))
        per_effect.append(
            AblationEffectShift(
                effect=var,
                n_cells=n_cells,
                max_cell_shift_sd=cell_max,
                fraction_above_threshold=above,
            )
        )
        if above > 0.5:
            effects_above_majority += 1
        if cell_max > overall_max:
            overall_max = cell_max
        weighted_frac_sum += above

    if not per_effect:
        return AblationDecision(
            threshold_sd=threshold_sd,
            max_cell_shift_sd=0.0,
            fraction_above_threshold=0.0,
            decision="gamma_dl_shrunk",
            per_effect=tuple(per_effect),
        )

    overall_frac = weighted_frac_sum / len(per_effect)
    majority_of_effects = effects_above_majority > len(per_effect) / 2
    decision: Literal["gamma_dl_zero", "gamma_dl_shrunk"] = (
        "gamma_dl_zero" if majority_of_effects else "gamma_dl_shrunk"
    )
    return AblationDecision(
        threshold_sd=threshold_sd,
        max_cell_shift_sd=overall_max,
        fraction_above_threshold=overall_frac,
        decision=decision,
        per_effect=tuple(per_effect),
    )
