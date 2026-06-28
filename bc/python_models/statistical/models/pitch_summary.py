"""Cell-grain Multinomial pitch-summary builder.

Models the count vector over the plate appearance's 12 final ball-strike
classes (balls 0-3 x strikes 0-2) per ``(result_family, season, league)`` cell
as ``Multinomial(n=cell_total, p=softmax(eta))``. Class 0 (``b0_s0``) is the
reference: its logit is pinned to zero, and the remaining 11 classes carry a
centered three-level hierarchy of log-odds against it — corpus ``beta0``, a
per-result-family ``result_logodds`` centered on ``beta0``, and a per-cell
``cell_logodds`` centered on its result-family mean. Pinning the reference
identifies the softmax without zero-sum constraints, so every level is fully
centered and mixes like the run-expectancy hierarchy. ``cell_class_prob =
softmax([0, cell_logodds])`` is the published final-count distribution and the
only cell-sized Deterministic.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import numpy as np
import pymc as pm
import pytensor.tensor as pt

from python_models.statistical.models._pitch_summary_data import PitchSummaryInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)


def build_pitch_summary_model(
    inputs: PitchSummaryInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the cell-grain Multinomial final-count model."""
    cfg = priors or BayesPriorConfig()
    counts = inputs.counts.astype(np.int64)
    cell_total = counts.sum(axis=1)
    n_cells = inputs.n_cells

    with pm.Model(coords=inputs.coords) as model:
        cell_result_idx = pm.Data("cell_result_idx", inputs.cell_result_idx)

        beta0 = pm.Normal("beta0", mu=0.0, sigma=cfg.alpha_scale, dims="class_nonref")
        sigma_result = pm.HalfNormal(
            "sigma_result", sigma=cfg.fixed_effect_scale_credit
        )
        result_logodds = pm.Normal(
            "result_logodds",
            mu=beta0,
            sigma=sigma_result,
            dims=("result_family", "class_nonref"),
        )
        sigma_cell = pm.HalfNormal("sigma_cell", sigma=cfg.sigma_cell_scale)
        cell_logodds = pm.Normal(
            "cell_logodds",
            mu=result_logodds[cell_result_idx],
            sigma=sigma_cell,
            dims=("cell", "class_nonref"),
        )

        ref_col = pt.zeros((n_cells, 1))
        eta = pt.concatenate([ref_col, cell_logodds], axis=1)
        cell_class_prob = pm.Deterministic(
            "cell_class_prob", pm.math.softmax(eta, axis=1), dims=("cell", "class")
        )

        _ = pm.Multinomial(
            "final_count_obs",
            n=cell_total,
            p=cell_class_prob,
            observed=counts,
        )

    _log.info(
        "build_pitch_summary_model cells=%d results=%d classes=%d events=%d",
        inputs.n_cells,
        len(inputs.coords["result_family"]),
        len(inputs.coords["class"]),
        inputs.n_events,
    )
    return model
