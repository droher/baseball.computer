"""Cell-grain Multinomial base-out transition builder.

Models the count vector over the 25 end classes (24 base-out states plus an
inning-end sentinel) per ``(season, league, start_state)`` cell as
``Multinomial(n=cell_total, p=softmax(eta))``. End class 0 is the reference: its
logit is pinned to zero, and the remaining 24 classes carry a centered
three-level hierarchy of log-odds against it — a corpus ``beta0``, a
per-start-state ``alpha_trans`` centered on ``beta0``, and a per-cell
``cell_logodds`` centered on its start-state mean. Pinning the reference
identifies the softmax without zero-sum constraints, mirroring the pitch-summary
parameterization. ``cell_class_prob = softmax([0, cell_logodds])`` sums to one per
cell by construction — the spec's hard Markov invariant — and is the only
cell-sized Deterministic.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import numpy as np
import pymc as pm
import pytensor.tensor as pt

from python_models.statistical.models._state_transition_data import (
    StateTransitionInputs,
)
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)

BETA0_SCALE: float = 1.5
SIGMA_START_SCALE: float = 1.0
SIGMA_CELL_SCALE: float = 0.5


def build_state_transition_model(
    inputs: StateTransitionInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the cell-grain Multinomial base-out transition model."""
    _ = priors
    counts = inputs.counts.astype(np.int64)
    cell_total = counts.sum(axis=1)
    n_cells = inputs.n_cells

    with pm.Model(coords=inputs.coords) as model:
        cell_start_idx = pm.Data("cell_start_idx", inputs.cell_start_idx)

        beta0 = pm.Normal(
            "beta0", mu=0.0, sigma=BETA0_SCALE, dims="end_class_nonref"
        )
        sigma_start = pm.HalfNormal("sigma_start", sigma=SIGMA_START_SCALE)
        alpha_trans = pm.Normal(
            "alpha_trans",
            mu=beta0,
            sigma=sigma_start,
            dims=("start_state", "end_class_nonref"),
        )
        sigma_cell = pm.HalfNormal("sigma_cell", sigma=SIGMA_CELL_SCALE)
        cell_logodds = pm.Normal(
            "cell_logodds",
            mu=alpha_trans[cell_start_idx],
            sigma=sigma_cell,
            dims=("cell", "end_class_nonref"),
        )

        ref_col = pt.zeros((n_cells, 1))
        eta = pt.concatenate([ref_col, cell_logodds], axis=1)
        cell_class_prob = pm.Deterministic(
            "cell_class_prob",
            pm.math.softmax(eta, axis=1),
            dims=("cell", "end_class"),
        )

        _ = pm.Multinomial(
            "end_state_obs",
            n=cell_total,
            p=cell_class_prob,
            observed=counts,
        )

    _log.info(
        "build_state_transition_model cells=%d start_states=%d end_classes=%d events=%d",
        inputs.n_cells,
        len(inputs.coords["start_state"]),
        len(inputs.coords["end_class"]),
        inputs.n_events,
    )
    return model
