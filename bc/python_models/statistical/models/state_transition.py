"""Cell-grain Multinomial base-out transition builder.

Models the count vector over the 25 end classes (24 base-out states plus an
inning-end sentinel) per ``(season, league, start_state)`` cell as
``Multinomial(n=cell_total, p=softmax(eta))``.

Base-out transitions obey a hard structural constraint: outs never decrease
within an event, so the reachable end classes for a start state are exactly the
base states at out counts ``>= start_outs`` plus the inning-end sentinel.
Structurally unreachable ``(start_state, end_class)`` entries are pinned to a
large negative logit so their softmax share is ~0, and only reachable entries
carry free parameters. Each start state pins its own reference — the modal
reachable end class, guaranteed reachable and well-observed — to a zero logit,
which identifies the per-start softmax without a zero-sum constraint. A universal
reference is impossible because class 0 (0 outs, bases empty) is unreachable from
any start with outs >= 1.

The remaining reachable non-reference logits carry a two-level hierarchy: a
per-``(start_state, end_class)`` base log-odds ``alpha_trans`` against that
start's reference, and a non-centered per-cell deviation ``z_cell * sigma_cell``
that pools cells of the same start state (different seasons/leagues) toward the
start-state mean. There is no corpus level across start states: their transition
distributions live in different reachable spaces against different references, so
pooling them is meaningless. Both levels are stored flat over the reachable
non-reference pairs so no parameter is spent on a structural zero.

``cell_class_prob = softmax(eta)`` sums to one per cell by construction — the
spec's hard Markov invariant — and is the only cell-sized Deterministic.
``start_state_prob`` is the per-start marginal transition distribution used as
the held-out baseline.
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

ALPHA_SCALE: float = 3.0
SIGMA_CELL_SCALE: float = 0.5
MASK_LOGIT: float = -30.0


def build_state_transition_model(
    inputs: StateTransitionInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the reachability-masked cell-grain transition model."""
    _ = priors
    counts = inputs.counts.astype(np.int64)
    cell_total = counts.sum(axis=1)
    n_cells = inputs.n_cells
    n_end = len(inputs.end_class_labels)
    n_start = len(inputs.start_state_labels)

    reachable = inputs.reachable_mask
    ref_by_start = inputs.ref_class_by_start
    cell_start = inputs.cell_start_idx

    observed_unreachable = int(counts[~reachable[cell_start]].sum())
    if observed_unreachable:
        raise AssertionError(
            f"{observed_unreachable} observed events land in structurally "
            "unreachable (start_state, end_class) entries"
        )

    alpha_pairs = [
        (s, k)
        for s in range(n_start)
        for k in range(n_end)
        if reachable[s, k] and k != ref_by_start[s]
    ]
    alpha_lookup = {pair: i for i, pair in enumerate(alpha_pairs)}
    alpha_start = np.array([s for s, _ in alpha_pairs], dtype=np.int64)
    alpha_class = np.array([k for _, k in alpha_pairs], dtype=np.int64)
    n_alpha = len(alpha_pairs)

    cell_rows: list[int] = []
    cell_cols: list[int] = []
    cell_alpha: list[int] = []
    for c in range(n_cells):
        s = int(cell_start[c])
        for k in range(n_end):
            if reachable[s, k] and k != ref_by_start[s]:
                cell_rows.append(c)
                cell_cols.append(k)
                cell_alpha.append(alpha_lookup[(s, k)])
    cell_row_idx = np.array(cell_rows, dtype=np.int64)
    cell_col_idx = np.array(cell_cols, dtype=np.int64)
    cell_alpha_idx = np.array(cell_alpha, dtype=np.int64)
    n_pairs = len(cell_rows)

    start_rows = np.arange(n_start)
    cell_ref_col = ref_by_start[cell_start]
    cell_ref_rows = np.arange(n_cells)

    with pm.Model(coords=inputs.coords) as model:
        alpha_trans = pm.Normal("alpha_trans", mu=0.0, sigma=ALPHA_SCALE, shape=n_alpha)
        sigma_cell = pm.HalfNormal("sigma_cell", sigma=SIGMA_CELL_SCALE)
        z_cell = pm.Normal("z_cell", mu=0.0, sigma=1.0, shape=n_pairs)
        cell_logodds = pt.cast(alpha_trans[cell_alpha_idx] + z_cell * sigma_cell, "float64")

        eta_start = pt.full((n_start, n_end), MASK_LOGIT, dtype="float64")
        eta_start = pt.set_subtensor(eta_start[start_rows, ref_by_start], 0.0)
        eta_start = pt.set_subtensor(
            eta_start[alpha_start, alpha_class], pt.cast(alpha_trans, "float64")
        )
        pm.Deterministic(
            "start_state_prob",
            pm.math.softmax(eta_start, axis=1),
            dims=("start_state", "end_class"),
        )

        eta = pt.full((n_cells, n_end), MASK_LOGIT, dtype="float64")
        eta = pt.set_subtensor(eta[cell_ref_rows, cell_ref_col], 0.0)
        eta = pt.set_subtensor(eta[cell_row_idx, cell_col_idx], cell_logodds)
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
        "build_state_transition_model cells=%d start_states=%d end_classes=%d "
        "alpha_pairs=%d cell_pairs=%d events=%d",
        inputs.n_cells,
        n_start,
        n_end,
        n_alpha,
        n_pairs,
        inputs.n_events,
    )
    return model
