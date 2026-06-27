"""Cell-grain assist-count submodel (Model C count head).

Estimates the missing assist count ``M`` before player allocation. Each
``(event_class, base_out)`` cell carries its own categorical over the
count classes ``{1, 2, 3, 4}``; the spec's Dirichlet-multinomial is
expressed as the collapsed cell-grain Multinomial over a centered
reference-class hierarchical softmax, which mixes like the
run-expectancy and pitch-summary hierarchies. Class 0 (``M=1``) is the
reference with its logit pinned to zero; the remaining classes carry a
centered two-level hierarchy of log-odds against it — a corpus
``beta0_count`` and a per-event-class ``event_class_logodds`` centered
on it — so the cell mean pools toward its event-class mean and the
event-class mean toward the global mean. ``cell_class_prob =
softmax([0, cell_logodds])`` is the published per-cell count
distribution.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import numpy as np
import pymc as pm
import pytensor.tensor as pt

from python_models.statistical.models._assist_count_data import AssistCountInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)

SIGMA_EVENT_CLASS_SCALE: float = 0.7
SIGMA_CELL_SCALE: float = 0.5


def build_assist_count_model(
    inputs: AssistCountInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the cell-grain hierarchical assist-count Multinomial model."""
    cfg = priors or BayesPriorConfig()
    counts = inputs.counts.astype(np.int64)
    cell_total = counts.sum(axis=1)
    n_cells = inputs.n_cells

    with pm.Model(coords=inputs.coords) as model:
        cell_event_class_idx = pm.Data(
            "cell_event_class_idx", inputs.cell_event_class_idx
        )

        beta0_count = pm.Normal(
            "beta0_count", mu=0.0, sigma=cfg.alpha_scale, dims="assist_count_nonref"
        )
        sigma_event_class = pm.HalfNormal(
            "sigma_event_class", sigma=SIGMA_EVENT_CLASS_SCALE
        )
        event_class_logodds = pm.Normal(
            "event_class_logodds",
            mu=beta0_count,
            sigma=sigma_event_class,
            dims=("event_class", "assist_count_nonref"),
        )
        sigma_cell = pm.HalfNormal("sigma_cell", sigma=SIGMA_CELL_SCALE)
        cell_logodds = pm.Normal(
            "cell_logodds",
            mu=event_class_logodds[cell_event_class_idx],
            sigma=sigma_cell,
            dims=("cell", "assist_count_nonref"),
        )

        n_event_classes = len(inputs.coords["event_class"])
        ref_ec = pt.zeros((n_event_classes, 1))
        eta_ec = pt.concatenate([ref_ec, event_class_logodds], axis=1)
        pm.Deterministic(
            "event_class_count_prob",
            pm.math.softmax(eta_ec, axis=1),
            dims=("event_class", "assist_count_class"),
        )

        ref_col = pt.zeros((n_cells, 1))
        eta = pt.concatenate([ref_col, cell_logodds], axis=1)
        cell_class_prob = pm.Deterministic(
            "cell_class_prob",
            pm.math.softmax(eta, axis=1),
            dims=("cell", "assist_count_class"),
        )

        _ = pm.Multinomial(
            "assist_count_obs",
            n=cell_total,
            p=cell_class_prob,
            observed=counts,
        )

    _log.info(
        "build_assist_count_model cells=%d event_classes=%d classes=%d events=%d",
        inputs.n_cells,
        len(inputs.coords["event_class"]),
        inputs.n_classes,
        inputs.n_events,
    )
    return model
