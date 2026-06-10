"""Event-grain hierarchical Bayes builder for ball-handler imputation (Model D).

Single-arm K=9 categorical softmax over the fielder position that handled
a batted ball, for events whose handler is recorded. Trimmed sibling of
the dual-arm fielding-credit builder: keeps the supervised softmax arm
and drops the aggregate-box Normal arm and the synthetic mask.

Per-position intercept (``alpha_position``) and each per-position FE
interaction (``delta_<fe>``) use ``pm.ZeroSumNormal`` over the position
axis so the softmax is identified. Scalar-per-event terms (season-league
/ scorer / park / source random effects) are omitted: they would enter
every position logit equally and cancel exactly inside the per-event
softmax.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import numpy as np
import pymc as pm
import pytensor.tensor as pt

from python_models.statistical.models._ball_handler_data import BallHandlerInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)


def build_ball_handler_model(
    inputs: BallHandlerInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the event-grain single-arm ball-handler PyMC model."""
    cfg = priors if priors is not None else BayesPriorConfig()
    coords: dict[str, list[str]] = dict(inputs.coords)
    coords["event"] = [str(k) for k in range(inputs.n_events)]
    K = inputs.n_positions

    with pm.Model(coords=coords) as model:
        alpha = pm.ZeroSumNormal(
            "alpha_position", sigma=cfg.alpha_scale, dims="position"
        )

        eta = pt.broadcast_to(alpha[None, :], (inputs.n_events, K))

        for column, design in inputs.fixed_effects.items():
            levels_coord = f"{column}_levels"
            if len(design.levels) <= 1:
                continue
            codes_data = pm.Data(f"{column}_codes_fe", design.codes.astype(np.int64))
            delta = pm.ZeroSumNormal(
                f"delta_{column}",
                sigma=cfg.fixed_effect_scale_credit,
                dims=(levels_coord, "position"),
            )
            eta = eta + delta[codes_data]

        pi = pm.math.softmax(eta, axis=1)

        _ = pm.Multinomial(
            "H_observed",
            n=1,
            p=pi,
            observed=inputs.counts.astype(np.int64),
            dims=("event", "position"),
        )

    _log.info(
        "build_ball_handler_model events=%d K=%d fixed_effects=%d",
        inputs.n_events,
        K,
        len(inputs.fixed_effects),
    )
    return model
