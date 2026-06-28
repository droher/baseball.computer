"""Event-grain hierarchical Bayes builder for fielding-credit allocation.

Two-arm likelihood over a shared softmax:

* Supervised arm: per-event ``Multinomial(U_e, pi_e)`` on the unmasked
  subset of well-attributed events. Y comes from the observed
  known_credit grid. This is the load-bearing source of per-event
  signal.
* Aggregate arm: per-(game, team, player, position) Normal target
  ``T_target[m] ~ Normal(Σ_{(e,k) in m} U_e * pi_{e,k}, sigma_box)``
  on the masked subset. ``T_target[m]`` is the sum of *hidden but
  known* credit at that cell, computed deterministically from the mask.

Per-position intercept (``alpha_position``) and each per-position FE
interaction (``delta_<fe>``) use ``pm.ZeroSumNormal`` over the position
axis so the softmax is identified. Scalar-per-event terms (season /
scorer / park / source random effects and per-event global FEs) are
omitted: they would enter every position logit equally and cancel
exactly inside the per-event softmax, in both likelihood arms — the
supervised arm consumes ``pi`` directly and the aggregate arm consumes
sums of ``U_e * pi_{e,k}``.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import numpy as np
import pymc as pm
import pytensor.tensor as pt

from python_models.statistical.models._credit_data import EventCreditInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)


def build_fielding_credit_model(
    inputs: EventCreditInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the event-grain dual-arm credit-allocation PyMC model."""
    cfg = priors if priors is not None else BayesPriorConfig()
    coords: dict[str, list[str]] = dict(inputs.coords)
    coords["event"] = [str(k) for k in range(inputs.n_events)]
    if inputs.n_supervised_events > 0:
        coords["supervised_event"] = [str(i) for i in range(inputs.n_supervised_events)]
    if inputs.n_targets > 0:
        coords["target"] = [str(m) for m in range(inputs.n_targets)]
    K = inputs.n_positions

    with pm.Model(coords=coords) as model:
        U_data = pm.Data("U", inputs.U.astype(np.int64))

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

        if inputs.n_supervised_events > 0:
            sup_event_idx = pm.Data(
                "Y_supervised_event_idx",
                inputs.Y_supervised_event_idx.astype(np.int64),
            )
            sup_U = pm.Data("Y_supervised_U", inputs.Y_supervised_U.astype(np.int64))
            sup_counts_observed = inputs.Y_supervised_counts.astype(np.int64)
            pi_sup = pi[sup_event_idx]
            _ = pm.Multinomial(
                "Y_supervised",
                n=sup_U,
                p=pi_sup,
                observed=sup_counts_observed,
                dims=("supervised_event", "position"),
            )
        else:
            msg = "build_fielding_credit_model: no supervised events present; the model loses per-event signal and degenerates back to v1 behavior."
            _log.warning(msg)

        if inputs.n_targets > 0:
            agg_event_idx = pm.Data(
                "aggregate_event_idx", inputs.aggregate_event_idx.astype(np.int64)
            )
            agg_position_idx = pm.Data(
                "aggregate_position_idx",
                inputs.aggregate_position_idx.astype(np.int64),
            )
            agg_row_idx = pm.Data(
                "aggregate_row_idx", inputs.aggregate_row_idx.astype(np.int64)
            )
            agg_sigma = pm.Data(
                "aggregate_sigma", inputs.aggregate_sigma.astype(np.float64)
            )
            agg_targets = pm.Data(
                "aggregate_targets", inputs.aggregate_targets.astype(np.float64)
            )

            expected_counts = U_data.astype("float64")[:, None] * pi
            contributions = expected_counts[agg_event_idx, agg_position_idx]

            zeros = pt.zeros((inputs.n_targets,), dtype="float64")
            t_pred = pt.inc_subtensor(zeros[agg_row_idx], contributions)

            _ = pm.Normal(
                "T_observed",
                mu=t_pred,
                sigma=agg_sigma,
                observed=agg_targets,
                dims="target",
            )

    _log.info(
        "build_fielding_credit_model credit_type=%s events=%d supervised=%d targets=%d K=%d",
        inputs.credit_type,
        inputs.n_events,
        inputs.n_supervised_events,
        inputs.n_targets,
        K,
    )
    return model
