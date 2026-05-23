"""Event-grain hierarchical Bayes builder for fielding-credit allocation.

Two-arm likelihood over a shared softmax:

* Supervised arm: per-event ``Multinomial(U_e, pi_e)`` on the unmasked
  subset of well-attributed events. Y comes from the observed
  known_credit grid. This is the load-bearing source of per-event
  signal — without it the per-event REs cancel under softmax.
* Aggregate arm: per-(game, team, player, position) Normal target
  ``T_target[m] ~ Normal(Σ_{(e,k) in m} U_e * pi_{e,k}, sigma_box)``
  on the masked subset. ``T_target[m]`` is the sum of *hidden but
  known* credit at that cell, computed deterministically from the mask.

Per-position intercept (``alpha_position``) and each per-position FE
interaction (``delta_<fe>``) use ``pm.ZeroSumNormal`` over the position
axis so the softmax is identified. Per-event REs (season, scorer,
park, source) and per-event global FEs are kept — the supervised arm
makes them data-informed (in v1 they were prior-only).
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging
import os

import numpy as np
import pymc as pm
import pytensor.tensor as pt

from python_models.statistical.models._credit_data import EventCreditInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)

NONCENTER_SEASON_ENV: str = "BC_CREDIT_NONCENTER_SEASON"


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
        coords["supervised_event"] = [
            str(i) for i in range(inputs.n_supervised_events)
        ]
    if inputs.n_targets > 0:
        coords["target"] = [str(m) for m in range(inputs.n_targets)]
    source_effect_active = len(coords["source"]) > 1
    K = inputs.n_positions

    with pm.Model(coords=coords) as model:
        season_idx = pm.Data("season_idx", inputs.season_idx)
        scorer_idx = pm.Data("scorer_idx", inputs.scorer_idx)
        park_idx = pm.Data("park_idx", inputs.park_idx)
        U_data = pm.Data("U", inputs.U.astype(np.int64))

        alpha = pm.ZeroSumNormal(
            "alpha_position", sigma=cfg.alpha_scale, dims="position"
        )

        sigma_season = pm.HalfNormal("sigma_season", sigma=cfg.sigma_season_scale)
        sigma_scorer = pm.HalfNormal("sigma_scorer", sigma=cfg.sigma_scorer_scale)
        sigma_park = pm.HalfNormal("sigma_park", sigma=cfg.sigma_park_scale)

        noncenter_season = os.environ.get(NONCENTER_SEASON_ENV, "0") == "1"
        if noncenter_season:
            z_season = pm.ZeroSumNormal("z_season", sigma=1.0, dims="season")
            beta_season = pm.Deterministic(
                "beta_season", z_season * sigma_season, dims="season"
            )
        else:
            beta_season = pm.ZeroSumNormal(
                "beta_season", sigma=sigma_season, dims="season"
            )
        z_scorer = pm.Normal("z_scorer", mu=0.0, sigma=1.0, dims="scorer")
        z_park = pm.Normal("z_park", mu=0.0, sigma=1.0, dims="park")
        beta_scorer = pm.Deterministic(
            "beta_scorer", z_scorer * sigma_scorer, dims="scorer"
        )
        beta_park = pm.Deterministic("beta_park", z_park * sigma_park, dims="park")

        per_event_terms: list[object] = [
            beta_season[season_idx],
            beta_scorer[scorer_idx],
            beta_park[park_idx],
        ]

        if source_effect_active:
            source_idx = pm.Data("source_idx", inputs.source_idx)
            sigma_source = pm.HalfNormal("sigma_source", sigma=cfg.sigma_source_scale)
            z_source = pm.Normal("z_source", mu=0.0, sigma=1.0, dims="source")
            beta_source = pm.Deterministic(
                "beta_source", z_source * sigma_source, dims="source"
            )
            per_event_terms.append(beta_source[source_idx])

        for column, design in inputs.global_effects.items():
            levels_coord = f"{column}_levels"
            if len(design.levels) <= 1:
                continue
            codes_data = pm.Data(f"{column}_codes", design.codes.astype(np.int64))
            gamma = pm.ZeroSumNormal(
                f"gamma_{column}",
                sigma=cfg.fixed_effect_scale,
                dims=levels_coord,
            )
            per_event_terms.append(gamma[codes_data])

        per_event_sum = per_event_terms[0]
        for term in per_event_terms[1:]:
            per_event_sum = per_event_sum + term

        eta = alpha[None, :] + per_event_sum[:, None]

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
            sup_U = pm.Data(
                "Y_supervised_U", inputs.Y_supervised_U.astype(np.int64)
            )
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
        "build_fielding_credit_model credit_type=%s events=%d supervised=%d targets=%d source_effect_active=%s K=%d",
        inputs.credit_type,
        inputs.n_events,
        inputs.n_supervised_events,
        inputs.n_targets,
        source_effect_active,
        K,
    )
    return model
