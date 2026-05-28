"""Event-grain hierarchical Bayes builder for batted-ball geometry (Model E).

Single-arm per-dimension categorical softmax over the directly recorded
geometry class. Generalizes the ball-handler builder's fixed K=9 position
set to a per-dimension class count drawn from the inputs, and adds a
flavor-gated per-class deep-learning covariate.

Per-class intercept (``alpha_class``) and each per-class FE interaction
(``delta_<fe>``) use ``pm.ZeroSumNormal`` over the class axis so the
softmax is identified. The season-league and scorer random effects are
scalar-per-event: they enter every class logit equally and cancel inside
the per-event softmax, so they are nuisance terms kept to mirror the
ball-handler builder. The DL term is per-class and does NOT cancel in the
softmax, which is the point of carrying it.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import numpy as np
import pymc as pm

from python_models.statistical.bayes.specs import GammaDlFlavor
from python_models.statistical.models._geometry_data import GeometryInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)


def build_geometry_model(
    inputs: GeometryInputs,
    *,
    priors: BayesPriorConfig | None = None,
    gamma_dl_flavor: GammaDlFlavor = "gamma_dl_zero",
) -> pm.Model:
    """Construct the event-grain single-arm geometry PyMC model."""
    cfg = priors if priors is not None else BayesPriorConfig()
    coords: dict[str, list[str]] = dict(inputs.coords)
    coords["class"] = list(inputs.class_labels)
    coords["event"] = [str(k) for k in range(inputs.n_events)]
    K = inputs.n_classes

    with pm.Model(coords=coords) as model:
        season_league_idx = pm.Data("season_league_idx", inputs.season_league_idx)
        scorer_idx = pm.Data("scorer_idx", inputs.scorer_idx)
        dl_logit = pm.Data(
            "dl_logit_per_class",
            inputs.dl_logit_per_class,
            dims=("event", "class"),
        )

        alpha = pm.ZeroSumNormal("alpha_class", sigma=cfg.alpha_scale, dims="class")

        sigma_season_league = pm.HalfNormal(
            "sigma_season_league", sigma=cfg.sigma_season_scale
        )
        sigma_scorer = pm.HalfNormal("sigma_scorer", sigma=cfg.sigma_scorer_scale)

        z_season_league = pm.ZeroSumNormal(
            "z_season_league", sigma=1.0, dims="season_league"
        )
        beta_season_league = pm.Deterministic(
            "beta_season_league",
            z_season_league * sigma_season_league,
            dims="season_league",
        )
        z_scorer = pm.Normal("z_scorer", mu=0.0, sigma=1.0, dims="scorer")
        beta_scorer = pm.Deterministic(
            "beta_scorer", z_scorer * sigma_scorer, dims="scorer"
        )

        per_event_sum = beta_season_league[season_league_idx] + beta_scorer[scorer_idx]
        eta = alpha[None, :] + per_event_sum[:, None]

        for column, design in inputs.fixed_effects.items():
            levels_coord = f"{column}_levels"
            if len(design.levels) <= 1:
                continue
            codes_data = pm.Data(f"{column}_codes_fe", design.codes.astype(np.int64))
            delta = pm.ZeroSumNormal(
                f"delta_{column}",
                sigma=cfg.fixed_effect_scale_credit,
                dims=(levels_coord, "class"),
            )
            eta = eta + delta[codes_data]

        if gamma_dl_flavor == "gamma_dl_shrunk" and inputs.dl_active:
            gamma_dl = pm.Normal("gamma_dl", cfg.gamma_dl_loc, cfg.gamma_dl_scale)
            dl_term = gamma_dl * dl_logit
        else:
            dl_term = 0.0 * dl_logit
        eta = eta + dl_term

        pi = pm.math.softmax(eta, axis=1)

        _ = pm.Multinomial(
            "G_observed",
            n=1,
            p=pi,
            observed=inputs.counts.astype(np.int64),
            dims=("event", "class"),
        )

    _log.info(
        "build_geometry_model dimension=%s events=%d K=%d fixed_effects=%d dl_active=%s gamma_dl_flavor=%s",
        inputs.dimension,
        inputs.n_events,
        K,
        len(inputs.fixed_effects),
        inputs.dl_active,
        gamma_dl_flavor,
    )
    return model
