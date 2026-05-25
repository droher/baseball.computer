"""Event-grain hierarchical Bayes builder for ball-handler imputation (Model D).

Single-arm K=9 categorical softmax over the fielder position that handled
a batted ball, for events whose handler is recorded. Trimmed sibling of
the dual-arm fielding-credit builder: keeps the supervised softmax arm,
drops the aggregate-box Normal arm, the synthetic mask, and the park /
source random effects.

Per-position intercept (``alpha_position``) and each per-position FE
interaction (``delta_<fe>``) use ``pm.ZeroSumNormal`` over the position
axis so the softmax is identified. The season-league and scorer random
effects are scalar-per-event: they enter every position logit equally and
cancel inside the per-event softmax, so they are nuisance terms kept to
mirror the credit builder and seed the deferred per-position extension.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import numpy as np
import pymc as pm

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
        season_league_idx = pm.Data("season_league_idx", inputs.season_league_idx)
        scorer_idx = pm.Data("scorer_idx", inputs.scorer_idx)

        alpha = pm.ZeroSumNormal(
            "alpha_position", sigma=cfg.alpha_scale, dims="position"
        )

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
