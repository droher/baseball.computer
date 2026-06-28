"""Event-grain hierarchical Bayes builder for the observation propensity model.

One Bernoulli per event; partial pooling on season / scorer / park (and
source_family when more than one level is present); integer-indexer
fixed-effect coefficients for each low-card categorical (a length-K
``delta`` vector with the reference level pinned at 0); per-continuous
slope on the standardized value plus a paired indicator for the
imputed-missing events. Reference levels are chosen alphabetically by
``EventObservationInputs`` so the design columns match the
``<cat>_levels`` coord order.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportAny=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import numpy as np
import pymc as pm

from python_models.statistical.models._event_data import EventObservationInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)


def build_observation_model(
    inputs: EventObservationInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the event-grain Model A observation-propensity PyMC model.

    Random effects: season uses ``pm.ZeroSumNormal`` (centered, with the
    sum-to-zero constraint baked in) because the unconstrained
    parameterization sits on a ridge with ``alpha`` — adding a constant
    to ``alpha`` and subtracting it from every season gives the same
    likelihood, and with only 116 seasons + ``σ_season ≈ 2.6`` that
    ridge mixes glacially (ess ≈ 30 per season at 4 chains × 1000
    draws). The zero-sum constraint snaps the season block onto a
    ``(n-1)``-dim subspace orthogonal to the intercept direction. Scorer
    and park use non-centered (``β = σ · z``, ``z ~ N(0,1)``) because
    their cells are sparse and the soft ridge mixes fast under wide
    coord counts. source_family joins only when
    ``len(coords['source']) > 1`` — the production population is
    single-source by construction. Fixed-effect categoricals each
    contribute a length-K ``delta_<col>`` vector indexed by the integer
    codes from ``FixedEffectDesign.codes`` (with the reference level
    pinned at 0); continuous slopes contribute a single
    Normal(0, continuous_slope_scale) coefficient on the standardized
    value plus a paired indicator slope for events with the raw value
    missing. The Bernoulli likelihood uses ``logit_p=eta`` directly so
    we never materialize a per-event ``p_observed`` posterior tensor
    (which at 12M events × 4000 draws × 8 B would be ~380 GB).
    """
    cfg = priors if priors is not None else BayesPriorConfig()
    coords: dict[str, list[str]] = dict(inputs.coords)
    source_effect_active = len(coords["source"]) > 1

    with pm.Model(coords=coords) as model:
        season_idx = pm.Data("season_idx", inputs.season_idx)
        scorer_idx = pm.Data("scorer_idx", inputs.scorer_idx)
        park_idx = pm.Data("park_idx", inputs.park_idx)
        y = pm.Data("y_observed", inputs.y.astype(np.int64))

        alpha = pm.Normal("alpha", mu=cfg.alpha_loc, sigma=cfg.alpha_scale)

        sigma_season = pm.HalfNormal("sigma_season", sigma=cfg.sigma_season_scale)
        sigma_scorer = pm.HalfNormal("sigma_scorer", sigma=cfg.sigma_scorer_scale)
        sigma_park = pm.HalfNormal("sigma_park", sigma=cfg.sigma_park_scale)

        beta_season = pm.ZeroSumNormal(
            "beta_season", sigma=sigma_season, dims="season"
        )

        z_scorer = pm.Normal("z_scorer", mu=0.0, sigma=1.0, dims="scorer")
        z_park = pm.Normal("z_park", mu=0.0, sigma=1.0, dims="park")

        beta_scorer = pm.Deterministic(
            "beta_scorer", z_scorer * sigma_scorer, dims="scorer"
        )
        beta_park = pm.Deterministic(
            "beta_park", z_park * sigma_park, dims="park"
        )

        eta_terms: list[object] = [
            alpha,
            beta_season[season_idx],
            beta_scorer[scorer_idx],
            beta_park[park_idx],
        ]

        if source_effect_active:
            source_idx = pm.Data("source_idx", inputs.source_idx)
            sigma_source = pm.HalfNormal(
                "sigma_source", sigma=cfg.sigma_source_scale
            )
            z_source = pm.Normal("z_source", mu=0.0, sigma=1.0, dims="source")
            beta_source = pm.Deterministic(
                "beta_source", z_source * sigma_source, dims="source"
            )
            eta_terms.append(beta_source[source_idx])

        for column, design in inputs.fixed_effects.items():
            levels_coord = f"{column}_levels"
            if len(design.levels) <= 1:
                continue
            codes_data = pm.Data(f"{column}_codes", design.codes.astype(np.int64))
            delta = pm.ZeroSumNormal(
                f"delta_{column}",
                sigma=cfg.fixed_effect_scale,
                dims=levels_coord,
            )
            eta_terms.append(delta[codes_data])

        for column, feature in inputs.continuous.items():
            x_cont = pm.Data(f"x_{column}", feature.values)
            gamma = pm.Normal(
                f"gamma_{column}",
                mu=0.0,
                sigma=cfg.continuous_slope_scale,
            )
            eta_terms.append(gamma * x_cont)
            if int(feature.is_missing.sum()) > 0:
                missing_data = pm.Data(
                    f"is_missing_{column}",
                    feature.is_missing.astype(np.float64),
                )
                delta_missing = pm.Normal(
                    f"delta_missing_{column}",
                    mu=0.0,
                    sigma=cfg.fixed_effect_scale,
                )
                eta_terms.append(delta_missing * missing_data)

        eta = eta_terms[0]
        for term in eta_terms[1:]:
            eta = eta + term

        _ = pm.Bernoulli("observed", logit_p=eta, observed=y)

    _log.info(
        "build_observation_model dim=%s rows=%d source_effect_active=%s",
        inputs.dimension,
        inputs.n_events,
        source_effect_active,
    )
    return model
