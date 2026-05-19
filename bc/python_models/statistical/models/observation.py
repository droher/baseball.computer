"""Scorer and source-family observation models (Model A).

PR2 ships the shared 4-dim builder ``build_observation_model`` covering
trajectory, location_side, location_depth, and broad_contact dimensions
in both ``gamma_dl_zero`` and ``gamma_dl_shrunk`` flavors. Prior
structure (non-centered season/scorer/source random intercepts,
Bernoulli likelihood on ``is_observed``) is shared across dims.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false

from __future__ import annotations

import numpy as np
import pymc as pm
from pymc import math as pmm

from python_models.statistical.models._data import ObservationModelInputs
from python_models.statistical.schemas import BayesPriorConfig, GammaDlFlavor


def build_observation_model(
    inputs: ObservationModelInputs,
    *,
    priors: BayesPriorConfig | None = None,
    gamma_dl_flavor: GammaDlFlavor = "gamma_dl_zero",
    dimension: str = "trajectory",
) -> pm.Model:
    """Construct the Model A observation-propensity PyMC model.

    `gamma_dl_flavor = "gamma_dl_zero"` freezes the DL covariate at zero
    (``dl_logit`` still carried as ``pm.Data`` so the shrunk path can
    swap it in without rebuilding). `gamma_dl_flavor = "gamma_dl_shrunk"`
    samples a scalar `gamma_dl ~ Normal(0, gamma_dl_scale)` and adds
    ``gamma_dl * dl_logit`` to the linear predictor.

    ``dimension`` is carried for diagnostics only — variable names stay
    bare so downstream posterior-summary queries (``var_names=["alpha", ...]``)
    don't need per-dim plumbing.
    """
    cfg = priors if priors is not None else BayesPriorConfig()
    _ = dimension
    n_events = int(inputs.y.shape[0])
    coords: dict[str, list[str]] = {
        "event": [str(i) for i in range(n_events)],
        "season": inputs.coords["season"],
        "scorer": inputs.coords["scorer"],
        "source": inputs.coords["source"],
    }

    with pm.Model(coords=coords) as model:
        season_idx = pm.Data("season_idx", inputs.season_idx, dims="event")
        scorer_idx = pm.Data("scorer_idx", inputs.scorer_idx, dims="event")
        source_idx = pm.Data("source_idx", inputs.source_idx, dims="event")
        dl_logit = pm.Data("dl_logit", inputs.dl_logit, dims="event")
        y = pm.Data("y_observed", inputs.y.astype(np.int64), dims="event")

        alpha = pm.Normal("alpha", mu=cfg.alpha_loc, sigma=cfg.alpha_scale)

        sigma_season = pm.HalfNormal("sigma_season", sigma=cfg.sigma_season_scale)
        sigma_scorer = pm.HalfNormal("sigma_scorer", sigma=cfg.sigma_scorer_scale)
        sigma_source = pm.HalfNormal("sigma_source", sigma=cfg.sigma_source_scale)

        z_season = pm.Normal("z_season", mu=0.0, sigma=1.0, dims="season")
        z_scorer = pm.Normal("z_scorer", mu=0.0, sigma=1.0, dims="scorer")
        z_source = pm.Normal("z_source", mu=0.0, sigma=1.0, dims="source")

        beta_season = pm.Deterministic(
            "beta_season", z_season * sigma_season, dims="season"
        )
        beta_scorer = pm.Deterministic(
            "beta_scorer", z_scorer * sigma_scorer, dims="scorer"
        )
        beta_source = pm.Deterministic(
            "beta_source", z_source * sigma_source, dims="source"
        )

        if gamma_dl_flavor == "gamma_dl_shrunk":
            gamma_dl = pm.Normal(
                "gamma_dl", mu=cfg.gamma_dl_loc, sigma=cfg.gamma_dl_scale
            )
            dl_term = gamma_dl * dl_logit
        else:
            dl_term = 0.0 * dl_logit

        eta = (
            alpha
            + beta_season[season_idx]
            + beta_scorer[scorer_idx]
            + beta_source[source_idx]
            + dl_term
        )
        p_observed = pm.Deterministic("p_observed", pmm.sigmoid(eta), dims="event")
        _ = pm.Bernoulli("observed", p=p_observed, observed=y, dims="event")

    return model


def build_trajectory_observedness_model(
    inputs: ObservationModelInputs,
    *,
    priors: BayesPriorConfig | None = None,
    gamma_dl_flavor: GammaDlFlavor = "gamma_dl_zero",
) -> pm.Model:
    return build_observation_model(
        inputs,
        priors=priors,
        gamma_dl_flavor=gamma_dl_flavor,
        dimension="trajectory",
    )
