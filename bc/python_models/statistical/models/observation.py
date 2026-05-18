"""Scorer and source-family observation models (A, B).

PR1 ships Model A for the trajectory dimension only. Other three
dimensions (location_side, location_depth, broad_contact) stay
``NotImplementedError`` until PR2.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false

from __future__ import annotations

import numpy as np
import pymc as pm
from pymc import math as pmm

from python_models.statistical.models._data import ObservationModelInputs
from python_models.statistical.schemas import BayesPriorConfig


def build_trajectory_observedness_model(
    inputs: ObservationModelInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    cfg = priors if priors is not None else BayesPriorConfig()
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

        gamma_dl: float = 0.0

        eta = (
            alpha
            + beta_season[season_idx]
            + beta_scorer[scorer_idx]
            + beta_source[source_idx]
            + gamma_dl * dl_logit
        )
        p_observed = pm.Deterministic("p_observed", pmm.sigmoid(eta), dims="event")
        _ = pm.Bernoulli("observed", p=p_observed, observed=y, dims="event")

    return model


def build_location_side_observedness_model() -> pm.Model:
    raise NotImplementedError("location_side observedness lands in PR2")


def build_location_depth_observedness_model() -> pm.Model:
    raise NotImplementedError("location_depth observedness lands in PR2")


def build_broad_contact_observedness_model() -> pm.Model:
    raise NotImplementedError("broad_contact observedness lands in PR2")
