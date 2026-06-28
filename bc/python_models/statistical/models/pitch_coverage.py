"""Event-grain hierarchical logistic for the ``has_count`` coverage arm.

``R_i ~ Bernoulli(p_i)`` models whether the plate appearance's final
ball-strike count was observed given source / context. The linear
predictor pools partially over a ``season|league`` cell
(``pm.ZeroSumNormal`` — the dense observation regime, mirroring the
season random effect in the obs-propensity model) and the ``scorer``
(non-centered), with a sum-to-zero fixed effect per context column
(``result_family``, ``alignment_regime``). The likelihood uses
``logit_p=eta`` directly so no per-event ``p`` posterior tensor is
materialized in the graph. The dataset is single-source by construction,
so no source random effect enters.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import numpy as np
import pymc as pm

from python_models.statistical.models._pitch_coverage_data import PitchCoverageInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)


def build_pitch_coverage_model(
    inputs: PitchCoverageInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the event-grain ``has_count`` Bernoulli coverage model."""
    cfg = priors if priors is not None else BayesPriorConfig()
    coords: dict[str, list[str]] = dict(inputs.coords)

    with pm.Model(coords=coords) as model:
        cell_idx = pm.Data("cell_idx", inputs.cell_idx)
        scorer_idx = pm.Data("scorer_idx", inputs.scorer_idx)
        y = pm.Data("y_observed", inputs.y.astype(np.int64))

        alpha = pm.Normal("alpha", mu=cfg.alpha_loc, sigma=cfg.alpha_scale)

        sigma_cell = pm.HalfNormal("sigma_cell", sigma=cfg.sigma_season_scale)
        beta_cell = pm.ZeroSumNormal(
            "beta_cell", sigma=sigma_cell, dims="season_league"
        )

        sigma_scorer = pm.HalfNormal("sigma_scorer", sigma=cfg.sigma_scorer_scale)
        z_scorer = pm.Normal("z_scorer", mu=0.0, sigma=1.0, dims="scorer")
        beta_scorer = pm.Deterministic(
            "beta_scorer", z_scorer * sigma_scorer, dims="scorer"
        )

        eta_terms: list[object] = [
            alpha,
            beta_cell[cell_idx],
            beta_scorer[scorer_idx],
        ]

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

        eta = eta_terms[0]
        for term in eta_terms[1:]:
            eta = eta + term

        _ = pm.Bernoulli("count_observed", logit_p=eta, observed=y)

    _log.info(
        "build_pitch_coverage_model events=%d cells=%d scorers=%d fixed_effects=%s",
        inputs.n_events,
        len(coords["season_league"]),
        len(coords["scorer"]),
        sorted(inputs.fixed_effects),
    )
    return model
