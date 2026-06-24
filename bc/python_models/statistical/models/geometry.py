"""Event-grain hierarchical Bayes builder for batted-ball geometry (Model E).

Single-arm per-dimension categorical softmax over the directly recorded
geometry class. Generalizes the ball-handler builder's fixed K=9 position
set to a per-dimension class count drawn from the inputs, and adds a
flavor-gated per-class deep-learning covariate.

Per-class intercept (``alpha_class``) and each per-class FE interaction
(``delta_<fe>``) use ``pm.ZeroSumNormal`` over the class axis so the
softmax is identified. The DL term is per-class and does NOT cancel in
the softmax, which is the point of carrying it. Scalar-per-event terms
(season-league / scorer random effects) are omitted: they would enter
every class logit equally and cancel exactly inside the per-event
softmax.

An optional per-class handler-posterior covariate (``gamma_handler``)
times a standardized per-event summary of Model D's ball-handler posterior
(the logit of P(handler is an outfielder)) enters the same way, gated by
``inputs.handler_active``. Like the DL term it is per-class and does NOT
cancel.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import numpy as np
import pymc as pm

from python_models.statistical.bayes.specs import GammaDlFlavor, GammaPropensityFlavor
from python_models.statistical.models._geometry_data import GeometryInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)

GAMMA_HANDLER_SCALE: float = 0.5


def build_geometry_model(
    inputs: GeometryInputs,
    *,
    priors: BayesPriorConfig | None = None,
    gamma_dl_flavor: GammaDlFlavor = "gamma_dl_zero",
    gamma_propensity_flavor: GammaPropensityFlavor = "gamma_propensity_zero",
) -> pm.Model:
    """Construct the event-grain single-arm geometry PyMC model."""
    cfg = priors if priors is not None else BayesPriorConfig()
    coords: dict[str, list[str]] = dict(inputs.coords)
    coords["class"] = list(inputs.class_labels)
    coords["event"] = [str(k) for k in range(inputs.n_events)]
    K = inputs.n_classes

    with pm.Model(coords=coords) as model:
        dl_logit = pm.Data(
            "dl_logit_per_class",
            inputs.dl_logit_per_class,
            dims=("event", "class"),
        )

        alpha = pm.ZeroSumNormal("alpha_class", sigma=cfg.alpha_scale, dims="class")

        eta = alpha[None, :]

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

        propensity_values = (
            inputs.propensity_z
            if inputs.propensity_z is not None
            else np.zeros(inputs.n_events, dtype=np.float64)
        )
        z_prop = pm.Data("propensity_z", propensity_values, dims="event")
        if (
            gamma_propensity_flavor == "gamma_propensity_class"
            and inputs.propensity_active
        ):
            gamma_propensity = pm.ZeroSumNormal(
                "gamma_propensity", sigma=cfg.gamma_propensity_scale, dims="class"
            )
            eta = eta + z_prop[:, None] * gamma_propensity[None, :]

        handler_values = (
            inputs.handler_z
            if inputs.handler_z is not None
            else np.zeros(inputs.n_events, dtype=np.float64)
        )
        z_handler = pm.Data("handler_z", handler_values, dims="event")
        if inputs.handler_active:
            gamma_handler = pm.ZeroSumNormal(
                "gamma_handler", sigma=GAMMA_HANDLER_SCALE, dims="class"
            )
            eta = eta + z_handler[:, None] * gamma_handler[None, :]

        pi = pm.math.softmax(eta, axis=1)

        _ = pm.Multinomial(
            "G_observed",
            n=1,
            p=pi,
            observed=inputs.counts.astype(np.int64),
            dims=("event", "class"),
        )

    _log.info(
        "build_geometry_model dimension=%s events=%d K=%d fixed_effects=%d "
        "dl_active=%s gamma_dl_flavor=%s propensity_active=%s "
        "gamma_propensity_flavor=%s handler_active=%s",
        inputs.dimension,
        inputs.n_events,
        K,
        len(inputs.fixed_effects),
        inputs.dl_active,
        gamma_dl_flavor,
        inputs.propensity_active,
        gamma_propensity_flavor,
        inputs.handler_active,
    )
    return model
