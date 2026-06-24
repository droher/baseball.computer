"""Event-grain error-credit allocation submodel (Model C error head).

Per-event K=9 softmax ``pi^E_e = softmax(eta^E_e)`` over the eligible
fielder positions with a supervised Multinomial likelihood
``E_{e,1:9} ~ Multinomial(U^E_e, pi^E_e)`` on well-attributed error
events. Errors are scorer-discretion outcomes, so the predictor carries
a per-(scorer, position) interaction ``delta_scorer`` on top of the
per-position intercept and the per-FE × position interactions; the
scorer-as-scalar term would cancel inside the per-event softmax, so the
scorer signal only survives as a position interaction. The
scorer-interaction arm is gated behind ``BC_ERROR_CREDIT_DISABLE_SCORER``
(mirroring ``run_values::_era_regime_enabled``) for the
scorer-confound sensitivity diagnostic.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging
import os

import numpy as np
import pymc as pm
import pytensor.tensor as pt

from python_models.statistical.models._error_credit_data import ErrorCreditInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)

SIGMA_SCORER_POSITION_SCALE: float = 0.5


def _scorer_interaction_enabled(n_scorers: int) -> bool:
    if n_scorers <= 1:
        return False
    return os.environ.get("BC_ERROR_CREDIT_DISABLE_SCORER", "") not in ("1", "true")


def build_error_credit_model(
    inputs: ErrorCreditInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the event-grain supervised error-allocation PyMC model."""
    cfg = priors or BayesPriorConfig()
    coords: dict[str, list[str]] = dict(inputs.coords)
    coords["event"] = [str(k) for k in range(inputs.n_events)]
    if inputs.n_supervised_events > 0:
        coords["supervised_event"] = [str(i) for i in range(inputs.n_supervised_events)]
    K = inputs.n_positions

    scorer_active = _scorer_interaction_enabled(len(coords.get("scorer", [])))

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

        if scorer_active:
            scorer_codes = pm.Data("scorer_codes", inputs.scorer_idx.astype(np.int64))
            delta_scorer = pm.ZeroSumNormal(
                "delta_scorer",
                sigma=SIGMA_SCORER_POSITION_SCALE,
                dims=("scorer", "position"),
            )
            eta = eta + delta_scorer[scorer_codes]

        pi = pm.math.softmax(eta, axis=1)

        if inputs.n_supervised_events > 0:
            sup_event_idx = pm.Data(
                "Y_supervised_event_idx",
                inputs.Y_supervised_event_idx.astype(np.int64),
            )
            sup_U = pm.Data("Y_supervised_U", inputs.Y_supervised_U.astype(np.int64))
            _ = pm.Multinomial(
                "E_supervised",
                n=sup_U,
                p=pi[sup_event_idx],
                observed=inputs.Y_supervised_counts.astype(np.int64),
                dims=("supervised_event", "position"),
            )
        else:
            _log.warning(
                "build_error_credit_model: no supervised events present; "
                "the model loses per-event signal."
            )

    _log.info(
        "build_error_credit_model events=%d supervised=%d K=%d scorer_active=%s",
        inputs.n_events,
        inputs.n_supervised_events,
        K,
        scorer_active,
    )
    return model
