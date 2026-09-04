"""Cell-grain NegativeBinomial run-expectancy builder.

Models the summed ``runs_to_end_of_inning`` over the events in each
``(state, season, league)`` cell as ``NB(mu=n*lambda, alpha=n*phi[state])``,
the exact aggregate of ``n`` per-event ``NB(lambda, phi[state])`` draws that
share the cell mean. The dispersion ``phi`` is per base-out state: the
empirical variance-to-mean ratio of runs-to-end runs from ~2 for bases-empty
and two-out states down to ~1.2 for loaded zero-out states, so one global
``phi`` under-disperses some states and over-disperses others.
``log lambda`` follows a centered global -> 24-state base-out -> cell
varying-intercept hierarchy: ``mu_state ~ N(global_mu, sigma_state)`` and
``theta_cell ~ N(mu_state, sigma_cell)``. Every cell clears a 25-event floor and
every state pools hundreds of thousands of events, so the data-dense centered
parameterization mixes without a funnel and pins each level generatively (no
sum-to-zero constraint, so the cell grid may be ragged). ``re_value =
exp(theta_cell)`` is the published run-expectancy value.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging
import os

import numpy as np
import pymc as pm

from python_models.statistical.models._run_values_data import RunExpectancyInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)


def _era_regime_enabled(n_era_regimes: int) -> bool:
    if n_era_regimes == 0:
        return False
    return os.environ.get("BC_RUN_VALUES_DISABLE_ERA_REGIME", "") not in ("1", "true")


def build_run_expectancy_model(
    inputs: RunExpectancyInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the cell-grain NegativeBinomial run-expectancy model."""
    cfg = priors or BayesPriorConfig()

    n_era_regimes = len(inputs.coords.get("era_regime", []))
    era_active = _era_regime_enabled(n_era_regimes)

    with pm.Model(coords=inputs.coords) as model:
        cell_state_idx = pm.Data("cell_state_idx", inputs.cell_state_idx)
        cell_event_count = pm.Data(
            "cell_event_count", inputs.cell_event_count.astype(np.float64)
        )

        global_mu = pm.Normal("global_mu", mu=cfg.alpha_loc, sigma=cfg.alpha_scale)
        sigma_state = pm.HalfNormal("sigma_state", sigma=cfg.sigma_state_scale)
        mu_state = pm.Normal("mu_state", mu=global_mu, sigma=sigma_state, dims="state")

        cell_loc = mu_state[cell_state_idx]
        if era_active:
            cell_era_design = pm.Data("cell_era_design", inputs.cell_era_design)
            sigma_era = pm.HalfNormal("sigma_era", sigma=cfg.sigma_era_scale)
            z_era = pm.Normal("z_era", mu=0.0, sigma=1.0, dims=("state", "era_regime"))
            a_era = pm.Deterministic(
                "a_era", z_era * sigma_era, dims=("state", "era_regime")
            )
            era_term = pm.math.sum(
                cell_era_design * a_era[cell_state_idx], axis=1
            )
            cell_loc = cell_loc + era_term

        sigma_cell = pm.HalfNormal("sigma_cell", sigma=cfg.sigma_cell_scale)
        theta_cell = pm.Normal(
            "theta_cell", mu=cell_loc, sigma=sigma_cell, dims="cell"
        )
        re_value = pm.Deterministic("re_value", pm.math.exp(theta_cell), dims="cell")

        phi = pm.Gamma(
            "phi",
            alpha=cfg.nb_phi_prior_alpha,
            beta=cfg.nb_phi_prior_beta,
            dims="state",
        )

        _ = pm.NegativeBinomial(
            "sum_runs_obs",
            mu=cell_event_count * re_value,
            alpha=cell_event_count * phi[cell_state_idx],
            observed=inputs.sum_runs.astype(np.int64),
        )

    _log.info(
        "build_run_expectancy_model cells=%d states=%d events=%d era_regimes=%d "
        "era_active=%s",
        inputs.n_cells,
        len(inputs.coords["state"]),
        inputs.n_events,
        n_era_regimes,
        era_active,
    )
    return model
