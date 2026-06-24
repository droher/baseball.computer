"""Team-game-grain NegativeBinomial park-factor builder.

Models team runs scored in a game as ``NB(mu=lambda, alpha=phi)`` with
``log lambda = log(exposure_pa) + alpha_season_league + offense + pitching
+ theta_park + home_adv``. ``theta_park`` is the published park factor:
a per-(park, season, league) effect centered to sum to zero *within* each
season-league group so era scoring cannot leak into it. The underlying
per-cell effect optionally follows an AR(1) persistence prior across
consecutive seasons within a (park, league) chain. Offense and pitching
team effects are non-centered random effects; no team-game-sized
Deterministic (e.g. lambda) is registered.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging
import os

import numpy as np
import pymc as pm
import pytensor.tensor as pt

from python_models.statistical.models._park_factor_data import ParkFactorInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)


def _ar1_enabled() -> bool:
    return os.environ.get("BC_PARK_FACTOR_DISABLE_AR1", "") not in ("1", "true")


def _home_adv_enabled() -> bool:
    return os.environ.get("BC_PARK_FACTOR_DISABLE_HOME_ADV", "") not in ("1", "true")


def _center_within_group(
    effect: pt.TensorVariable, group_idx: np.ndarray, n_groups: int
) -> pt.TensorVariable:
    membership = np.zeros((group_idx.shape[0], n_groups), dtype=np.float64)
    membership[np.arange(group_idx.shape[0]), group_idx.astype(np.int64)] = 1.0
    counts = membership.sum(axis=0)
    counts[counts == 0.0] = 1.0
    m = pt.as_tensor_variable(membership)
    inv_counts = pt.as_tensor_variable(1.0 / counts)
    group_means = (m.T @ effect) * inv_counts
    return effect - m @ group_means


def _ar1_park_effect(
    inputs: ParkFactorInputs,
    *,
    cfg: BayesPriorConfig,
) -> pt.TensorVariable:
    chain_idx = pt.as_tensor_variable(inputs.ar_chain_idx.astype(np.int64))
    step_idx = pt.as_tensor_variable(inputs.ar_step_idx.astype(np.int64))

    rho = pm.Beta("rho_park", alpha=2.0, beta=1.0)
    sigma_innov = pm.HalfNormal("sigma_park_innov", sigma=cfg.sigma_park_scale)
    sigma_init = pm.HalfNormal("sigma_park_init", sigma=cfg.sigma_park_scale)

    init_dist = pm.Normal.dist(mu=0.0, sigma=sigma_init)
    ar_grid = pm.AR(
        "ar_park",
        rho=pt.stack([pt.zeros_like(rho), rho]),
        sigma=sigma_innov,
        init_dist=init_dist,
        constant=True,
        dims=("ar_chain", "ar_step"),
    )
    return ar_grid[chain_idx, step_idx]


def build_park_factor_model(
    inputs: ParkFactorInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the team-game-grain NegativeBinomial park-factor model."""
    cfg = priors or BayesPriorConfig()
    ar1_active = _ar1_enabled()
    home_adv_active = _home_adv_enabled()

    coords = dict(inputs.coords)
    if ar1_active:
        coords["ar_chain"] = [str(i) for i in range(inputs.n_ar_chains)]
        coords["ar_step"] = [str(i) for i in range(inputs.n_ar_steps)]

    with pm.Model(coords=coords) as model:
        season_league_idx = pm.Data("season_league_idx", inputs.season_league_idx)
        offense_idx = pm.Data("offense_idx", inputs.offense_idx)
        pitching_idx = pm.Data("pitching_idx", inputs.pitching_idx)
        park_season_league_idx = pm.Data(
            "park_season_league_idx", inputs.park_season_league_idx
        )
        log_exposure = pm.Data("log_exposure", np.log(inputs.exposure_pa))

        alpha_season_league = pm.Normal(
            "alpha_season_league",
            mu=cfg.alpha_loc,
            sigma=cfg.alpha_scale,
            dims="season_league",
        )

        z_offense = pm.ZeroSumNormal("z_offense", sigma=1.0, dims="offense_team")
        sigma_offense = pm.HalfNormal("sigma_offense", sigma=cfg.sigma_offense_scale)
        offense = pm.Deterministic(
            "offense", z_offense * sigma_offense, dims="offense_team"
        )

        z_pitching = pm.ZeroSumNormal("z_pitching", sigma=1.0, dims="pitching_team")
        sigma_pitching = pm.HalfNormal("sigma_pitching", sigma=cfg.sigma_pitching_scale)
        pitching = pm.Deterministic(
            "pitching", z_pitching * sigma_pitching, dims="pitching_team"
        )

        if ar1_active:
            theta_raw = _ar1_park_effect(inputs, cfg=cfg)
        else:
            sigma_park = pm.HalfNormal("sigma_park", sigma=cfg.sigma_park_scale)
            z_theta_park = pm.Normal(
                "z_theta_park", mu=0.0, sigma=1.0, dims="park_season_league"
            )
            theta_raw = z_theta_park * sigma_park

        theta_park = pm.Deterministic(
            "theta_park",
            _center_within_group(
                theta_raw,
                inputs.cell_season_league_idx,
                len(inputs.coords["season_league"]),
            ),
            dims="park_season_league",
        )

        log_lambda = (
            log_exposure
            + alpha_season_league[season_league_idx]
            + offense[offense_idx]
            + pitching[pitching_idx]
            + theta_park[park_season_league_idx]
        )

        if home_adv_active:
            home_idx = pm.Data("home_idx", inputs.home_idx)
            home_adv = pm.Normal(
                "home_adv", mu=cfg.park_home_adv_loc, sigma=cfg.park_home_adv_scale
            )
            log_lambda = log_lambda + home_adv * home_idx

        lam = pm.math.exp(log_lambda)

        phi = pm.Gamma("phi", alpha=cfg.nb_phi_prior_alpha, beta=cfg.nb_phi_prior_beta)

        _ = pm.NegativeBinomial(
            "team_runs_obs",
            mu=lam,
            alpha=phi,
            observed=inputs.team_runs.astype(np.int64),
        )

    _log.info(
        "build_park_factor_model team_games=%d season_leagues=%d offense=%d "
        "pitching=%d park_cells=%d ar1=%s home_adv=%s",
        inputs.n_events,
        len(inputs.coords["season_league"]),
        len(inputs.coords["offense_team"]),
        len(inputs.coords["pitching_team"]),
        len(inputs.coords["park_season_league"]),
        ar1_active,
        home_adv_active,
    )
    return model
