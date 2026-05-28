"""Team-game-grain NegativeBinomial park-factor builder.

Models team runs scored in a game as ``NB(mu=lambda, alpha=phi)`` with
``log lambda = log(exposure_pa) + alpha_season_league + offense + pitching
+ theta_park``. ``theta_park`` is a sum-to-zero per-(park, season, league)
random effect — the published park factor — registered as the only
coord-sized Deterministic. Offense and pitching team effects are
non-centered random effects; no team-game-sized Deterministic (e.g.
lambda) is registered.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import numpy as np
import pymc as pm

from python_models.statistical.models._park_factor_data import ParkFactorInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)


def build_park_factor_model(
    inputs: ParkFactorInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the team-game-grain NegativeBinomial park-factor model."""
    cfg = priors or BayesPriorConfig()

    with pm.Model(coords=inputs.coords) as model:
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

        sigma_park = pm.HalfNormal("sigma_park", sigma=cfg.sigma_park_scale)
        z_theta_park = pm.ZeroSumNormal(
            "z_theta_park", sigma=1.0, dims="park_season_league"
        )
        theta_park = pm.Deterministic(
            "theta_park", z_theta_park * sigma_park, dims="park_season_league"
        )

        log_lambda = (
            log_exposure
            + alpha_season_league[season_league_idx]
            + offense[offense_idx]
            + pitching[pitching_idx]
            + theta_park[park_season_league_idx]
        )
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
        "pitching=%d park_cells=%d",
        inputs.n_events,
        len(inputs.coords["season_league"]),
        len(inputs.coords["offense_team"]),
        len(inputs.coords["pitching_team"]),
        len(inputs.coords["park_season_league"]),
    )
    return model
