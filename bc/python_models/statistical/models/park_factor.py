"""Team-game-grain NegativeBinomial park-factor builder.

Models team runs scored in a game as ``NB(mu=lambda, alpha=phi)`` with
``log lambda = log(exposure_pa) + alpha_season_league + offense + pitching
+ theta_park + home_adv``. ``theta_park`` is the published park factor:
a per-(park, season, league) effect centered to sum to zero *within* each
season-league group so era scoring cannot leak into it. The underlying
per-cell effect optionally follows a non-centered, gap-aware AR(1)
persistence prior along each (park, league) chain of observed cells:
``theta_t = rho**d * theta_{t-1} + eps_t * sqrt((1 - rho**(2d)) / (1 - rho**2))``
where ``d`` is the season distance to the previous observed cell, so the
prior is the stationary AR(1) bridge over unobserved seasons and no latent
cells exist for them. Offense and pitching team effects are non-centered
random effects; no team-game-sized Deterministic (e.g. lambda) is
registered.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging
import os
from typing import cast

import numpy as np
import pymc as pm
import pytensor.tensor as pt

from python_models.statistical.models._park_factor_data import ParkFactorInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)

ALPHA_SEASON_LEAGUE_SCALE: float = 0.5
SIGMA_TEAM_EFFECT_SCALE: float = 0.4
SIGMA_PARK_INNOV_SCALE: float = 0.15
SIGMA_PARK_INIT_SCALE: float = 0.3


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


def _stationary_bridge_variance_ratio(
    rho: pt.TensorVariable, season_gap: np.ndarray
) -> pt.TensorVariable:
    gap = season_gap.astype(np.int64)
    max_gap = int(gap.max()) if gap.shape[0] else 0
    powers = 2 * np.arange(max_gap, dtype=np.float64)
    included = (np.arange(max_gap)[None, :] < gap[:, None]).astype(np.float64)
    rho_powers = pt.power(rho, pt.as_tensor_variable(powers))
    return pt.as_tensor_variable(included) @ rho_powers


def _chain_blocks(
    chain_idx: np.ndarray, step_idx: np.ndarray, season_gap: np.ndarray
) -> tuple[np.ndarray, list[tuple[int, int, np.ndarray]]]:
    order = np.lexsort((step_idx, chain_idx))
    sorted_chain = chain_idx[order]
    sorted_gap = season_gap[order]
    blocks: list[tuple[int, int, np.ndarray]] = []
    start = 0
    n = order.shape[0]
    while start < n:
        end = start + 1
        while end < n and sorted_chain[end] == sorted_chain[start]:
            end += 1
        offsets = np.cumsum(sorted_gap[start:end]).astype(np.float64)
        exponent = offsets[:, None] - offsets[None, :]
        exponent[exponent < 0.0] = 0.0
        blocks.append((start, end, exponent))
        start = end
    return order, blocks


def _propagate_ar1(
    rho: pt.TensorVariable,
    innovations: pt.TensorVariable,
    chain_idx: np.ndarray,
    step_idx: np.ndarray,
    season_gap: np.ndarray,
) -> pt.TensorVariable:
    order, blocks = _chain_blocks(chain_idx, step_idx, season_gap)
    sorted_innovations = innovations[pt.as_tensor_variable(order)]
    pieces: list[pt.TensorVariable] = []
    for start, end, exponent in blocks:
        segment = sorted_innovations[start:end]
        if end - start == 1:
            pieces.append(segment)
            continue
        lower = np.tril(np.ones_like(exponent))
        weights = pt.power(
            rho, pt.as_tensor_variable(exponent)
        ) * pt.as_tensor_variable(lower)
        pieces.append(weights @ segment)
    inverse = np.empty_like(order)
    inverse[order] = np.arange(order.shape[0], dtype=np.int64)
    return cast(
        pt.TensorVariable, pt.concatenate(pieces)[pt.as_tensor_variable(inverse)]
    )


def _ar1_park_effect(inputs: ParkFactorInputs) -> pt.TensorVariable:
    rho = pm.Beta("rho_park", alpha=2.0, beta=1.0)
    sigma_innov = pm.HalfNormal("sigma_park_innov", sigma=SIGMA_PARK_INNOV_SCALE)
    sigma_init = pm.HalfNormal("sigma_park_init", sigma=SIGMA_PARK_INIT_SCALE)
    eps_raw = pm.Normal("eps_park_raw", mu=0.0, sigma=1.0, dims="park_season_league")

    season_gap = inputs.ar_season_gap.astype(np.int64)
    is_head = pt.as_tensor_variable((season_gap == 0).astype(np.float64))
    bridge_scale = pt.sqrt(
        _stationary_bridge_variance_ratio(rho, np.maximum(season_gap, 1))
    )
    innovation_scale = (
        is_head * sigma_init + (1.0 - is_head) * sigma_innov * bridge_scale
    )
    return _propagate_ar1(
        rho,
        eps_raw * innovation_scale,
        inputs.ar_chain_idx.astype(np.int64),
        inputs.ar_step_idx.astype(np.int64),
        season_gap,
    )


def build_park_factor_model(
    inputs: ParkFactorInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the team-game-grain NegativeBinomial park-factor model."""
    cfg = priors or BayesPriorConfig()
    ar1_active = _ar1_enabled()
    home_adv_active = _home_adv_enabled()

    with pm.Model(coords=dict(inputs.coords)) as model:
        season_league_idx = pm.Data("season_league_idx", inputs.season_league_idx)
        offense_idx = pm.Data("offense_idx", inputs.offense_idx)
        pitching_idx = pm.Data("pitching_idx", inputs.pitching_idx)
        park_season_league_idx = pm.Data(
            "park_season_league_idx", inputs.park_season_league_idx
        )
        log_exposure = pm.Data("log_exposure", np.log(inputs.exposure_pa))

        baseline_log_rate = float(
            np.log(inputs.team_runs.sum() / inputs.exposure_pa.sum())
        )

        alpha_season_league = pm.Normal(
            "alpha_season_league",
            mu=baseline_log_rate,
            sigma=ALPHA_SEASON_LEAGUE_SCALE,
            dims="season_league",
        )

        z_offense = pm.ZeroSumNormal("z_offense", sigma=1.0, dims="offense_team")
        sigma_offense = pm.HalfNormal("sigma_offense", sigma=SIGMA_TEAM_EFFECT_SCALE)
        offense = pm.Deterministic(
            "offense", z_offense * sigma_offense, dims="offense_team"
        )

        z_pitching = pm.ZeroSumNormal("z_pitching", sigma=1.0, dims="pitching_team")
        sigma_pitching = pm.HalfNormal("sigma_pitching", sigma=SIGMA_TEAM_EFFECT_SCALE)
        pitching = pm.Deterministic(
            "pitching", z_pitching * sigma_pitching, dims="pitching_team"
        )

        if ar1_active:
            theta_raw = _ar1_park_effect(inputs)
        else:
            sigma_park = pm.HalfNormal("sigma_park", sigma=SIGMA_PARK_INIT_SCALE)
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
