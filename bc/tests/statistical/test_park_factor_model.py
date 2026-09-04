"""Builder tests for the park-factor count model."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

import numpy as np
import pymc as pm
import pytensor.tensor as pt
import pytest
from pytensor.tensor.random.basic import BetaRV

from python_models.statistical.models._park_factor_data import (
    ParkFactorHeldOutSet,
    ParkFactorInputs,
)
from python_models.statistical.models.park_factor import (
    _center_within_group,
    _propagate_ar1,
    _stationary_bridge_variance_ratio,
    build_park_factor_model,
)


def _tiny_inputs(seasons: tuple[int, int] = (1990, 1991)) -> ParkFactorInputs:
    first, second = seasons
    park_cells = [
        f"PARKA|{first}|NL",
        f"PARKB|{first}|NL",
        f"PARKA|{second}|NL",
        f"PARKB|{second}|NL",
    ]
    season_leagues = [f"{first}|NL", f"{second}|NL"]
    offense = ["AWY", "HOM"]
    pitching = ["AWY", "HOM"]
    n = 16
    gap = second - first
    rng = np.random.default_rng(20260513)
    return ParkFactorInputs(
        team_runs=rng.integers(0, 8, size=n).astype(np.int64),
        exposure_pa=rng.integers(30, 45, size=n).astype(np.float64),
        season_league_idx=(np.arange(n) // 8).astype(np.int64),
        offense_idx=(np.arange(n) % 2).astype(np.int64),
        pitching_idx=((np.arange(n) + 1) % 2).astype(np.int64),
        park_season_league_idx=(np.arange(n) % 4).astype(np.int64),
        home_idx=(np.arange(n) % 2).astype(np.int64),
        park_season_league_labels=list(park_cells),
        park_id_by_cell=["PARKA", "PARKB", "PARKA", "PARKB"],
        season_by_cell=[first, first, second, second],
        league_by_cell=["NL", "NL", "NL", "NL"],
        cell_season_league_idx=np.array([0, 0, 1, 1], dtype=np.int64),
        ar_chain_idx=np.array([0, 1, 0, 1], dtype=np.int64),
        ar_step_idx=np.array([0, 0, 1, 1], dtype=np.int64),
        ar_season_gap=np.array([0, 0, gap, gap], dtype=np.int64),
        n_ar_chains=2,
        outcome="team_runs",
        coords={
            "source": ["__single__"],
            "season_league": season_leagues,
            "offense_team": offense,
            "pitching_team": pitching,
            "park_season_league": park_cells,
        },
        held_out=ParkFactorHeldOutSet(
            team_runs=np.zeros(0, dtype=np.int64),
            exposure_pa=np.zeros(0, dtype=np.float64),
            season_league_idx=np.zeros(0, dtype=np.int64),
            offense_idx=np.zeros(0, dtype=np.int64),
            pitching_idx=np.zeros(0, dtype=np.int64),
            park_season_league_idx=np.zeros(0, dtype=np.int64),
        ),
    )


def test_builds_model_with_theta_park() -> None:
    inputs = _tiny_inputs()
    model = build_park_factor_model(inputs)
    assert isinstance(model, pm.Model)
    assert "theta_park" in model.named_vars
    assert "team_runs_obs" in {rv.name for rv in model.observed_RVs}


def test_no_team_game_sized_deterministic() -> None:
    inputs = _tiny_inputs()
    model = build_park_factor_model(inputs)
    assert "lam" not in model.named_vars
    assert "lambda" not in model.named_vars

    n_events = inputs.n_events
    coord_lengths = {name: len(vals) for name, vals in inputs.coords.items()}
    assert n_events not in coord_lengths.values()

    assert model.deterministics
    for det in model.deterministics:
        dims = tuple(model.named_vars_to_dims.get(det.name, ()))
        assert dims, f"{det.name} carries no named dims"
        det_lengths = tuple(coord_lengths[d] for d in dims)
        assert n_events not in det_lengths, f"{det.name} is team-game-sized {dims}"


def test_center_within_group_unit() -> None:
    rng = np.random.default_rng(11)
    effect = pt.as_tensor_variable(rng.normal(size=6))
    group = np.array([0, 0, 0, 1, 1, 1], dtype=np.int64)
    centered = _center_within_group(effect, group, 2).eval()
    assert abs(float(centered[:3].sum())) < 1e-9
    assert abs(float(centered[3:].sum())) < 1e-9


@pytest.mark.parametrize("ar1_disabled", ["", "1"])
def test_theta_park_centered_within_season_league(
    monkeypatch: pytest.MonkeyPatch, ar1_disabled: str
) -> None:
    monkeypatch.setenv("BC_PARK_FACTOR_DISABLE_AR1", ar1_disabled)
    inputs = _tiny_inputs(seasons=(1990, 1995))
    model = build_park_factor_model(inputs)
    assert ("rho_park" in model.named_vars) == (ar1_disabled == "")
    draws = pm.draw(model["theta_park"], draws=5, random_seed=7)
    group = inputs.cell_season_league_idx
    n_groups = len(inputs.coords["season_league"])
    for d in range(draws.shape[0]):
        for g in range(n_groups):
            within = draws[d][group == g]
            assert abs(float(within.sum())) < 1e-6


def test_ar1_vars_present_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BC_PARK_FACTOR_DISABLE_AR1", raising=False)
    inputs = _tiny_inputs()
    model = build_park_factor_model(inputs)
    assert "rho_park" in model.named_vars
    assert "sigma_park_innov" in model.named_vars
    assert "sigma_park_init" in model.named_vars
    assert "sigma_park" not in model.named_vars


def test_ar1_has_no_latent_cells_for_unobserved_seasons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BC_PARK_FACTOR_DISABLE_AR1", raising=False)
    inputs = _tiny_inputs(seasons=(1965, 1998))
    model = build_park_factor_model(inputs)
    assert "ar_chain" not in model.coords
    assert "ar_step" not in model.coords
    eps = model["eps_park_raw"]
    assert tuple(model.named_vars_to_dims[eps.name]) == ("park_season_league",)
    assert tuple(eps.shape.eval()) == (inputs.n_cells,)


def test_ar1_gradient_is_finite_at_chain_heads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BC_PARK_FACTOR_DISABLE_AR1", raising=False)
    inputs = _tiny_inputs(seasons=(1965, 1998))
    model = build_park_factor_model(inputs)
    point = model.initial_point()
    gradient_fn = model.compile_dlogp()
    assert np.isfinite(gradient_fn(point)).all()
    rng = np.random.default_rng(5)
    perturbed = {
        name: value + rng.normal(scale=0.5, size=np.shape(value))
        for name, value in point.items()
    }
    assert np.isfinite(gradient_fn(perturbed)).all()


def _bridge_ratio(rho: float, gap: int) -> float:
    return (1.0 - rho ** (2 * gap)) / (1.0 - rho**2)


@pytest.mark.parametrize("rho", [0.3, 0.958])
def test_gap_bridging_matches_stationary_formula(rho: float) -> None:
    gaps = np.array([0, 1, 2, 33], dtype=np.int64)
    ratio = _stationary_bridge_variance_ratio(
        pt.as_tensor_variable(np.float64(rho)), gaps
    ).eval()
    assert ratio[0] == pytest.approx(0.0)
    assert ratio[1] == pytest.approx(1.0)
    for gap, value in zip(gaps[1:], ratio[1:], strict=True):
        assert float(value) == pytest.approx(_bridge_ratio(rho, int(gap)))


def test_gap_bridging_two_cell_chain_variance() -> None:
    rho, sigma_init, sigma_innov, gap = 0.9, 0.2, 0.05, 33
    chain_idx = np.array([0, 0], dtype=np.int64)
    step_idx = np.array([0, 1], dtype=np.int64)
    season_gap = np.array([0, gap], dtype=np.int64)
    rho_t = pt.as_tensor_variable(np.float64(rho))
    scale = np.array(
        [
            sigma_init,
            sigma_innov * np.sqrt(_bridge_ratio(rho, gap)),
        ]
    )
    weights = np.column_stack(
        [
            _propagate_ar1(
                rho_t,
                pt.as_tensor_variable(np.eye(2)[:, k] * scale),
                chain_idx,
                step_idx,
                season_gap,
            ).eval()
            for k in range(2)
        ]
    )
    covariance = weights @ weights.T
    assert covariance[0, 0] == pytest.approx(sigma_init**2)
    assert covariance[1, 0] == pytest.approx(rho**gap * sigma_init**2)
    assert covariance[1, 1] == pytest.approx(
        rho ** (2 * gap) * sigma_init**2 + sigma_innov**2 * _bridge_ratio(rho, gap)
    )


def test_stationary_start_keeps_marginal_variance_across_gaps() -> None:
    rho, sigma_innov = 0.8, 0.1
    stationary_var = sigma_innov**2 / (1.0 - rho**2)
    season_gap = np.array([0, 1, 5, 1, 12], dtype=np.int64)
    n = season_gap.shape[0]
    chain_idx = np.zeros(n, dtype=np.int64)
    step_idx = np.arange(n, dtype=np.int64)
    rho_t = pt.as_tensor_variable(np.float64(rho))
    ratio = _stationary_bridge_variance_ratio(rho_t, season_gap).eval()
    scale = np.where(
        season_gap == 0, np.sqrt(stationary_var), sigma_innov * np.sqrt(ratio)
    )
    weights = np.column_stack(
        [
            _propagate_ar1(
                rho_t,
                pt.as_tensor_variable(np.eye(n)[:, k] * scale),
                chain_idx,
                step_idx,
                season_gap,
            ).eval()
            for k in range(n)
        ]
    )
    marginal_var = np.diag(weights @ weights.T)
    assert marginal_var == pytest.approx(np.full(n, stationary_var))


def test_unit_gaps_reduce_to_plain_ar1() -> None:
    rng = np.random.default_rng(3)
    rho = 0.7
    chain_idx = np.array([0, 1, 0, 1, 0], dtype=np.int64)
    step_idx = np.array([0, 0, 1, 1, 2], dtype=np.int64)
    season_gap = np.array([0, 0, 1, 1, 1], dtype=np.int64)
    innovations = rng.normal(size=5)
    theta = _propagate_ar1(
        pt.as_tensor_variable(np.float64(rho)),
        pt.as_tensor_variable(innovations),
        chain_idx,
        step_idx,
        season_gap,
    ).eval()
    for cid in np.unique(chain_idx):
        members = np.flatnonzero(chain_idx == cid)
        members = members[np.argsort(step_idx[members])]
        previous = 0.0
        for cell in members:
            expected = rho * previous + innovations[cell]
            assert theta[cell] == pytest.approx(expected)
            previous = expected
    ratio = _stationary_bridge_variance_ratio(
        pt.as_tensor_variable(np.float64(rho)), season_gap
    ).eval()
    assert ratio[season_gap == 1] == pytest.approx(np.ones(3))


def test_ar1_vars_absent_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BC_PARK_FACTOR_DISABLE_AR1", "1")
    inputs = _tiny_inputs()
    model = build_park_factor_model(inputs)
    assert "rho_park" not in model.named_vars
    assert "sigma_park_innov" not in model.named_vars
    assert "sigma_park" in model.named_vars
    assert "theta_park" in model.named_vars


def test_rho_prior_is_beta() -> None:
    inputs = _tiny_inputs()
    model = build_park_factor_model(inputs)
    rho = model["rho_park"]
    assert isinstance(rho.owner.op, BetaRV)
    alpha, beta = rho.owner.op.dist_params(rho.owner)
    assert float(alpha.eval()) == pytest.approx(2.0)
    assert float(beta.eval()) == pytest.approx(1.0)


def test_home_adv_term_gated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BC_PARK_FACTOR_DISABLE_HOME_ADV", raising=False)
    inputs = _tiny_inputs()
    model = build_park_factor_model(inputs)
    assert "home_adv" in model.named_vars

    monkeypatch.setenv("BC_PARK_FACTOR_DISABLE_HOME_ADV", "1")
    model_off = build_park_factor_model(inputs)
    assert "home_adv" not in model_off.named_vars


@pytest.mark.slow
def test_prior_predictive_runs() -> None:
    inputs = _tiny_inputs()
    model = build_park_factor_model(inputs)
    idata = pm.sample_prior_predictive(draws=2, model=model)
    assert "team_runs_obs" in idata.prior_predictive
