"""Builder tests for the park-factor count model."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

import numpy as np
import pymc as pm
import pytensor.tensor as pt
import pytest

from python_models.statistical.models._park_factor_data import (
    ParkFactorHeldOutSet,
    ParkFactorInputs,
)
from python_models.statistical.models.park_factor import (
    _center_within_group,
    build_park_factor_model,
)


def _tiny_inputs() -> ParkFactorInputs:
    park_cells = [
        "PARKA|1990|NL",
        "PARKB|1990|NL",
        "PARKA|1991|NL",
        "PARKB|1991|NL",
    ]
    season_leagues = ["1990|NL", "1991|NL"]
    offense = ["AWY", "HOM"]
    pitching = ["AWY", "HOM"]
    n = 16
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
        season_by_cell=[1990, 1990, 1991, 1991],
        league_by_cell=["NL", "NL", "NL", "NL"],
        cell_season_league_idx=np.array([0, 0, 1, 1], dtype=np.int64),
        ar_chain_idx=np.array([0, 1, 0, 1], dtype=np.int64),
        ar_step_idx=np.array([0, 0, 1, 1], dtype=np.int64),
        n_ar_chains=2,
        n_ar_steps=2,
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


def test_theta_park_centered_within_season_league(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BC_PARK_FACTOR_DISABLE_AR1", "1")
    inputs = _tiny_inputs()
    model = build_park_factor_model(inputs)
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
    assert isinstance(rho.owner.op, pm.Beta.rv_type)
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
