"""Builder tests for the park-factor count model."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

import numpy as np
import pymc as pm
import pytest

from python_models.statistical.models._park_factor_data import (
    ParkFactorHeldOutSet,
    ParkFactorInputs,
)
from python_models.statistical.models.park_factor import build_park_factor_model


def _tiny_inputs() -> ParkFactorInputs:
    park_cells = ["PARKA|1990|NL", "PARKB|1990|NL"]
    season_leagues = ["1990|NL"]
    offense = ["AWY", "HOM"]
    pitching = ["AWY", "HOM"]
    n = 12
    rng = np.random.default_rng(20260513)
    return ParkFactorInputs(
        team_runs=rng.integers(0, 8, size=n).astype(np.int64),
        exposure_pa=rng.integers(30, 45, size=n).astype(np.float64),
        season_league_idx=np.zeros(n, dtype=np.int64),
        offense_idx=(np.arange(n) % 2).astype(np.int64),
        pitching_idx=((np.arange(n) + 1) % 2).astype(np.int64),
        park_season_league_idx=(np.arange(n) % 2).astype(np.int64),
        park_season_league_labels=list(park_cells),
        park_id_by_cell=["PARKA", "PARKB"],
        season_by_cell=[1990, 1990],
        league_by_cell=["NL", "NL"],
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


@pytest.mark.slow
def test_prior_predictive_runs() -> None:
    inputs = _tiny_inputs()
    model = build_park_factor_model(inputs)
    idata = pm.sample_prior_predictive(draws=2, model=model)
    assert "team_runs_obs" in idata.prior_predictive
