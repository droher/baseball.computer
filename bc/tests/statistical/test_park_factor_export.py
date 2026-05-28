"""Parameter-grain export + held-out NB eval tests for the park-factor count path."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import arviz as az
import numpy as np
import polars as pl

from python_models.statistical.bayes.training import (
    _evaluate_held_out_nb,
    _export_park_factor_posterior,
    _export_park_factor_summary,
)
from python_models.statistical.models._park_factor_data import (
    ParkFactorHeldOutSet,
    ParkFactorInputs,
)

PARK_CELLS = ["PARKA|1990|NL", "PARKB|1990|NL", "PARKC|1991|AL"]
PARK_IDS = ["PARKA", "PARKB", "PARKC"]
SEASONS = [1990, 1990, 1991]
LEAGUES = ["NL", "NL", "AL"]
SEASON_LEAGUES = ["1990|NL", "1991|AL"]
OFFENSE = ["AWY", "HOM"]
PITCHING = ["AWY", "HOM"]

N_CHAIN = 2
N_DRAW = 7


def _inputs(*, held_out_games: int) -> ParkFactorInputs:
    n = 9
    rng = np.random.default_rng(20260513)
    if held_out_games > 0:
        held = ParkFactorHeldOutSet(
            team_runs=rng.integers(0, 8, size=held_out_games).astype(np.int64),
            exposure_pa=rng.integers(30, 45, size=held_out_games).astype(np.float64),
            season_league_idx=(np.arange(held_out_games) % 2).astype(np.int64),
            offense_idx=(np.arange(held_out_games) % 2).astype(np.int64),
            pitching_idx=((np.arange(held_out_games) + 1) % 2).astype(np.int64),
            park_season_league_idx=np.array(
                [(-1 if i == 0 else i % 3) for i in range(held_out_games)],
                dtype=np.int64,
            ),
        )
    else:
        held = ParkFactorHeldOutSet(
            team_runs=np.zeros(0, dtype=np.int64),
            exposure_pa=np.zeros(0, dtype=np.float64),
            season_league_idx=np.zeros(0, dtype=np.int64),
            offense_idx=np.zeros(0, dtype=np.int64),
            pitching_idx=np.zeros(0, dtype=np.int64),
            park_season_league_idx=np.zeros(0, dtype=np.int64),
        )
    return ParkFactorInputs(
        team_runs=rng.integers(0, 8, size=n).astype(np.int64),
        exposure_pa=rng.integers(30, 45, size=n).astype(np.float64),
        season_league_idx=(np.arange(n) % 2).astype(np.int64),
        offense_idx=(np.arange(n) % 2).astype(np.int64),
        pitching_idx=((np.arange(n) + 1) % 2).astype(np.int64),
        park_season_league_idx=(np.arange(n) % 3).astype(np.int64),
        park_season_league_labels=list(PARK_CELLS),
        park_id_by_cell=list(PARK_IDS),
        season_by_cell=list(SEASONS),
        league_by_cell=list(LEAGUES),
        outcome="team_runs",
        coords={
            "source": ["__single__"],
            "season_league": list(SEASON_LEAGUES),
            "offense_team": list(OFFENSE),
            "pitching_team": list(PITCHING),
            "park_season_league": list(PARK_CELLS),
        },
        held_out=held,
    )


def _idata() -> az.InferenceData:
    rng = np.random.default_rng(7)
    posterior = {
        "theta_park": rng.normal(size=(N_CHAIN, N_DRAW, len(PARK_CELLS))),
        "alpha_season_league": rng.normal(
            loc=-1.0, size=(N_CHAIN, N_DRAW, len(SEASON_LEAGUES))
        ),
        "offense": rng.normal(scale=0.2, size=(N_CHAIN, N_DRAW, len(OFFENSE))),
        "pitching": rng.normal(scale=0.2, size=(N_CHAIN, N_DRAW, len(PITCHING))),
        "phi": rng.gamma(5.0, 1.0, size=(N_CHAIN, N_DRAW)),
    }
    coords = {
        "park_season_league": list(PARK_CELLS),
        "season_league": list(SEASON_LEAGUES),
        "offense_team": list(OFFENSE),
        "pitching_team": list(PITCHING),
    }
    dims = {
        "theta_park": ["park_season_league"],
        "alpha_season_league": ["season_league"],
        "offense": ["offense_team"],
        "pitching": ["pitching_team"],
    }
    return az.from_dict(posterior=posterior, coords=coords, dims=dims)


def test_posterior_export_schema_and_row_count(tmp_path: Path) -> None:
    inputs = _inputs(held_out_games=0)
    idata = _idata()
    target = tmp_path / "park_factor_posterior.parquet"
    _export_park_factor_posterior(idata, inputs, target_path=target)

    df = pl.read_parquet(target)
    assert df.height == len(PARK_CELLS) * N_CHAIN * N_DRAW
    assert df.schema == {
        "park_id": pl.Utf8,
        "season": pl.Int16,
        "league": pl.Utf8,
        "outcome": pl.Utf8,
        "theta_draw": pl.Float64,
        "chain": pl.Int16,
        "draw": pl.Int32,
    }
    assert df.get_column("outcome").unique().to_list() == ["team_runs"]
    assert sorted(df.get_column("park_id").unique().to_list()) == sorted(PARK_IDS)
    assert df.get_column("chain").max() == N_CHAIN - 1
    assert df.get_column("draw").max() == N_DRAW - 1

    theta = np.asarray(idata.posterior["theta_park"].values, dtype=np.float64)
    cell_a = df.filter(pl.col("park_id") == "PARKA").sort(["chain", "draw"])
    assert np.allclose(
        cell_a.get_column("theta_draw").to_numpy(),
        theta[:, :, 0].reshape(-1),
    )


def test_summary_export_schema_and_park_factor_relation(tmp_path: Path) -> None:
    inputs = _inputs(held_out_games=0)
    idata = _idata()
    target = tmp_path / "park_factor_summary.parquet"
    _export_park_factor_summary(idata, inputs, target_path=target)

    df = pl.read_parquet(target)
    assert df.height == len(PARK_CELLS)
    assert df.schema == {
        "park_id": pl.Utf8,
        "season": pl.Int16,
        "league": pl.Utf8,
        "outcome": pl.Utf8,
        "theta_mean": pl.Float64,
        "theta_sd": pl.Float64,
        "theta_hdi_lower": pl.Float64,
        "theta_hdi_upper": pl.Float64,
        "park_factor_mean": pl.Float64,
        "ess_bulk": pl.Float64,
        "rhat": pl.Float64,
    }

    assert df.get_column("park_id").to_list() == PARK_IDS
    assert df.get_column("season").to_list() == SEASONS
    assert df.get_column("league").to_list() == LEAGUES
    for k, label in enumerate(PARK_CELLS):
        row = df.row(k, named=True)
        rebuilt = f"{row['park_id']}|{row['season']}|{row['league']}"
        assert rebuilt == label

    pf = df.get_column("park_factor_mean").to_numpy()
    theta_mean = df.get_column("theta_mean").to_numpy()
    assert np.allclose(pf, np.exp(theta_mean))

    theta = np.asarray(idata.posterior["theta_park"].values, dtype=np.float64)
    assert np.allclose(theta_mean, theta.mean(axis=(0, 1)), atol=1e-9)


def test_held_out_nb_zero_games_short_circuits() -> None:
    inputs = _inputs(held_out_games=0)
    idata = _idata()
    metrics = _evaluate_held_out_nb(inputs, idata)
    assert metrics == {"n_games": 0}


def test_held_out_nb_returns_documented_keys_with_finite_lift() -> None:
    inputs = _inputs(held_out_games=6)
    idata = _idata()
    metrics = _evaluate_held_out_nb(inputs, idata)

    expected_keys = {
        "n_games",
        "mean_loglik",
        "mean_loglik_baseline",
        "loglik_lift",
        "rmse",
        "rmse_baseline",
        "rmse_improvement",
    }
    assert set(metrics) == expected_keys
    assert metrics["n_games"] == 6

    def _f(key: str) -> float:
        value = metrics[key]
        assert isinstance(value, float)
        return value

    assert np.isfinite(_f("loglik_lift"))
    assert np.isfinite(_f("rmse_improvement"))
    assert np.isclose(
        _f("loglik_lift"),
        _f("mean_loglik") - _f("mean_loglik_baseline"),
    )
    assert np.isclose(
        _f("rmse_improvement"),
        _f("rmse_baseline") - _f("rmse"),
    )
