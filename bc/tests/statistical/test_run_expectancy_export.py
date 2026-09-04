"""Cell-grain export tests for the run-expectancy count path."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import arviz as az
import numpy as np
import polars as pl

from python_models.statistical.bayes.training import (
    _export_run_expectancy_posterior,
    _export_run_expectancy_summary,
)
from python_models.statistical.models._run_values_data import (
    RunExpectancyHeldOutSet,
    RunExpectancyInputs,
    _build_era_regime_design,
)

CELL_LABELS = ["1933|AL|0_0", "1933|NL|1_3", "1934|AL|2_7"]
STATE_LABELS = ["0_0", "1_3", "2_7"]
OUTS = [0, 1, 2]
BASE = [0, 3, 7]
SEASONS = [1933, 1933, 1934]
LEAGUES = ["AL", "NL", "AL"]

N_CHAIN = 2
N_DRAW = 7
N_CELL = len(CELL_LABELS)


def _inputs() -> RunExpectancyInputs:
    cell_era_design, era_labels = _build_era_regime_design(
        list(SEASONS), list(LEAGUES)
    )
    return RunExpectancyInputs(
        sum_runs=np.array([10, 4, 2], dtype=np.int64),
        cell_event_count=np.array([40, 30, 35], dtype=np.int64),
        cell_state_idx=np.arange(N_CELL, dtype=np.int64),
        cell_era_design=cell_era_design,
        cell_labels=list(CELL_LABELS),
        era_labels=list(era_labels),
        state_by_cell=[0, 11, 23],
        outs_by_cell=list(OUTS),
        base_state_by_cell=list(BASE),
        season_by_cell=list(SEASONS),
        league_by_cell=list(LEAGUES),
        state_labels=list(STATE_LABELS),
        outcome="runs_to_end",
        coords={
            "source": ["__single__"],
            "state": list(STATE_LABELS),
            "cell": list(CELL_LABELS),
            "era_regime": list(era_labels),
        },
        held_out=RunExpectancyHeldOutSet(
            sum_runs=np.zeros(0, dtype=np.int64),
            cell_event_count=np.zeros(0, dtype=np.int64),
            cell_idx=np.zeros(0, dtype=np.int64),
            cell_state_idx=np.zeros(0, dtype=np.int64),
        ),
    )


def _idata() -> az.InferenceData:
    rng = np.random.default_rng(7)
    posterior = {
        "re_value": rng.gamma(2.0, 0.5, size=(N_CHAIN, N_DRAW, N_CELL)),
        "mu_state": rng.normal(size=(N_CHAIN, N_DRAW, len(STATE_LABELS))),
        "phi": rng.gamma(5.0, 1.0, size=(N_CHAIN, N_DRAW, len(STATE_LABELS))),
    }
    coords = {
        "cell": list(CELL_LABELS),
        "state": list(STATE_LABELS),
    }
    dims = {
        "re_value": ["cell"],
        "mu_state": ["state"],
        "phi": ["state"],
    }
    return az.from_dict(posterior=posterior, coords=coords, dims=dims)


def test_posterior_export_schema_and_row_count(tmp_path: Path) -> None:
    inputs = _inputs()
    idata = _idata()
    target = tmp_path / "run_expectancy_posterior.parquet"
    _export_run_expectancy_posterior(idata, inputs, target_path=target)

    df = pl.read_parquet(target)
    assert df.height == N_CELL * N_CHAIN * N_DRAW
    assert df.schema == {
        "state": pl.Utf8,
        "season": pl.Int16,
        "league": pl.Utf8,
        "outcome": pl.Utf8,
        "value": pl.Float64,
        "chain": pl.Int16,
        "draw": pl.Int32,
    }
    assert df.get_column("outcome").unique().to_list() == ["runs_to_end"]
    assert sorted(df.get_column("state").unique().to_list()) == sorted(STATE_LABELS)
    assert df.get_column("chain").max() == N_CHAIN - 1
    assert df.get_column("draw").max() == N_DRAW - 1

    value = np.asarray(idata.posterior["re_value"].values, dtype=np.float64)
    cell_0 = df.filter(
        (pl.col("state") == STATE_LABELS[0])
        & (pl.col("season") == SEASONS[0])
        & (pl.col("league") == LEAGUES[0])
    ).sort(["chain", "draw"])
    assert np.allclose(
        cell_0.get_column("value").to_numpy(),
        value[:, :, 0].reshape(-1),
    )


def test_summary_export_schema_and_one_row_per_cell(tmp_path: Path) -> None:
    inputs = _inputs()
    idata = _idata()
    target = tmp_path / "run_expectancy_summary.parquet"
    _export_run_expectancy_summary(idata, inputs, target_path=target)

    df = pl.read_parquet(target)
    assert df.height == N_CELL
    assert df.schema == {
        "state": pl.Utf8,
        "base_state": pl.Int8,
        "outs": pl.Int8,
        "season": pl.Int16,
        "league": pl.Utf8,
        "outcome": pl.Utf8,
        "re_value_mean": pl.Float64,
        "re_value_sd": pl.Float64,
        "re_value_hdi_lower": pl.Float64,
        "re_value_hdi_upper": pl.Float64,
        "ess_bulk": pl.Float64,
        "rhat": pl.Float64,
    }

    assert df.get_column("season").to_list() == SEASONS
    assert df.get_column("league").to_list() == LEAGUES
    assert df.get_column("outs").to_list() == OUTS
    assert df.get_column("base_state").to_list() == BASE
    for k in range(N_CELL):
        row = df.row(k, named=True)
        assert row["state"] == STATE_LABELS[k]
        rebuilt = f"{row['season']}|{row['league']}|{row['outs']}_{row['base_state']}"
        assert rebuilt == CELL_LABELS[k]

    value = np.asarray(idata.posterior["re_value"].values, dtype=np.float64)
    assert np.allclose(
        df.get_column("re_value_mean").to_numpy(),
        value.mean(axis=(0, 1)),
        atol=1e-9,
    )
    assert np.all(
        df.get_column("re_value_hdi_lower").to_numpy()
        <= df.get_column("re_value_hdi_upper").to_numpy()
    )
