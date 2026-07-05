"""Cell-grain export tests for the pitch-summary multinomial path."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import arviz as az
import numpy as np
import polars as pl

from python_models.statistical.bayes.training import (
    _evaluate_pitch_summary_held_out,
    _export_pitch_summary_summary,
)
from python_models.statistical.models._pitch_summary_data import (
    PitchSummaryHeldOutSet,
    PitchSummaryInputs,
)

CELL_LABELS = ["out_in_play|2023|AL", "strikeout|2023|NL", "strikeout|2024|AL"]
RESULT_BY_CELL = ["out_in_play", "strikeout", "strikeout"]
SEASONS = [2023, 2023, 2024]
LEAGUES = ["AL", "NL", "AL"]
RESULT_FAMILY_LABELS = ["out_in_play", "strikeout"]

N_CHAIN = 2
N_DRAW = 7
N_CELL = len(CELL_LABELS)
N_CLASS = 12


def _class_axes() -> tuple[list[str], list[int], list[int]]:
    labels: list[str] = []
    balls: list[int] = []
    strikes: list[int] = []
    for b in range(4):
        for s in range(3):
            labels.append(f"b{b}_s{s}")
            balls.append(b)
            strikes.append(s)
    return labels, balls, strikes


def _inputs() -> PitchSummaryInputs:
    class_labels, balls_by_class, strikes_by_class = _class_axes()
    result_to_idx = {r: i for i, r in enumerate(RESULT_FAMILY_LABELS)}
    cell_result_idx = np.array(
        [result_to_idx[r] for r in RESULT_BY_CELL], dtype=np.int64
    )
    rng = np.random.default_rng(11)
    counts = rng.integers(2, 9, size=(N_CELL, N_CLASS)).astype(np.int64)
    return PitchSummaryInputs(
        counts=counts,
        cell_result_idx=cell_result_idx,
        cell_labels=list(CELL_LABELS),
        result_by_cell=list(RESULT_BY_CELL),
        season_by_cell=list(SEASONS),
        league_by_cell=list(LEAGUES),
        class_labels=class_labels,
        balls_by_class=balls_by_class,
        strikes_by_class=strikes_by_class,
        result_family_labels=list(RESULT_FAMILY_LABELS),
        outcome="final_count",
        coords={
            "source": ["__single__"],
            "class": list(class_labels),
            "class_nonref": list(class_labels[1:]),
            "result_family": list(RESULT_FAMILY_LABELS),
            "cell": list(CELL_LABELS),
        },
        held_out=PitchSummaryHeldOutSet(
            counts=rng.integers(1, 6, size=(N_CELL, N_CLASS)).astype(np.int64),
            cell_idx=np.arange(N_CELL, dtype=np.int64),
            cell_result_idx=cell_result_idx,
        ),
    )


def _idata() -> az.InferenceData:
    rng = np.random.default_rng(7)
    logits = rng.normal(size=(N_CHAIN, N_DRAW, N_CELL, N_CLASS))
    prob = np.exp(logits)
    prob /= prob.sum(axis=-1, keepdims=True)
    posterior = {
        "cell_class_prob": prob,
        "beta0": rng.normal(size=(N_CHAIN, N_DRAW, N_CLASS - 1)),
        "result_logodds": rng.normal(
            size=(N_CHAIN, N_DRAW, len(RESULT_FAMILY_LABELS), N_CLASS - 1)
        ),
    }
    class_labels, _balls, _strikes = _class_axes()
    coords = {
        "cell": list(CELL_LABELS),
        "class": list(class_labels),
        "class_nonref": list(class_labels[1:]),
        "result_family": list(RESULT_FAMILY_LABELS),
    }
    dims = {
        "cell_class_prob": ["cell", "class"],
        "beta0": ["class_nonref"],
        "result_logodds": ["result_family", "class_nonref"],
    }
    return az.from_dict(posterior=posterior, coords=coords, dims=dims)


def test_summary_export_schema_and_row_count(tmp_path: Path) -> None:
    inputs = _inputs()
    idata = _idata()
    target = tmp_path / "pitch_summary_summary.parquet"
    _export_pitch_summary_summary(idata, inputs, target_path=target)

    df = pl.read_parquet(target)
    assert df.height == N_CELL * N_CLASS
    assert df.schema == {
        "result_family": pl.Utf8,
        "season": pl.Int16,
        "league": pl.Utf8,
        "final_count_class": pl.Utf8,
        "balls": pl.Int8,
        "strikes": pl.Int8,
        "outcome": pl.Utf8,
        "prob_mean": pl.Float64,
        "prob_sd": pl.Float64,
        "prob_hdi_lower": pl.Float64,
        "prob_hdi_upper": pl.Float64,
        "ess_bulk": pl.Float64,
        "rhat": pl.Float64,
    }
    assert df.get_column("outcome").unique().to_list() == ["final_count"]
    assert sorted(df.get_column("final_count_class").unique().to_list()) == sorted(
        inputs.class_labels
    )


def test_summary_one_row_per_cell_class_and_means_match(tmp_path: Path) -> None:
    inputs = _inputs()
    idata = _idata()
    target = tmp_path / "pitch_summary_summary.parquet"
    _export_pitch_summary_summary(idata, inputs, target_path=target)

    df = pl.read_parquet(target)
    grain = df.select("result_family", "season", "league", "final_count_class")
    assert grain.n_unique() == df.height

    prob = np.asarray(idata.posterior["cell_class_prob"].values, dtype=np.float64)
    expected_mean = prob.mean(axis=(0, 1)).reshape(-1)
    assert np.allclose(df.get_column("prob_mean").to_numpy(), expected_mean, atol=1e-9)
    assert np.all(
        df.get_column("prob_hdi_lower").to_numpy()
        <= df.get_column("prob_hdi_upper").to_numpy()
    )

    for k in range(N_CELL):
        cell_rows = df.filter(
            (pl.col("result_family") == RESULT_BY_CELL[k])
            & (pl.col("season") == SEASONS[k])
            & (pl.col("league") == LEAGUES[k])
        )
        assert cell_rows.height == N_CLASS
        for row in cell_rows.iter_rows(named=True):
            assert row["final_count_class"] == f"b{row['balls']}_s{row['strikes']}"


def test_held_out_eval_keys_present() -> None:
    inputs = _inputs()
    idata = _idata()
    metrics = _evaluate_pitch_summary_held_out(inputs, idata)
    assert metrics["n_cells"] == inputs.held_out.n_cells
    assert "loglik_lift" in metrics
    assert "tv_improvement" in metrics


def test_held_out_eval_empty_returns_zero_cells() -> None:
    inputs = _inputs()
    empty = inputs.model_copy(
        update={
            "held_out": PitchSummaryHeldOutSet(
                counts=np.zeros((0, N_CLASS), dtype=np.int64),
                cell_idx=np.zeros(0, dtype=np.int64),
                cell_result_idx=np.zeros(0, dtype=np.int64),
            )
        }
    )
    metrics = _evaluate_pitch_summary_held_out(empty, _idata())
    assert metrics == {"n_cells": 0}


def _jensen_gap_idata() -> az.InferenceData:
    rng = np.random.default_rng(3)
    prob = rng.dirichlet(np.ones(N_CLASS), size=(N_CHAIN, N_DRAW, N_CELL))
    n_result = len(RESULT_FAMILY_LABELS)
    result_lo = np.empty((N_CHAIN, N_DRAW, n_result, N_CLASS - 1))
    half = N_DRAW // 2
    result_lo[:, :half, :, :] = 6.0
    result_lo[:, half:, :, :] = -6.0
    posterior = {
        "cell_class_prob": prob,
        "beta0": rng.normal(size=(N_CHAIN, N_DRAW, N_CLASS - 1)),
        "result_logodds": result_lo,
    }
    class_labels, _balls, _strikes = _class_axes()
    coords = {
        "cell": list(CELL_LABELS),
        "class": list(class_labels),
        "class_nonref": list(class_labels[1:]),
        "result_family": list(RESULT_FAMILY_LABELS),
    }
    dims = {
        "cell_class_prob": ["cell", "class"],
        "beta0": ["class_nonref"],
        "result_logodds": ["result_family", "class_nonref"],
    }
    return az.from_dict(posterior=posterior, coords=coords, dims=dims)


def test_held_out_baseline_is_mean_of_softmax_not_softmax_of_mean() -> None:
    from scipy.special import softmax

    inputs = _inputs()
    idata = _jensen_gap_idata()
    result_lo_draws = np.asarray(
        idata.posterior["result_logodds"].values, dtype=np.float64
    )

    ref_draws = np.zeros(result_lo_draws.shape[:-1] + (1,))
    mean_of_softmax = softmax(
        np.concatenate([ref_draws, result_lo_draws], axis=-1), axis=-1
    ).mean(axis=(0, 1))

    logodds_mean = result_lo_draws.mean(axis=(0, 1))
    softmax_of_mean = softmax(
        np.concatenate([np.zeros((logodds_mean.shape[0], 1)), logodds_mean], axis=1),
        axis=1,
    )
    assert not np.allclose(mean_of_softmax, softmax_of_mean, atol=1e-3)

    counts = inputs.held_out.counts.astype(np.float64)
    total = float(counts.sum())
    eps = 1e-12

    def _baseline_loglik(base_prob: np.ndarray) -> float:
        base = base_prob[inputs.held_out.cell_result_idx]
        return float(np.sum(counts * np.log(base + eps))) / total

    loglik_mean_of_softmax = _baseline_loglik(mean_of_softmax)
    loglik_softmax_of_mean = _baseline_loglik(softmax_of_mean)
    assert not np.isclose(loglik_mean_of_softmax, loglik_softmax_of_mean, atol=1e-6)

    metrics = _evaluate_pitch_summary_held_out(inputs, idata)
    assert np.isclose(
        metrics["loglik_per_event_baseline"], loglik_mean_of_softmax, atol=1e-9
    )
    assert not np.isclose(
        metrics["loglik_per_event_baseline"], loglik_softmax_of_mean, atol=1e-6
    )
