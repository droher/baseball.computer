"""Cell-grain export tests for the pitch-summary multinomial path."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import arviz as az
import numpy as np
import polars as pl
import pytest
from scipy.special import softmax

from python_models.statistical.bayes.training import (
    _evaluate_pitch_summary_held_out,
    _export_pitch_summary_summary,
)
from python_models.statistical.models._pitch_summary_data import (
    PitchSummaryHeldOutSet,
    PitchSummaryInputs,
    _reachable_mask,
    _reference_by_result,
)
from python_models.statistical.models.pitch_summary import MASK_LOGIT

CELL_LABELS = [
    "out_in_play|2023|AL",
    "strikeout|2023|NL",
    "strikeout|2024|AL",
    "walk|2023|AL",
]
RESULT_BY_CELL = ["out_in_play", "strikeout", "strikeout", "walk"]
SEASONS = [2023, 2023, 2024, 2023]
LEAGUES = ["AL", "NL", "AL", "AL"]
RESULT_FAMILY_LABELS = ["out_in_play", "strikeout", "walk"]

N_CHAIN = 2
N_DRAW = 7
N_CELL = len(CELL_LABELS)
N_CLASS = 12
N_RESULT = len(RESULT_FAMILY_LABELS)


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


def _cell_result_idx() -> np.ndarray:
    result_to_idx = {r: i for i, r in enumerate(RESULT_FAMILY_LABELS)}
    return np.array([result_to_idx[r] for r in RESULT_BY_CELL], dtype=np.int64)


def _inputs() -> PitchSummaryInputs:
    class_labels, balls_by_class, strikes_by_class = _class_axes()
    cell_result_idx = _cell_result_idx()
    reachable = _reachable_mask(RESULT_FAMILY_LABELS)
    rng = np.random.default_rng(11)
    counts = (
        rng.integers(2, 9, size=(N_CELL, N_CLASS)) * reachable[cell_result_idx]
    ).astype(np.int64)
    held_counts = (
        rng.integers(1, 6, size=(N_CELL, N_CLASS)) * reachable[cell_result_idx]
    ).astype(np.int64)
    return PitchSummaryInputs(
        counts=counts,
        cell_result_idx=cell_result_idx,
        reachable_mask=reachable,
        ref_class_by_result=_reference_by_result(counts, cell_result_idx, reachable),
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
            "result_family": list(RESULT_FAMILY_LABELS),
            "cell": list(CELL_LABELS),
        },
        held_out=PitchSummaryHeldOutSet(
            counts=held_counts,
            cell_idx=np.arange(N_CELL, dtype=np.int64),
            cell_result_idx=cell_result_idx,
        ),
    )


def _masked_softmax_draws(
    rng: np.random.Generator, reachable_rows: np.ndarray, *, scale: float = 1.0
) -> np.ndarray:
    n_rows = reachable_rows.shape[0]
    logits = rng.normal(scale=scale, size=(N_CHAIN, N_DRAW, n_rows, N_CLASS))
    logits = np.where(reachable_rows[None, None, :, :], logits, MASK_LOGIT)
    return softmax(logits, axis=-1)


def _idata(inputs: PitchSummaryInputs, *, seed: int = 7) -> az.InferenceData:
    rng = np.random.default_rng(seed)
    cell_prob = _masked_softmax_draws(rng, inputs.cell_reachable_mask)
    result_prob = _masked_softmax_draws(rng, inputs.reachable_mask)
    posterior = {
        "cell_class_prob": cell_prob,
        "result_class_prob": result_prob,
    }
    coords = {
        "cell": list(CELL_LABELS),
        "class": list(inputs.class_labels),
        "result_family": list(RESULT_FAMILY_LABELS),
    }
    dims = {
        "cell_class_prob": ["cell", "class"],
        "result_class_prob": ["result_family", "class"],
    }
    return az.from_dict(posterior=posterior, coords=coords, dims=dims)


def test_summary_export_schema_and_row_count(tmp_path: Path) -> None:
    inputs = _inputs()
    idata = _idata(inputs)
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
    idata = _idata(inputs)
    target = tmp_path / "pitch_summary_summary.parquet"
    _export_pitch_summary_summary(idata, inputs, target_path=target)

    df = pl.read_parquet(target)
    grain = df.select("result_family", "season", "league", "final_count_class")
    assert grain.n_unique() == df.height

    prob = np.asarray(idata.posterior["cell_class_prob"].values, dtype=np.float64)
    reachable = inputs.cell_reachable_mask.reshape(-1)
    expected_mean = np.where(reachable, prob.mean(axis=(0, 1)).reshape(-1), 0.0)
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


def test_summary_export_zeroes_impossible_classes_and_rows_sum_to_one(
    tmp_path: Path,
) -> None:
    inputs = _inputs()
    idata = _idata(inputs)
    target = tmp_path / "pitch_summary_summary.parquet"
    _export_pitch_summary_summary(idata, inputs, target_path=target)

    df = pl.read_parquet(target)
    class_index = {label: j for j, label in enumerate(inputs.class_labels)}
    result_index = {r: i for i, r in enumerate(inputs.result_family_labels)}
    reachable_flags = np.array(
        [
            bool(
                inputs.reachable_mask[
                    result_index[row["result_family"]],
                    class_index[row["final_count_class"]],
                ]
            )
            for row in df.iter_rows(named=True)
        ]
    )
    assert (~reachable_flags).any()

    impossible = df.filter(pl.Series(~reachable_flags))
    for column in ("prob_mean", "prob_sd", "prob_hdi_lower", "prob_hdi_upper"):
        assert (impossible.get_column(column) == 0.0).all(), column
    assert impossible.get_column("ess_bulk").is_null().all()
    assert impossible.get_column("rhat").is_null().all()

    possible = df.filter(pl.Series(reachable_flags))
    assert (possible.get_column("prob_mean") > 0.0).all()
    assert possible.get_column("ess_bulk").is_not_null().all()

    sums = df.group_by(["result_family", "season", "league"]).agg(
        pl.col("prob_mean").sum().alias("total")
    )
    assert np.allclose(sums.get_column("total").to_numpy(), 1.0, atol=1e-9)


def test_held_out_eval_keys_present() -> None:
    inputs = _inputs()
    idata = _idata(inputs)
    metrics = _evaluate_pitch_summary_held_out(inputs, idata)
    assert metrics["n_cells"] == inputs.held_out.n_cells
    assert "loglik_lift" in metrics
    assert "tv_improvement" in metrics


def test_held_out_eval_masks_impossible_counts_with_the_builder_mask() -> None:
    inputs = _inputs()
    idata = _idata(inputs)
    clean = _evaluate_pitch_summary_held_out(inputs, idata)

    dirty_counts = inputs.held_out.counts.copy()
    cell_reachable = inputs.reachable_mask[inputs.held_out.cell_result_idx]
    dirty_counts[~cell_reachable] = 7
    dirty = inputs.model_copy(
        update={
            "held_out": inputs.held_out.model_copy(update={"counts": dirty_counts})
        }
    )
    assert _evaluate_pitch_summary_held_out(dirty, idata) == clean
    assert clean["n_events"] == int(inputs.held_out.counts.sum())


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
    metrics = _evaluate_pitch_summary_held_out(empty, _idata(inputs))
    assert metrics == {"n_cells": 0}


def test_held_out_baseline_is_posterior_mean_of_result_class_prob() -> None:
    inputs = _inputs()
    idata = _idata(inputs, seed=3)
    result_draws = np.asarray(
        idata.posterior["result_class_prob"].values, dtype=np.float64
    )
    mean_of_prob = result_draws.mean(axis=(0, 1))

    counts = inputs.held_out.counts.astype(np.float64)
    total = float(counts.sum())
    eps = 1e-12
    base = mean_of_prob[inputs.held_out.cell_result_idx]
    expected = float(np.sum(counts * np.log(base + eps))) / total

    metrics = _evaluate_pitch_summary_held_out(inputs, idata)
    baseline = metrics["loglik_per_event_baseline"]
    assert isinstance(baseline, float)
    assert baseline == pytest.approx(expected, abs=1e-9)
