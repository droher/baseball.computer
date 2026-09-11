"""Generic HDI-coverage validator + state_transition realization extractor."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.hdi_coverage import (
    compute_hdi_coverage,
    compute_predictive_coverage,
    compute_predictive_mean_coverage,
    state_transition_held_out_realization,
)
from python_models.statistical.splits import game_hash_fold
from tests.statistical.run_values_fixtures import event_row


def _estimate(n_cells: int, lower: float, upper: float) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "cell": [f"c{i}" for i in range(n_cells)],
            "prob_hdi_lower": [lower] * n_cells,
            "prob_hdi_upper": [upper] * n_cells,
        }
    )


def _realization(freqs: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "cell": [f"c{i}" for i in range(len(freqs))],
            "empirical_freq": freqs,
        }
    )


def test_coverage_inside_band_no_finding() -> None:
    est = _estimate(100, 0.2, 0.8)
    freqs = [0.5] * 94 + [0.95] * 6
    res = compute_hdi_coverage(
        est,
        _realization(freqs),
        model_name="m",
        join_keys=("cell",),
        realization_col="empirical_freq",
    )
    assert res.coverage == pytest.approx(0.94)
    assert res.in_band
    assert res.finding is None
    assert res.n_cells == 100


def test_coverage_outside_band_flags() -> None:
    est = _estimate(50, 0.2, 0.8)
    res = compute_hdi_coverage(
        est,
        _realization([0.95] * 50),
        model_name="m",
        join_keys=("cell",),
        realization_col="empirical_freq",
    )
    assert res.coverage == pytest.approx(0.0)
    assert not res.in_band
    assert res.finding is not None
    assert res.finding.code == "hdi_coverage_out_of_band"


def test_coverage_full_is_above_band() -> None:
    est = _estimate(20, 0.0, 1.0)
    res = compute_hdi_coverage(
        est,
        _realization([0.5] * 20),
        model_name="m",
        join_keys=("cell",),
        realization_col="empirical_freq",
    )
    assert res.coverage == pytest.approx(1.0)
    assert not res.in_band
    assert res.finding is not None


def test_missing_estimate_column_raises() -> None:
    est = pl.DataFrame({"cell": ["c0"], "prob_hdi_lower": [0.1]})
    with pytest.raises(ValueError):
        _ = compute_hdi_coverage(
            est,
            _realization([0.5]),
            model_name="m",
            join_keys=("cell",),
            realization_col="empirical_freq",
        )


def test_no_join_overlap_reports_no_cells() -> None:
    est = _estimate(3, 0.2, 0.8)
    real = pl.DataFrame({"cell": ["z0", "z1"], "empirical_freq": [0.5, 0.5]})
    res = compute_hdi_coverage(
        est,
        real,
        model_name="m",
        join_keys=("cell",),
        realization_col="empirical_freq",
    )
    assert res.n_cells == 0
    assert res.coverage is None
    assert res.finding is not None
    assert res.finding.code == "hdi_coverage_no_cells"


def _held_out_game_ids(count: int) -> list[str]:
    out: list[str] = []
    i = 0
    while len(out) < count:
        gid = f"GAME{i:06d}"
        if game_hash_fold(gid, fold_count=10) == 0:
            out.append(gid)
        i += 1
    return out


def test_state_transition_realization_recovers_frequencies(tmp_path: Path) -> None:
    games = _held_out_game_ids(3)
    rows: list[dict[str, object]] = []
    for gid in games:
        for _ in range(20):
            rows.append(
                event_row(
                    game_id=gid,
                    season=2015,
                    league="NL",
                    outs=0,
                    base=1,
                    end_outs=0,
                    end_base=2,
                )
            )
        for _ in range(20):
            rows.append(
                event_row(
                    game_id=gid,
                    season=2015,
                    league="NL",
                    outs=0,
                    base=1,
                    end_outs=3,
                    end_base=0,
                )
            )
    parquet = tmp_path / "dataset.parquet"
    pl.DataFrame(rows).write_parquet(parquet)

    realization = state_transition_held_out_realization(parquet, min_cell_events=25)
    cell = realization.filter(
        (pl.col("start_state") == "0_1")
        & (pl.col("season") == 2015)
        & (pl.col("league") == "NL")
    )
    freq = dict(
        zip(
            cell.get_column("end_class").to_list(),
            cell.get_column("empirical_freq").to_list(),
        )
    )
    assert freq["0_2"] == pytest.approx(0.5)
    assert freq["inning_end"] == pytest.approx(0.5)
    assert freq["0_0"] == pytest.approx(0.0)
    assert "1_0" in freq


def _multinomial_predictive_frames(
    n_cells: int,
    means: list[float],
    *,
    prob_sd: float,
    n_events: int,
    gen_seed: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Estimate + realization where each realization is one draw of the model."""
    k = len(means)
    m = np.asarray(means, dtype=np.float64)
    rng = np.random.default_rng(gen_seed)
    est_rows: list[dict[str, object]] = []
    real_rows: list[dict[str, object]] = []
    for c in range(n_cells):
        p = np.clip(rng.normal(m, prob_sd), 0.0, 1.0)
        p = p / p.sum()
        counts = rng.multinomial(n_events, p)
        freq = counts.astype(np.float64) / n_events
        for j in range(k):
            est_rows.append(
                {
                    "cell": f"c{c}",
                    "end_class": f"k{j}",
                    "prob_mean": float(m[j]),
                    "prob_sd": prob_sd,
                }
            )
            real_rows.append(
                {
                    "cell": f"c{c}",
                    "end_class": f"k{j}",
                    "empirical_freq": float(freq[j]),
                    "cell_total": n_events,
                }
            )
    return pl.DataFrame(est_rows), pl.DataFrame(real_rows)


def test_predictive_coverage_same_model_is_nominal() -> None:
    est, real = _multinomial_predictive_frames(
        200, [0.4, 0.3, 0.2, 0.1], prob_sd=0.01, n_events=200, gen_seed=12345
    )
    res = compute_predictive_coverage(
        est,
        real,
        model_name="m",
        cell_keys=("cell",),
        class_key="end_class",
        realization_col="empirical_freq",
        cell_size_col="cell_total",
    )
    assert res.coverage_kind == "predictive"
    assert res.n_cells == 800
    assert res.coverage is not None
    assert 0.90 <= res.coverage <= 0.98
    assert res.in_band
    assert res.finding is None


def test_predictive_coverage_shifted_collapses() -> None:
    est, real = _multinomial_predictive_frames(
        60, [0.4, 0.3, 0.2, 0.1], prob_sd=0.01, n_events=200, gen_seed=7
    )
    shifted = real.with_columns(
        pl.when(pl.col("end_class") == "k0")
        .then(pl.lit(1.0))
        .otherwise(pl.lit(0.0))
        .alias("empirical_freq")
    )
    res = compute_predictive_coverage(
        est,
        shifted,
        model_name="m",
        cell_keys=("cell",),
        class_key="end_class",
        realization_col="empirical_freq",
        cell_size_col="cell_total",
    )
    assert res.coverage is not None
    assert res.coverage < 0.1
    assert not res.in_band
    assert res.finding is not None
    assert res.finding.code == "predictive_coverage_out_of_band"


def test_predictive_coverage_incomplete_class_set_raises() -> None:
    est, real = _multinomial_predictive_frames(
        5, [0.4, 0.3, 0.2, 0.1], prob_sd=0.01, n_events=100, gen_seed=3
    )
    truncated = real.filter(pl.col("end_class") != "k0")
    with pytest.raises(ValueError, match="class sets diverge"):
        _ = compute_predictive_coverage(
            est,
            truncated,
            model_name="m",
            cell_keys=("cell",),
            class_key="end_class",
            realization_col="empirical_freq",
            cell_size_col="cell_total",
        )


def test_predictive_coverage_deterministic() -> None:
    est, real = _multinomial_predictive_frames(
        40, [0.5, 0.3, 0.2], prob_sd=0.02, n_events=150, gen_seed=99
    )
    a = compute_predictive_coverage(
        est,
        real,
        model_name="m",
        cell_keys=("cell",),
        class_key="end_class",
        realization_col="empirical_freq",
        cell_size_col="cell_total",
    )
    b = compute_predictive_coverage(
        est,
        real,
        model_name="m",
        cell_keys=("cell",),
        class_key="end_class",
        realization_col="empirical_freq",
        cell_size_col="cell_total",
    )
    assert a.coverage == b.coverage


def _mean_predictive_frames(
    n_cells: int,
    *,
    re_mean: float,
    re_sd: float,
    sample_sd: float,
    n_events: int,
    gen_seed: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    rng = np.random.default_rng(gen_seed)
    total_sd = float(np.sqrt(re_sd**2 + sample_sd**2 / n_events))
    est_rows: list[dict[str, object]] = []
    real_rows: list[dict[str, object]] = []
    for c in range(n_cells):
        est_rows.append(
            {"state": f"s{c}", "re_value_mean": re_mean, "re_value_sd": re_sd}
        )
        real_rows.append(
            {
                "state": f"s{c}",
                "held_out_mean": float(rng.normal(re_mean, total_sd)),
                "held_out_sd": sample_sd,
                "cell_total": n_events,
            }
        )
    return pl.DataFrame(est_rows), pl.DataFrame(real_rows)


def test_predictive_mean_coverage_same_model_is_nominal() -> None:
    est, real = _mean_predictive_frames(
        400, re_mean=0.5, re_sd=0.01, sample_sd=1.0, n_events=300, gen_seed=2024
    )
    res = compute_predictive_mean_coverage(
        est,
        real,
        model_name="re",
        join_keys=("state",),
        realization_col="held_out_mean",
        cell_size_col="cell_total",
        sample_sd_col="held_out_sd",
        mean_col="re_value_mean",
        sd_col="re_value_sd",
    )
    assert res.coverage is not None
    assert res.coverage_kind == "studentized_mean"
    assert 0.90 <= res.coverage <= 0.98
    assert res.in_band
    assert res.finding is None


def test_predictive_mean_coverage_shifted_collapses() -> None:
    est, real = _mean_predictive_frames(
        100, re_mean=0.5, re_sd=0.01, sample_sd=1.0, n_events=300, gen_seed=5
    )
    shifted = real.with_columns(
        (pl.col("held_out_mean") + 100.0).alias("held_out_mean")
    )
    res = compute_predictive_mean_coverage(
        est,
        shifted,
        model_name="re",
        join_keys=("state",),
        realization_col="held_out_mean",
        cell_size_col="cell_total",
        sample_sd_col="held_out_sd",
        mean_col="re_value_mean",
        sd_col="re_value_sd",
    )
    assert res.coverage == pytest.approx(0.0)
    assert not res.in_band
    assert res.finding is not None
    assert res.finding.code == "studentized_mean_coverage_out_of_band"
