"""Empirical coverage validation for aggregate posterior surfaces.

Two coverage flavors ship here. ``compute_hdi_coverage`` (parameter
coverage) is generic over an estimate table (mean / sd / HDI columns per
cell) and a held-out realization frame (one empirical value per cell),
joined on arbitrary keys; it reports the fraction of cells whose held-out
realization lands inside the estimate's parameter HDI. ``compute_predictive_coverage``
(multinomial) and ``compute_predictive_mean_coverage`` (count/mean)
instead simulate a posterior-predictive interval per held-out cell that
folds in finite-sample noise at the cell's own event count, so dense
cells are no longer graded against a parameter HDI that is by
construction much tighter than the held-out frequency's sampling spread.

The per-model realization extractor is model-specific; this module ships
the ``state_transition`` and ``run_expectancy`` extractors end to end
(recomputing held-out cell realizations from the model's dataset parquet).
Other aggregate surfaces (park factors) reuse the generic entry points by
supplying their own realization frame.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import polars as pl
from pydantic import BaseModel

from python_models.statistical.models._run_values_data import (
    filter_event_population,
)
from python_models.statistical.schemas import ValidationFinding
from python_models.statistical.splits import game_hash_fold

_log = logging.getLogger(__name__)

DEFAULT_COVERAGE_BAND: tuple[float, float] = (0.88, 0.99)
HDI_PROB: float = 0.94

_HOLDOUT_FOLD_COUNT: int = 10
_HOLDOUT_FOLD_ID: int = 0
_MIN_CELL_EVENTS: int = 25
_N_OUTS: int = 3
_N_BASES: int = 8
_INNING_END_LABEL: str = "inning_end"

_PREDICTIVE_N_SIMS: int = 2000
_PREDICTIVE_SEED: int = 20260714
_MEAN_SUM_TOLERANCE: float = 0.05


class HdiCoverageResult(BaseModel):
    model_name: str
    n_cells: int
    coverage: float | None
    coverage_band: tuple[float, float]
    in_band: bool
    coverage_kind: str = "parameter"
    finding: ValidationFinding | None = None


def compute_hdi_coverage(
    estimate: pl.DataFrame,
    realization: pl.DataFrame,
    *,
    model_name: str,
    join_keys: tuple[str, ...],
    realization_col: str,
    hdi_lower_col: str = "prob_hdi_lower",
    hdi_upper_col: str = "prob_hdi_upper",
    coverage_band: tuple[float, float] = DEFAULT_COVERAGE_BAND,
) -> HdiCoverageResult:
    """Empirical HDI coverage of ``estimate`` cells against ``realization``.

    Inner-joins the two frames on ``join_keys``, then reports the fraction
    of matched cells whose ``realization_col`` value lands within
    ``[hdi_lower_col, hdi_upper_col]``. A coverage outside ``coverage_band``
    yields a ``warn`` finding.
    """
    missing_est = [
        c
        for c in (*join_keys, hdi_lower_col, hdi_upper_col)
        if c not in estimate.columns
    ]
    if missing_est:
        raise ValueError(f"estimate frame missing columns {missing_est}")
    missing_real = [c for c in (*join_keys, realization_col) if c not in realization.columns]
    if missing_real:
        raise ValueError(f"realization frame missing columns {missing_real}")

    joined = estimate.join(realization, on=list(join_keys), how="inner")
    n_cells = int(joined.height)
    if n_cells == 0:
        finding = ValidationFinding(
            severity="warn",
            code="hdi_coverage_no_cells",
            message=(
                f"no cells joined between estimate and realization for "
                f"{model_name}; HDI coverage is unverifiable"
            ),
        )
        return HdiCoverageResult(
            model_name=model_name,
            n_cells=0,
            coverage=None,
            coverage_band=coverage_band,
            in_band=False,
            finding=finding,
        )

    n_inside = int(
        joined.filter(
            (pl.col(realization_col) >= pl.col(hdi_lower_col))
            & (pl.col(realization_col) <= pl.col(hdi_upper_col))
        ).height
    )
    coverage = n_inside / n_cells
    low, high = coverage_band
    in_band = low <= coverage <= high
    finding: ValidationFinding | None = None
    if not in_band:
        finding = ValidationFinding(
            severity="warn",
            code="hdi_coverage_out_of_band",
            message=(
                f"{model_name} empirical {HDI_PROB:.0%} HDI coverage={coverage:.4f} "
                f"over {n_cells} held-out cells is outside the acceptance band "
                f"[{low}, {high}]"
            ),
        )
    _log.info(
        "hdi_coverage model=%s coverage=%.4f n_cells=%d in_band=%s",
        model_name,
        coverage,
        n_cells,
        in_band,
    )
    return HdiCoverageResult(
        model_name=model_name,
        n_cells=n_cells,
        coverage=coverage,
        coverage_band=coverage_band,
        in_band=in_band,
        finding=finding,
    )


def _suffix_expr(column: str) -> pl.Expr:
    return pl.col(column).str.split("_").list.tail(2).list.join("_")


def _reachable_end_classes(start_suffix: str) -> list[str]:
    start_outs = int(start_suffix.split("_", 1)[0])
    labels = [
        f"{eo}_{eb}"
        for eo in range(_N_OUTS)
        for eb in range(_N_BASES)
        if eo >= start_outs
    ]
    labels.append(_INNING_END_LABEL)
    return labels


def state_transition_held_out_realization(
    dataset_parquet: Path,
    *,
    min_cell_events: int = _MIN_CELL_EVENTS,
) -> pl.DataFrame:
    """Recompute held-out cell class frequencies for the transition model.

    Applies the fit's population filter and the same deterministic 10% game
    holdout the fit uses, then materializes, per qualifying
    ``(start_state, season, league)`` cell, the empirical frequency of every
    reachable ``end_class`` (zeros included). Cells with fewer than
    ``min_cell_events`` held-out events are dropped as too noisy to grade.
    """
    end_outs = (
        pl.col("run_expectancy_end_key").str.split("_").list.tail(2).list.first()
    ).cast(pl.Int32)
    end_suffix = _suffix_expr("run_expectancy_end_key")
    scanned = (
        filter_event_population(
            pl.scan_parquet(dataset_parquet), label="state_transition realization"
        )
        .filter(
            pl.col("game_id").is_not_null()
            & pl.col("run_expectancy_start_key").is_not_null()
            & pl.col("run_expectancy_end_key").is_not_null()
        )
        .select(
            pl.col("game_id"),
            pl.col("season").cast(pl.Int64).alias("season"),
            pl.col("league").cast(pl.Utf8).alias("league"),
            _suffix_expr("run_expectancy_start_key").alias("start_state"),
            pl.when(end_outs >= _N_OUTS)
            .then(pl.lit(_INNING_END_LABEL))
            .otherwise(end_suffix)
            .alias("end_class"),
        )
        .collect()
    )
    if scanned.height == 0:
        raise ValueError(f"no transition rows in {dataset_parquet}")

    game_ids = scanned.get_column("game_id").unique().to_list()
    held_games = [
        g
        for g in game_ids
        if game_hash_fold(str(g), fold_count=_HOLDOUT_FOLD_COUNT) == _HOLDOUT_FOLD_ID
    ]
    held = scanned.filter(pl.col("game_id").is_in(held_games))
    counts = held.group_by(["season", "league", "start_state", "end_class"]).agg(
        pl.len().alias("n")
    )
    totals = counts.group_by(["season", "league", "start_state"]).agg(
        pl.col("n").sum().alias("cell_total")
    )
    kept = totals.filter(pl.col("cell_total") >= min_cell_events)

    count_map = {
        (row[0], row[1], row[2], row[3]): row[4]
        for row in counts.select(
            ["season", "league", "start_state", "end_class", "n"]
        ).iter_rows()
    }

    seasons: list[int] = []
    leagues: list[str] = []
    starts: list[str] = []
    end_classes: list[str] = []
    freqs: list[float] = []
    cell_totals: list[int] = []
    for season, league, start_state, cell_total in kept.select(
        ["season", "league", "start_state", "cell_total"]
    ).iter_rows():
        total = float(cell_total)
        for end_class in _reachable_end_classes(str(start_state)):
            n = count_map.get((season, league, start_state, end_class), 0)
            seasons.append(int(season))
            leagues.append(str(league))
            starts.append(str(start_state))
            end_classes.append(end_class)
            freqs.append(n / total)
            cell_totals.append(int(cell_total))

    realization = pl.DataFrame(
        {
            "start_state": pl.Series("start_state", starts, dtype=pl.Utf8),
            "season": pl.Series("season", seasons, dtype=pl.Int64),
            "league": pl.Series("league", leagues, dtype=pl.Utf8),
            "end_class": pl.Series("end_class", end_classes, dtype=pl.Utf8),
            "empirical_freq": pl.Series("empirical_freq", freqs, dtype=pl.Float64),
            "cell_total": pl.Series("cell_total", cell_totals, dtype=pl.Int64),
        }
    )
    _log.info(
        "state_transition realization cells=%d held_out_games=%d rows=%d",
        kept.height,
        len(held_games),
        realization.height,
    )
    return realization


def state_transition_coverage_pair(
    summary_parquet: Path,
    dataset_parquet: Path,
    *,
    min_cell_events: int = _MIN_CELL_EVENTS,
    coverage_band: tuple[float, float] = DEFAULT_COVERAGE_BAND,
) -> tuple[HdiCoverageResult, HdiCoverageResult]:
    """(predictive, parameter) coverage for ``state_transition_summary``.

    Builds the held-out realization once and grades it two ways: against
    per-cell posterior-predictive intervals and against the published
    parameter HDIs.
    """
    estimate = pl.read_parquet(summary_parquet).with_columns(
        pl.col("season").cast(pl.Int64)
    )
    realization = state_transition_held_out_realization(
        dataset_parquet, min_cell_events=min_cell_events
    )
    predictive = compute_predictive_coverage(
        estimate,
        realization,
        model_name="state_transition",
        cell_keys=("start_state", "season", "league"),
        class_key="end_class",
        realization_col="empirical_freq",
        cell_size_col="cell_total",
        mean_col="prob_mean",
        sd_col="prob_sd",
        coverage_band=coverage_band,
    )
    parameter = compute_hdi_coverage(
        estimate,
        realization,
        model_name="state_transition",
        join_keys=("start_state", "season", "league", "end_class"),
        realization_col="empirical_freq",
        hdi_lower_col="prob_hdi_lower",
        hdi_upper_col="prob_hdi_upper",
        coverage_band=coverage_band,
    )
    return predictive, parameter


def _coverage_finding(
    model_name: str,
    coverage: float,
    n_cells: int,
    coverage_band: tuple[float, float],
    *,
    kind: str,
    code_prefix: str,
) -> ValidationFinding | None:
    low, high = coverage_band
    if low <= coverage <= high:
        return None
    return ValidationFinding(
        severity="warn",
        code=f"{code_prefix}_out_of_band",
        message=(
            f"{model_name} empirical {HDI_PROB:.0%} {kind} coverage={coverage:.4f} "
            f"over {n_cells} held-out comparison points is outside the acceptance "
            f"band [{low}, {high}]"
        ),
    )


def _no_cells_result(
    model_name: str,
    coverage_band: tuple[float, float],
    *,
    kind: str,
    code_prefix: str,
) -> HdiCoverageResult:
    return HdiCoverageResult(
        model_name=model_name,
        n_cells=0,
        coverage=None,
        coverage_band=coverage_band,
        in_band=False,
        coverage_kind=kind,
        finding=ValidationFinding(
            severity="warn",
            code=f"{code_prefix}_no_cells",
            message=(
                f"no cells joined between estimate and realization for "
                f"{model_name}; {kind} coverage is unverifiable"
            ),
        ),
    )


def compute_predictive_coverage(
    estimate: pl.DataFrame,
    realization: pl.DataFrame,
    *,
    model_name: str,
    cell_keys: tuple[str, ...],
    class_key: str,
    realization_col: str,
    cell_size_col: str,
    mean_col: str = "prob_mean",
    sd_col: str = "prob_sd",
    n_sims: int = _PREDICTIVE_N_SIMS,
    seed: int = _PREDICTIVE_SEED,
    hdi_prob: float = HDI_PROB,
    coverage_band: tuple[float, float] = DEFAULT_COVERAGE_BAND,
) -> HdiCoverageResult:
    """Posterior-predictive coverage for a per-cell multinomial surface.

    For every held-out cell with ``cell_size_col`` events, draws ``n_sims``
    posterior-predictive frequency vectors and forms the per-(cell, class)
    predictive interval at ``hdi_prob``. Reports the fraction of
    (cell, class) points whose held-out ``realization_col`` frequency lands
    inside its predictive interval. Each draw combines a per-class parameter
    draw from ``mean_col`` / ``sd_col`` with an exact size-``cell_size_col``
    multinomial.
    """
    join_keys = (*cell_keys, class_key)
    missing_est = [c for c in (*join_keys, mean_col, sd_col) if c not in estimate.columns]
    if missing_est:
        raise ValueError(f"estimate frame missing columns {missing_est}")
    missing_real = [
        c for c in (*join_keys, realization_col, cell_size_col)
        if c not in realization.columns
    ]
    if missing_real:
        raise ValueError(f"realization frame missing columns {missing_real}")

    joined = estimate.join(realization, on=list(join_keys), how="inner").sort(
        [*cell_keys, class_key]
    )
    n_points = int(joined.height)
    if n_points == 0:
        return _no_cells_result(
            model_name, coverage_band, kind="predictive", code_prefix="predictive_coverage"
        )

    rng = np.random.default_rng(seed)
    lo_q = (1.0 - hdi_prob) / 2.0
    hi_q = 1.0 - lo_q
    n_inside = 0
    for cell_key, cell_df in joined.group_by(cell_keys, maintain_order=True):
        means = cell_df.get_column(mean_col).to_numpy().astype(np.float64)
        sds = cell_df.get_column(sd_col).to_numpy().astype(np.float64)
        emp = cell_df.get_column(realization_col).to_numpy().astype(np.float64)
        n_events = int(cell_df.get_column(cell_size_col).to_numpy()[0])
        k = means.shape[0]
        mean_sum = float(means.sum())
        if abs(mean_sum - 1.0) > _MEAN_SUM_TOLERANCE:
            raise ValueError(
                f"{model_name} cell {cell_key} joined class means sum to "
                f"{mean_sum:.4f}; the estimate/realization class sets diverge "
                f"and renormalizing would bias predictive coverage"
            )

        probs = rng.normal(means, np.maximum(sds, 0.0), size=(n_sims, k))
        np.clip(probs, 0.0, 1.0, out=probs)
        row_sums = probs.sum(axis=1)
        degenerate = row_sums <= 0.0
        if degenerate.any():
            fallback = means / means.sum() if means.sum() > 0 else np.full(k, 1.0 / k)
            probs[degenerate] = fallback
            row_sums = probs.sum(axis=1)
        probs /= row_sums[:, None]

        counts = np.zeros((n_sims, k), dtype=np.int64)
        remaining = np.full(n_sims, n_events, dtype=np.int64)
        for j in range(k - 1):
            denom = probs[:, j:].sum(axis=1)
            ratio = np.clip(np.where(denom > 0.0, probs[:, j] / denom, 0.0), 0.0, 1.0)
            drawn = rng.binomial(remaining, ratio)
            counts[:, j] = drawn
            remaining = remaining - drawn
        counts[:, k - 1] = remaining
        freq = counts.astype(np.float64) / float(n_events)

        lo = np.quantile(freq, lo_q, axis=0)
        hi = np.quantile(freq, hi_q, axis=0)
        n_inside += int(np.sum((emp >= lo) & (emp <= hi)))

    coverage = n_inside / n_points
    low, high = coverage_band
    in_band = low <= coverage <= high
    finding = _coverage_finding(
        model_name, coverage, n_points, coverage_band,
        kind="predictive", code_prefix="predictive_coverage",
    )
    _log.info(
        "predictive_coverage model=%s coverage=%.4f n_points=%d in_band=%s",
        model_name, coverage, n_points, in_band,
    )
    return HdiCoverageResult(
        model_name=model_name,
        n_cells=n_points,
        coverage=coverage,
        coverage_band=coverage_band,
        in_band=in_band,
        coverage_kind="predictive",
        finding=finding,
    )


def compute_predictive_mean_coverage(
    estimate: pl.DataFrame,
    realization: pl.DataFrame,
    *,
    model_name: str,
    join_keys: tuple[str, ...],
    realization_col: str,
    cell_size_col: str,
    sample_sd_col: str,
    mean_col: str,
    sd_col: str,
    n_sims: int = _PREDICTIVE_N_SIMS,
    seed: int = _PREDICTIVE_SEED,
    hdi_prob: float = HDI_PROB,
    coverage_band: tuple[float, float] = DEFAULT_COVERAGE_BAND,
) -> HdiCoverageResult:
    """Posterior-predictive coverage of a per-cell held-out mean.

    Simulates the predictive distribution of the cell's held-out sample
    mean as the posterior mean (``mean_col`` / ``sd_col``) plus the
    central-limit sampling noise of a mean of ``cell_size_col`` draws whose
    per-event spread is the held-out ``sample_sd_col``. Reports the
    fraction of cells whose held-out ``realization_col`` mean lands inside
    the ``hdi_prob`` predictive interval.
    """
    missing_est = [c for c in (*join_keys, mean_col, sd_col) if c not in estimate.columns]
    if missing_est:
        raise ValueError(f"estimate frame missing columns {missing_est}")
    missing_real = [
        c for c in (*join_keys, realization_col, cell_size_col, sample_sd_col)
        if c not in realization.columns
    ]
    if missing_real:
        raise ValueError(f"realization frame missing columns {missing_real}")

    joined = estimate.join(realization, on=list(join_keys), how="inner").sort(
        list(join_keys)
    )
    n_cells = int(joined.height)
    if n_cells == 0:
        return _no_cells_result(
            model_name, coverage_band, kind="predictive", code_prefix="predictive_coverage"
        )

    means = joined.get_column(mean_col).to_numpy().astype(np.float64)
    sds = np.maximum(joined.get_column(sd_col).to_numpy().astype(np.float64), 0.0)
    emp = joined.get_column(realization_col).to_numpy().astype(np.float64)
    n_events = joined.get_column(cell_size_col).to_numpy().astype(np.float64)
    sample_sd = np.maximum(
        np.nan_to_num(joined.get_column(sample_sd_col).to_numpy().astype(np.float64)),
        0.0,
    )
    mean_sampling_sd = sample_sd / np.sqrt(n_events)

    rng = np.random.default_rng(seed)
    param = rng.normal(means[:, None], sds[:, None], size=(n_cells, n_sims))
    noise = rng.normal(0.0, mean_sampling_sd[:, None], size=(n_cells, n_sims))
    pred = param + noise
    lo_q = (1.0 - hdi_prob) / 2.0
    hi_q = 1.0 - lo_q
    lo = np.quantile(pred, lo_q, axis=1)
    hi = np.quantile(pred, hi_q, axis=1)
    n_inside = int(np.sum((emp >= lo) & (emp <= hi)))

    coverage = n_inside / n_cells
    low, high = coverage_band
    in_band = low <= coverage <= high
    finding = _coverage_finding(
        model_name, coverage, n_cells, coverage_band,
        kind="predictive", code_prefix="predictive_coverage",
    )
    _log.info(
        "predictive_mean_coverage model=%s coverage=%.4f n_cells=%d in_band=%s",
        model_name, coverage, n_cells, in_band,
    )
    return HdiCoverageResult(
        model_name=model_name,
        n_cells=n_cells,
        coverage=coverage,
        coverage_band=coverage_band,
        in_band=in_band,
        coverage_kind="predictive",
        finding=finding,
    )


def run_expectancy_held_out_realization(
    dataset_parquet: Path,
    *,
    min_cell_events: int = _MIN_CELL_EVENTS,
) -> pl.DataFrame:
    """Recompute held-out per-cell mean runs-to-end for the run-expectancy model.

    Applies the fit's population filter and the same deterministic 10% game
    holdout the fit uses, then materializes, per ``(state, season, league)``
    cell, the held-out mean and standard deviation of
    ``runs_to_end_of_inning`` plus the event count. ``state`` is the
    ``(outs, base)`` suffix of ``run_expectancy_start_key`` matching the
    summary export's ``state``. Cells with fewer than ``min_cell_events``
    held-out events are dropped.
    """
    scanned = (
        filter_event_population(
            pl.scan_parquet(dataset_parquet), label="run_expectancy realization"
        )
        .filter(
            pl.col("game_id").is_not_null()
            & pl.col("run_expectancy_start_key").is_not_null()
            & pl.col("runs_to_end_of_inning").is_not_null()
        )
        .select(
            pl.col("game_id"),
            pl.col("season").cast(pl.Int64).alias("season"),
            pl.col("league").cast(pl.Utf8).alias("league"),
            _suffix_expr("run_expectancy_start_key").alias("state"),
            pl.col("runs_to_end_of_inning").cast(pl.Float64).alias("runs_to_end"),
        )
        .collect()
    )
    if scanned.height == 0:
        raise ValueError(f"no run-expectancy rows in {dataset_parquet}")

    game_ids = scanned.get_column("game_id").unique().to_list()
    held_games = [
        g
        for g in game_ids
        if game_hash_fold(str(g), fold_count=_HOLDOUT_FOLD_COUNT) == _HOLDOUT_FOLD_ID
    ]
    held = scanned.filter(pl.col("game_id").is_in(held_games))
    realization = (
        held.group_by(["state", "season", "league"])
        .agg(
            pl.col("runs_to_end").mean().alias("held_out_mean"),
            pl.col("runs_to_end").std().alias("held_out_sd"),
            pl.len().alias("cell_total"),
        )
        .filter(pl.col("cell_total") >= min_cell_events)
        .with_columns(pl.col("held_out_sd").fill_null(0.0))
    )
    _log.info(
        "run_expectancy realization cells=%d held_out_games=%d",
        realization.height,
        len(held_games),
    )
    return realization


def run_expectancy_coverage_pair(
    summary_parquet: Path,
    dataset_parquet: Path,
    *,
    min_cell_events: int = _MIN_CELL_EVENTS,
    coverage_band: tuple[float, float] = DEFAULT_COVERAGE_BAND,
) -> tuple[HdiCoverageResult, HdiCoverageResult]:
    """(predictive, parameter) coverage for ``run_expectancy_summary``.

    Builds the held-out realization once and grades the per-cell held-out
    mean of runs-to-end against posterior-predictive intervals of the mean
    and against the published parameter HDIs.
    """
    estimate = pl.read_parquet(summary_parquet).with_columns(
        pl.col("season").cast(pl.Int64)
    )
    realization = run_expectancy_held_out_realization(
        dataset_parquet, min_cell_events=min_cell_events
    )
    predictive = compute_predictive_mean_coverage(
        estimate,
        realization,
        model_name="run_expectancy",
        join_keys=("state", "season", "league"),
        realization_col="held_out_mean",
        cell_size_col="cell_total",
        sample_sd_col="held_out_sd",
        mean_col="re_value_mean",
        sd_col="re_value_sd",
        coverage_band=coverage_band,
    )
    parameter = compute_hdi_coverage(
        estimate,
        realization,
        model_name="run_expectancy",
        join_keys=("state", "season", "league"),
        realization_col="held_out_mean",
        hdi_lower_col="re_value_hdi_lower",
        hdi_upper_col="re_value_hdi_upper",
        coverage_band=coverage_band,
    )
    return predictive, parameter
