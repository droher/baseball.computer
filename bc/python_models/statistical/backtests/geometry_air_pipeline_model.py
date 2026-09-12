from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from dataclasses import dataclass
from typing import Literal, Self, cast

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict, Field, model_validator
from scipy.special import gammaln

from python_models.statistical.backtests.geometry_air_dirichlet import (
    PosteriorGrid,
    QuadratureConvergence,
    QuadratureConvergenceError,
    posterior_grid,
    posterior_moments,
)
from python_models.statistical.backtests.geometry_air_pipeline_data import CLASSES

FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]
Arm = Literal["recorded_result", "recorded"]
ARMS: tuple[Arm, ...] = ("recorded_result", "recorded")
KAPPA_GRID: tuple[float, ...] = (
    1.0,
    2.0,
    3.0,
    5.0,
    10.0,
    20.0,
    30.0,
    50.0,
    100.0,
    200.0,
    300.0,
    500.0,
    1000.0,
    2000.0,
    3000.0,
    5000.0,
    10000.0,
)
ORDER_PAIRS = ((64, 128), (128, 256), (256, 512), (512, 1024))
TOLERANCE = 1e-5
GRID_CACHE_SIZE = 64
BOUNDARY_MASS_LIMIT = 0.10
EXPERIMENT_ID = "geometry-air-pipeline-translation-v1"


class PipelineCell(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    values: tuple[str, ...]
    counts: tuple[tuple[int, int, int], ...] = Field(min_length=1)
    orders: tuple[int, ...] = Field(min_length=1)
    log_marginal: tuple[float, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_cell(self) -> Self:
        flat = [value for season in self.counts for value in season]
        if min(flat) < 0 or sum(flat) == 0:
            raise ValueError("fitted cells must contain nonnegative observed counts")
        if len(self.orders) != len(KAPPA_GRID) or len(self.log_marginal) != len(
            KAPPA_GRID
        ):
            raise ValueError("cells must carry one order and marginal per grid value")
        if any(not np.isfinite(value) for value in self.log_marginal):
            raise ValueError("log marginal likelihoods must be finite")
        return self


class PipelineFit(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    model_type: Literal["air_pipeline_translation_v1"] = "air_pipeline_translation_v1"
    experiment_id: str = Field(min_length=1)
    fit_id: str = Field(min_length=1)
    pipeline: str = Field(min_length=1)
    arm: Arm
    seasons: tuple[int, ...] = Field(min_length=1)
    kappa_grid: tuple[float, ...] = KAPPA_GRID
    kappa_posterior: tuple[float, ...]
    cells: tuple[PipelineCell, ...] = Field(min_length=1)
    training_events: int = Field(gt=0)
    draws: int = Field(gt=0)
    seed: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_fit(self) -> Self:
        if self.kappa_grid != KAPPA_GRID:
            raise ValueError("unexpected concentration grid")
        if len(self.kappa_posterior) != len(KAPPA_GRID) or not np.isclose(
            sum(self.kappa_posterior), 1.0, atol=1e-9
        ):
            raise ValueError(
                "concentration posterior must be a distribution on the grid"
            )
        if len(self.seasons) != len(set(self.seasons)) or list(self.seasons) != sorted(
            self.seasons
        ):
            raise ValueError("seasons must be unique and sorted")
        keys = [cell.values for cell in self.cells]
        if len(keys) != len(set(keys)) or keys != sorted(keys):
            raise ValueError("cells must have unique sorted identities")
        width = 2 if self.arm == "recorded_result" else 1
        if any(len(key) != width or key[0] not in CLASSES for key in keys):
            raise ValueError("cell identity does not match the arm")
        if any(len(cell.counts) != len(self.seasons) for cell in self.cells):
            raise ValueError("cell counts must cover every season")
        total = sum(sum(sum(row) for row in cell.counts) for cell in self.cells)
        if total != self.training_events:
            raise ValueError("cell counts do not conserve training events")
        return self

    @property
    def lower_boundary_mass(self) -> float:
        return float(self.kappa_posterior[0])

    @property
    def upper_boundary_mass(self) -> float:
        return float(self.kappa_posterior[-1])

    @property
    def kappa_posterior_mean(self) -> float:
        return float(np.dot(self.kappa_posterior, self.kappa_grid))


@dataclass(frozen=True, slots=True)
class CellPrediction:
    values: tuple[str, ...]
    status: Literal["posterior_cell", "prior_only_cell"]
    draws: FloatArray


@dataclass(frozen=True, slots=True)
class PipelinePrediction:
    probabilities: FloatArray
    cell_index: IntArray
    cells: tuple[CellPrediction, ...]


def cell_values(arm: Arm, recorded: object, result: object) -> tuple[str, ...]:
    if arm == "recorded":
        return (str(recorded),)
    return (str(recorded), str(result))


_GRIDS: OrderedDict[tuple[bytes, tuple[int, ...], float, int], PosteriorGrid] = (
    OrderedDict()
)


def cached_grid(counts: IntArray, kappa: float, order: int) -> PosteriorGrid:
    key = (counts.tobytes(), counts.shape, float(kappa), int(order))
    grid = _GRIDS.get(key)
    if grid is None:
        grid = posterior_grid(counts, kappa, order)
        _GRIDS[key] = grid
        while len(_GRIDS) > GRID_CACHE_SIZE:
            _ = _GRIDS.popitem(last=False)
    else:
        _GRIDS.move_to_end(key)
    return grid


def log_marginal_likelihood(counts: IntArray, kappa: float, order: int) -> float:
    grid = cached_grid(counts, kappa, order)
    totals = counts.sum(axis=1).astype(np.float64)
    normalizers = cast(FloatArray, gammaln(kappa) - gammaln(totals + kappa))
    return float(grid.log_normalizer + normalizers.sum())


def convergence_report(
    counts: IntArray, kappa: float, lower: int, higher: int
) -> QuadratureConvergence:
    coarse = posterior_moments(cached_grid(counts, kappa, lower))
    fine = posterior_moments(cached_grid(counts, kappa, higher))
    errors = (
        float(np.max(np.abs(coarse.global_mean - fine.global_mean))),
        float(np.max(np.abs(coarse.global_second - fine.global_second))),
        float(np.max(np.abs(coarse.regime_mean - fine.regime_mean))),
        float(np.max(np.abs(coarse.regime_second - fine.regime_second))),
    )
    return QuadratureConvergence(
        lower_order=lower,
        higher_order=higher,
        tolerance=TOLERANCE,
        global_mean_error=errors[0],
        global_second_error=errors[1],
        regime_mean_error=errors[2],
        regime_second_error=errors[3],
        maximum_error=max(errors),
    )


def converged_order(counts: IntArray, kappa: float) -> int:
    report = None
    for lower, higher in ORDER_PAIRS:
        report = convergence_report(counts, kappa, lower, higher)
        if report.maximum_error <= TOLERANCE:
            return higher
    if report is None:
        raise RuntimeError("quadrature order schedule is empty")
    raise QuadratureConvergenceError(report)


def season_counts(
    frame: pl.DataFrame, *, arm: Arm, seasons: tuple[int, ...]
) -> dict[tuple[str, ...], IntArray]:
    counts: dict[tuple[str, ...], IntArray] = {}
    grouped = frame.group_by(
        "season", "recorded_air_subtype", "result_family", "target_class"
    ).len()
    for season, recorded, result, target, n in grouped.iter_rows():
        values = cell_values(arm, recorded, result)
        if values not in counts:
            counts[values] = np.zeros((len(seasons), len(CLASSES)), dtype=np.int64)
        counts[values][seasons.index(int(season)), CLASSES.index(str(target))] += int(n)
    return counts


def fit_pipeline(
    frame: pl.DataFrame,
    *,
    pipeline: str,
    arm: Arm,
    seasons: tuple[int, ...],
    fit_id: str,
    experiment_id: str = EXPERIMENT_ID,
    draws: int = 4096,
    seed: int = 20260911,
) -> PipelineFit:
    required = {"season", "recorded_air_subtype", "result_family", "target_class"}
    if not required <= set(frame.columns) or frame.is_empty():
        raise ValueError("nonempty reference frame required")
    if any(frame[column].null_count() for column in required):
        raise ValueError("training columns must not contain nulls")
    if not set(frame["season"].to_list()) <= set(seasons):
        raise ValueError("training seasons must be declared")
    for column in ("recorded_air_subtype", "target_class"):
        if not set(frame[column].to_list()) <= set(CLASSES):
            raise ValueError(f"invalid airborne class in {column}")
    cells: list[PipelineCell] = []
    log_posterior = np.zeros(len(KAPPA_GRID))
    for values, counts in sorted(
        season_counts(frame, arm=arm, seasons=seasons).items()
    ):
        orders = [converged_order(counts, kappa) for kappa in KAPPA_GRID]
        marginals = [
            log_marginal_likelihood(counts, kappa, order)
            for kappa, order in zip(KAPPA_GRID, orders, strict=True)
        ]
        log_posterior += np.array(marginals)
        cells.append(
            PipelineCell(
                values=values,
                counts=tuple(
                    cast(tuple[int, int, int], tuple(int(v) for v in row))
                    for row in counts
                ),
                orders=tuple(orders),
                log_marginal=tuple(marginals),
            )
        )
    posterior = np.exp(log_posterior - log_posterior.max())
    posterior /= posterior.sum()
    return PipelineFit(
        experiment_id=experiment_id,
        fit_id=fit_id,
        pipeline=pipeline,
        arm=arm,
        seasons=seasons,
        kappa_posterior=tuple(float(value) for value in posterior),
        cells=tuple(cells),
        training_events=frame.height,
        draws=draws,
        seed=seed,
    )


def cell_seed(fit: PipelineFit, values: tuple[str, ...], season: int | None) -> int:
    payload = json.dumps(
        [
            fit.experiment_id,
            fit.fit_id,
            fit.pipeline,
            fit.arm,
            values,
            season,
            fit.seed,
        ],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return int.from_bytes(hashlib.sha256(payload.encode()).digest()[:8], "big")


def sample_cell(
    fit: PipelineFit,
    counts: IntArray,
    orders: tuple[int, ...],
    *,
    season: int | None,
    seed: int,
) -> FloatArray:
    rng = np.random.default_rng(seed)
    kappa_index: IntArray = rng.choice(
        len(KAPPA_GRID), size=fit.draws, p=np.array(fit.kappa_posterior)
    )
    output = np.empty((fit.draws, len(CLASSES)), dtype=np.float64)
    season_index = None if season is None else fit.seasons.index(season)
    for index in np.unique(kappa_index):
        positions = np.flatnonzero(kappa_index == index)
        kappa = KAPPA_GRID[int(index)]
        grid = cached_grid(counts, kappa, orders[int(index)])
        nodes: IntArray = rng.choice(
            grid.nodes.shape[0], size=positions.size, p=grid.weights
        )
        alpha = kappa * grid.nodes[nodes]
        if season_index is not None:
            alpha = alpha + counts[season_index][np.newaxis, :]
        gamma = rng.gamma(alpha)
        output[positions] = gamma / gamma.sum(axis=1, keepdims=True)
    if not np.isfinite(output).all() or (output < 0).any():
        raise RuntimeError("cell draws are not finite probabilities")
    return output


def predict_pipeline(
    fit: PipelineFit, frame: pl.DataFrame, *, season: int | None
) -> PipelinePrediction:
    if season is not None and season not in fit.seasons:
        raise ValueError("referenced season prediction requires a fitted season")
    fitted = {cell.values: cell for cell in fit.cells}
    keys = [
        cell_values(fit.arm, recorded, result)
        for recorded, result in frame.select(
            "recorded_air_subtype", "result_family"
        ).iter_rows()
    ]
    unique_keys = sorted(set(keys))
    cells: list[CellPrediction] = []
    for values in unique_keys:
        cell = fitted.get(values)
        if cell is None:
            counts = np.zeros((len(fit.seasons), len(CLASSES)), dtype=np.int64)
            orders = tuple(ORDER_PAIRS[0][1] for _ in KAPPA_GRID)
            status: Literal["posterior_cell", "prior_only_cell"] = "prior_only_cell"
        else:
            counts = np.array(cell.counts, dtype=np.int64)
            orders = cell.orders
            status = "posterior_cell"
        cells.append(
            CellPrediction(
                values=values,
                status=status,
                draws=sample_cell(
                    fit,
                    counts,
                    orders,
                    season=season,
                    seed=cell_seed(fit, values, season),
                ),
            )
        )
    position = {cell.values: index for index, cell in enumerate(cells)}
    cell_index = np.array([position[key] for key in keys], dtype=np.int64)
    means = np.stack([cell.draws.mean(axis=0) for cell in cells])
    return PipelinePrediction(
        probabilities=means[cell_index], cell_index=cell_index, cells=tuple(cells)
    )
