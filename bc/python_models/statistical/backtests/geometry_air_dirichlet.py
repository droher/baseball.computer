from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import numpy as np
import numpy.typing as npt
from scipy.special import gammaln, logsumexp, roots_legendre


FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]


@dataclass(frozen=True, slots=True)
class PosteriorGrid:
    nodes: FloatArray
    weights: FloatArray
    counts: IntArray
    concentration: float
    order: int
    log_normalizer: float


@dataclass(frozen=True, slots=True)
class PosteriorMoments:
    global_mean: FloatArray
    global_second: FloatArray
    regime_mean: FloatArray
    regime_second: FloatArray


@dataclass(frozen=True, slots=True)
class QuadratureConvergence:
    lower_order: int
    higher_order: int
    tolerance: float
    global_mean_error: float
    global_second_error: float
    regime_mean_error: float
    regime_second_error: float
    maximum_error: float


class QuadratureConvergenceError(RuntimeError):
    def __init__(self, report: QuadratureConvergence) -> None:
        self.report = report
        super().__init__(
            f"quadrature moment error {report.maximum_error:.8g} exceeds "
            f"tolerance {report.tolerance:.8g} for orders "
            f"{report.lower_order}/{report.higher_order}"
        )


def _readonly(values: FloatArray | IntArray) -> FloatArray | IntArray:
    output = values.copy()
    output.flags.writeable = False
    return output


def _validated_counts(counts: npt.ArrayLike) -> IntArray:
    values = np.asarray(counts)
    if values.ndim != 2 or values.shape[0] == 0 or values.shape[1] != 3:
        raise ValueError("counts must have shape [G, 3] with at least one regime")
    if np.issubdtype(values.dtype, np.bool_):
        raise ValueError("counts must be nonnegative integers")
    if np.issubdtype(values.dtype, np.integer):
        if (values < 0).any() or (values > np.iinfo(np.int64).max).any():
            raise ValueError("counts must be finite nonnegative integers")
        integers = values.astype(np.int64)
    elif np.issubdtype(values.dtype, np.floating):
        numeric = values.astype(np.float64)
        if (
            not np.isfinite(numeric).all()
            or (numeric < 0).any()
            or not np.equal(numeric, np.floor(numeric)).all()
            or (numeric >= float(2**63)).any()
        ):
            raise ValueError("counts must be finite nonnegative integers")
        integers = numeric.astype(np.int64)
    else:
        raise ValueError("counts must be finite nonnegative integers")
    return np.asarray(_readonly(integers), dtype=np.int64)


def _validated_concentration(concentration: float) -> float:
    if isinstance(concentration, bool):
        raise ValueError("concentration must be positive and finite")
    value = float(concentration)
    if not np.isfinite(value) or value <= 0:
        raise ValueError("concentration must be positive and finite")
    return value


def _validated_order(order: int) -> int:
    if isinstance(order, bool) or not isinstance(order, int) or order < 2:
        raise ValueError("quadrature order must be an integer of at least 2")
    return order


def posterior_grid(
    counts: npt.ArrayLike, concentration: float, order: int
) -> PosteriorGrid:
    count_values = _validated_counts(counts)
    concentration_value = _validated_concentration(concentration)
    order_value = _validated_order(order)
    roots, root_weights = cast(
        tuple[FloatArray, FloatArray], roots_legendre(order_value)
    )
    unit_nodes = (roots + 1.0) / 2.0
    unit_weights = root_weights / 2.0
    u = np.repeat(unit_nodes, order_value)
    v = np.tile(unit_nodes, order_value)
    nodes = np.column_stack((u, (1.0 - u) * v, (1.0 - u) * (1.0 - v)))
    base_weights = (
        np.repeat(unit_weights, order_value)
        * np.tile(unit_weights, order_value)
        * (1.0 - u)
    )
    log_weights = np.log(base_weights)
    scaled_nodes = concentration_value * nodes
    for regime_counts in count_values:
        log_weights += np.sum(
            cast(
                FloatArray,
                gammaln(regime_counts[np.newaxis, :] + scaled_nodes)
                - gammaln(scaled_nodes),
            ),
            axis=1,
        )
    log_normalizer = cast(float, logsumexp(log_weights))
    weights = np.exp(log_weights - log_normalizer)
    weights /= weights.sum()
    if (
        not np.isfinite(nodes).all()
        or not np.isfinite(weights).all()
        or (nodes <= 0).any()
        or (weights < 0).any()
        or not np.allclose(nodes.sum(axis=1), 1.0, rtol=0.0, atol=1e-14)
        or not np.isclose(weights.sum(), 1.0, rtol=0.0, atol=1e-14)
    ):
        raise RuntimeError("quadrature grid is not a finite normalized simplex")
    return PosteriorGrid(
        nodes=np.asarray(_readonly(nodes), dtype=np.float64),
        weights=np.asarray(_readonly(weights), dtype=np.float64),
        counts=count_values,
        concentration=concentration_value,
        order=order_value,
        log_normalizer=log_normalizer,
    )


def posterior_moments(grid: PosteriorGrid) -> PosteriorMoments:
    nodes = grid.nodes
    weights = grid.weights
    global_mean = np.einsum("q,qc->c", weights, nodes)
    global_second = np.einsum("q,qc,qd->cd", weights, nodes, nodes)
    regime_count = grid.counts.shape[0]
    regime_mean = np.empty((regime_count, 3), dtype=np.float64)
    regime_second = np.empty((regime_count, 3, 3), dtype=np.float64)
    for regime_index, counts in enumerate(grid.counts):
        alpha = counts[np.newaxis, :] + grid.concentration * nodes
        alpha_total = float(sum(int(value) for value in counts)) + grid.concentration
        regime_mean[regime_index] = np.einsum("q,qc->c", weights, alpha / alpha_total)
        products = alpha[:, :, np.newaxis] * alpha[:, np.newaxis, :]
        diagonal = np.arange(3)
        products[:, diagonal, diagonal] += alpha
        regime_second[regime_index] = np.einsum(
            "q,qcd->cd", weights, products / (alpha_total * (alpha_total + 1.0))
        )
    for values in (global_mean, global_second, regime_mean, regime_second):
        if not np.isfinite(values).all():
            raise RuntimeError("posterior moments are not finite")
    return PosteriorMoments(
        global_mean=np.asarray(_readonly(global_mean), dtype=np.float64),
        global_second=np.asarray(_readonly(global_second), dtype=np.float64),
        regime_mean=np.asarray(_readonly(regime_mean), dtype=np.float64),
        regime_second=np.asarray(_readonly(regime_second), dtype=np.float64),
    )


def validate_quadrature_convergence(
    counts: npt.ArrayLike,
    concentration: float,
    lower_order: int = 64,
    higher_order: int = 128,
    tolerance: float = 1e-5,
) -> QuadratureConvergence:
    if isinstance(tolerance, bool) or not np.isfinite(tolerance) or tolerance <= 0:
        raise ValueError("tolerance must be positive and finite")
    lower_order_value = _validated_order(lower_order)
    higher_order_value = _validated_order(higher_order)
    if higher_order_value <= lower_order_value:
        raise ValueError("higher quadrature order must exceed lower order")
    lower = posterior_moments(posterior_grid(counts, concentration, lower_order_value))
    higher = posterior_moments(
        posterior_grid(counts, concentration, higher_order_value)
    )
    errors = (
        float(np.max(np.abs(lower.global_mean - higher.global_mean))),
        float(np.max(np.abs(lower.global_second - higher.global_second))),
        float(np.max(np.abs(lower.regime_mean - higher.regime_mean))),
        float(np.max(np.abs(lower.regime_second - higher.regime_second))),
    )
    report = QuadratureConvergence(
        lower_order=lower_order_value,
        higher_order=higher_order_value,
        tolerance=float(tolerance),
        global_mean_error=errors[0],
        global_second_error=errors[1],
        regime_mean_error=errors[2],
        regime_second_error=errors[3],
        maximum_error=max(errors),
    )
    if report.maximum_error > report.tolerance:
        raise QuadratureConvergenceError(report)
    return report


def sample_posterior(
    counts: npt.ArrayLike,
    concentration: float,
    order: int,
    draws: int,
    seed: int,
) -> FloatArray:
    if isinstance(draws, bool) or not isinstance(draws, int) or draws <= 0:
        raise ValueError("draws must be a positive integer")
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("seed must be a nonnegative integer")
    grid = posterior_grid(counts, concentration, order)
    rng = np.random.default_rng(seed)
    node_indices = rng.choice(grid.nodes.shape[0], size=draws, p=grid.weights)
    shared_q = grid.nodes[node_indices]
    output = np.empty((draws, grid.counts.shape[0], 3), dtype=np.float64)
    for draw_index in range(draws):
        for regime_index, regime_counts in enumerate(grid.counts):
            alpha = regime_counts + grid.concentration * shared_q[draw_index]
            output[draw_index, regime_index] = rng.dirichlet(alpha)
    if (
        not np.isfinite(output).all()
        or (output < 0).any()
        or not np.allclose(output.sum(axis=2), 1.0, rtol=0.0, atol=1e-14)
    ):
        raise RuntimeError("posterior samples are not finite normalized simplexes")
    return output
