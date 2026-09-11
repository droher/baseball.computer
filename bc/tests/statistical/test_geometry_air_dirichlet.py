from __future__ import annotations

from collections.abc import Callable
from typing import cast

import numpy as np
import numpy.typing as npt
import pytest

from python_models.statistical.backtests.geometry_air_dirichlet import (
    QuadratureConvergenceError,
    posterior_grid,
    posterior_moments,
    sample_posterior,
    validate_quadrature_convergence,
)


FloatArray = npt.NDArray[np.float64]


def test_no_data_moments_match_uniform_simplex_and_dirichlet_mixture() -> None:
    concentration = 4.0
    grid = posterior_grid(np.zeros((2, 3), dtype=np.int64), concentration, 16)
    moments = posterior_moments(grid)
    expected_global_second = np.full((3, 3), 1.0 / 12.0)
    np.fill_diagonal(expected_global_second, 1.0 / 6.0)
    expected_regime_second = np.full((3, 3), concentration / (12 * (concentration + 1)))
    np.fill_diagonal(
        expected_regime_second,
        (concentration + 2) / (6 * (concentration + 1)),
    )

    assert grid.nodes.shape == (16**2, 3)
    assert grid.weights.shape == (16**2,)
    assert np.allclose(grid.nodes.sum(axis=1), 1.0)
    assert np.isclose(grid.weights.sum(), 1.0)
    assert grid.nodes.flags.writeable is False
    assert grid.weights.flags.writeable is False
    assert np.allclose(moments.global_mean, np.full(3, 1.0 / 3.0), atol=1e-14)
    assert np.allclose(moments.global_second, expected_global_second, atol=1e-14)
    assert np.allclose(moments.regime_mean, np.full((2, 3), 1.0 / 3.0), atol=1e-14)
    assert np.allclose(
        moments.regime_second,
        np.broadcast_to(expected_regime_second, (2, 3, 3)),
        atol=1e-14,
    )


def test_class_and_regime_permutations_preserve_moments() -> None:
    counts = np.asarray([[10, 2, 3], [1, 9, 4], [0, 2, 8]], dtype=np.int64)
    class_permutation = np.asarray([2, 0, 1], dtype=np.int64)
    regime_permutation = np.asarray([2, 0, 1], dtype=np.int64)
    original = posterior_moments(posterior_grid(counts, 7.0, 64))
    permuted = posterior_moments(
        posterior_grid(
            counts[regime_permutation][:, class_permutation],
            7.0,
            64,
        )
    )

    assert np.allclose(
        permuted.global_mean, original.global_mean[class_permutation], atol=1e-12
    )
    assert np.allclose(
        permuted.global_second,
        original.global_second[np.ix_(class_permutation, class_permutation)],
        atol=1e-12,
    )
    assert np.allclose(
        permuted.regime_mean,
        original.regime_mean[regime_permutation][:, class_permutation],
        atol=1e-12,
    )
    expected_second = original.regime_second[regime_permutation]
    expected_second = expected_second[:, class_permutation][:, :, class_permutation]
    assert np.allclose(permuted.regime_second, expected_second, atol=1e-12)


def test_quadrature_convergence_is_reported_and_fails_closed() -> None:
    counts = np.asarray([[21, 7, 3], [4, 19, 8], [0, 5, 13]], dtype=np.int64)
    report = validate_quadrature_convergence(counts, 12.0)

    assert report.lower_order == 64
    assert report.higher_order == 128
    assert report.maximum_error <= 1e-5
    with pytest.raises(QuadratureConvergenceError) as captured:
        _ = validate_quadrature_convergence(
            counts,
            12.0,
            lower_order=8,
            higher_order=16,
            tolerance=1e-18,
        )
    assert captured.value.report.maximum_error > captured.value.report.tolerance


def test_sampling_matches_first_and_second_moments_with_mcse() -> None:
    counts = np.asarray([[18, 7, 4], [3, 15, 9]], dtype=np.int64)
    moments = posterior_moments(posterior_grid(counts, 6.0, 64))
    draws = sample_posterior(counts, 6.0, 64, 40_000, 291)

    assert draws.shape == (40_000, 2, 3)
    assert np.isfinite(draws).all()
    assert (draws >= 0).all()
    assert np.allclose(draws.sum(axis=2), 1.0)
    empirical_mean = draws.mean(axis=0)
    empirical_second = np.einsum("dgc,dgh->gch", draws, draws) / draws.shape[0]
    mean_mcse = draws.std(axis=0, ddof=1) / np.sqrt(draws.shape[0])
    products = draws[:, :, :, np.newaxis] * draws[:, :, np.newaxis, :]
    second_mcse = products.std(axis=0, ddof=1) / np.sqrt(draws.shape[0])
    assert np.all(np.abs(empirical_mean - moments.regime_mean) <= 5 * mean_mcse + 5e-4)
    assert np.all(
        np.abs(empirical_second - moments.regime_second) <= 5 * second_mcse + 5e-4
    )


def test_sampling_is_seeded_and_shared_global_draw_induces_covariance() -> None:
    counts = np.zeros((2, 3), dtype=np.int64)
    first = sample_posterior(counts, 20.0, 64, 25_000, 881)
    repeated = sample_posterior(counts, 20.0, 64, 25_000, 881)
    different = sample_posterior(counts, 20.0, 64, 25_000, 882)
    covariance = np.cov(first[:, 0, 0], first[:, 1, 0], ddof=1)[0, 1]

    assert np.array_equal(first, repeated)
    assert not np.array_equal(first, different)
    np.testing.assert_allclose(covariance, 1.0 / 18.0, rtol=0.0, atol=0.004)


def test_regime_total_does_not_overflow_int64() -> None:
    maximum = np.iinfo(np.int64).max
    counts = np.asarray([[maximum, maximum, maximum]], dtype=np.int64)
    moments = posterior_moments(posterior_grid(counts, 3.0, 8))

    assert np.isfinite(moments.regime_mean).all()
    assert np.isfinite(moments.regime_second).all()
    assert np.isclose(moments.regime_mean.sum(), 1.0)
    assert np.isclose(moments.regime_second.sum(), 1.0)


@pytest.mark.parametrize(
    ("operation", "message"),
    [
        (lambda: posterior_grid(np.zeros((0, 3)), 1.0, 8), "shape"),
        (lambda: posterior_grid(np.zeros((2, 2)), 1.0, 8), "shape"),
        (lambda: posterior_grid([[1, -1, 2]], 1.0, 8), "nonnegative integers"),
        (lambda: posterior_grid([[1, 1.5, 2]], 1.0, 8), "nonnegative integers"),
        (lambda: posterior_grid([[1, float("nan"), 2]], 1.0, 8), "finite"),
        (lambda: posterior_grid([[True, False, True]], 1.0, 8), "nonnegative integers"),
        (
            lambda: posterior_grid(
                np.asarray([[2**63, 0, 0]], dtype=np.uint64), 1.0, 8
            ),
            "nonnegative integers",
        ),
        (lambda: posterior_grid([[1, 2, 3]], 0.0, 8), "concentration"),
        (lambda: posterior_grid([[1, 2, 3]], True, 8), "concentration"),
        (lambda: posterior_grid([[1, 2, 3]], float("inf"), 8), "concentration"),
        (lambda: posterior_grid([[1, 2, 3]], 1.0, 1), "order"),
        (lambda: posterior_grid([[1, 2, 3]], 1.0, cast(int, 8.0)), "order"),
        (lambda: posterior_grid([[1, 2, 3]], 1.0, True), "order"),
        (
            lambda: validate_quadrature_convergence([[1, 2, 3]], 1.0, tolerance=True),
            "tolerance",
        ),
        (
            lambda: validate_quadrature_convergence(
                [[1, 2, 3]], 1.0, lower_order=16, higher_order=16
            ),
            "higher quadrature order",
        ),
        (lambda: sample_posterior([[1, 2, 3]], 1.0, 8, 0, 1), "draws"),
        (lambda: sample_posterior([[1, 2, 3]], 1.0, 8, 1, -1), "seed"),
    ],
)
def test_invalid_inputs_are_rejected(
    operation: Callable[[], object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _ = operation()
