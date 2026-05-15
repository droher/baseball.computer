"""Calibrator behavior on small synthetic logits/probs."""

from __future__ import annotations

import numpy as np
import pytest

from python_models.statistical.deep.calibrators import (
    IsotonicKnots,
    fit_isotonic_per_class,
    fit_multiclass_temperature,
    fit_platt_per_slice,
)


def _multiclass_synth(rng: np.random.Generator, n: int, k: int, t_true: float):
    base_logits = rng.normal(size=(n, k)) * 1.5
    probs = np.exp(base_logits / t_true)
    probs /= probs.sum(axis=1, keepdims=True)
    labels = np.array(
        [rng.choice(k, p=probs[i]) for i in range(n)], dtype=np.int64
    )
    return base_logits.astype(np.float64), labels


def test_temperature_returns_positive_scalar() -> None:
    rng = np.random.default_rng(1)
    logits, labels = _multiclass_synth(rng, n=300, k=4, t_true=1.5)
    t = fit_multiclass_temperature(logits, labels)
    assert isinstance(t, float)
    assert t > 0.0


def test_temperature_rejects_1d_logits() -> None:
    with pytest.raises(ValueError, match="must be 2-d"):
        _ = fit_multiclass_temperature(np.array([0.1, 0.2]), np.array([0, 1]))


def test_isotonic_per_class_returns_knots_per_class() -> None:
    rng = np.random.default_rng(2)
    n, k = 200, 3
    probs = rng.dirichlet(alpha=np.ones(k), size=n).astype(np.float64)
    labels = np.array(
        [int(np.argmax(probs[i])) for i in range(n)], dtype=np.int64
    )
    result = fit_isotonic_per_class(probs, labels)
    assert set(result.keys()) == {0, 1, 2}
    for knots in result.values():
        assert isinstance(knots, IsotonicKnots)
        assert knots.sorted_probs.ndim == 1
        assert knots.calibrated_means.ndim == 1
        assert knots.sorted_probs.size == knots.calibrated_means.size


def test_isotonic_rejects_1d_probs() -> None:
    with pytest.raises(ValueError, match="must be 2-d"):
        _ = fit_isotonic_per_class(np.array([0.1, 0.9]), np.array([0, 1]))


def test_platt_dense_slice_gets_own_fit() -> None:
    rng = np.random.default_rng(3)
    n = 200
    probs = rng.uniform(0.0, 1.0, size=n).astype(np.float64)
    labels = (rng.uniform(0.0, 1.0, size=n) < probs).astype(np.int64)
    slices = np.asarray(["era_a"] * 100 + ["era_b"] * 100, dtype=object)
    coeffs = fit_platt_per_slice(probs, labels, slices, min_rows_per_slice=30)
    assert set(coeffs.keys()) == {"era_a", "era_b"}
    a_a, _ = coeffs["era_a"]
    a_b, _ = coeffs["era_b"]
    assert isinstance(a_a, float)
    assert isinstance(a_b, float)


def test_platt_sparse_slice_falls_back_to_parent() -> None:
    rng = np.random.default_rng(4)
    n = 200
    probs = rng.uniform(0.0, 1.0, size=n).astype(np.float64)
    labels = (rng.uniform(0.0, 1.0, size=n) < probs).astype(np.int64)
    slices = np.asarray(
        ["era_a"] * 100
        + ["era_b_small"] * 5
        + ["era_b_dense"] * 95,
        dtype=object,
    )

    def parent_fn(slc: str) -> str:
        return "era_b" if slc.startswith("era_b") else slc

    coeffs = fit_platt_per_slice(
        probs,
        labels,
        slices,
        parent_slice_fn=parent_fn,
        min_rows_per_slice=30,
    )
    assert "era_b_small" in coeffs
    assert "era_b_dense" in coeffs


def test_platt_rejects_2d_probs() -> None:
    with pytest.raises(ValueError, match="must be 1-d"):
        _ = fit_platt_per_slice(
            np.array([[0.1, 0.9]]),
            np.array([0]),
            np.asarray(["x"], dtype=object),
        )
