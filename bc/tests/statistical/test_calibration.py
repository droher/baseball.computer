"""Calibration metrics on synthetic inputs."""

from __future__ import annotations

import numpy as np
import pytest

from python_models.statistical.calibration import (
    apply_isotonic,
    apply_temperature,
    brier_score,
    expected_calibration_error,
    fit_isotonic,
    fit_temperature,
    log_loss,
    reliability_curve,
)


def test_brier_score_perfect_predictions_is_zero() -> None:
    score = brier_score([0.0, 1.0, 0.0, 1.0], [0, 1, 0, 1])
    assert score == 0.0


def test_brier_score_worst_predictions_is_one() -> None:
    score = brier_score([1.0, 0.0, 1.0, 0.0], [0, 1, 0, 1])
    assert score == 1.0


def test_log_loss_perfect_close_to_zero() -> None:
    score = log_loss([0.999_999, 0.000_001], [1, 0])
    assert score < 1e-3


def test_ece_perfect_calibration() -> None:
    rng = np.random.default_rng(seed=1)
    probs = rng.uniform(0.0, 1.0, size=20_000)
    labels = (rng.uniform(0.0, 1.0, size=20_000) < probs).astype(np.int64)
    ece = expected_calibration_error(probs, labels, n_bins=20)
    assert ece < 0.02


def test_ece_miscalibrated_is_nonzero() -> None:
    rng = np.random.default_rng(seed=2)
    probs = rng.uniform(0.0, 1.0, size=5_000)
    labels = np.zeros_like(probs, dtype=np.int64)
    ece = expected_calibration_error(probs, labels, n_bins=20)
    assert ece > 0.3


def test_reliability_curve_returns_per_bin_rows() -> None:
    rng = np.random.default_rng(seed=3)
    probs = rng.uniform(0.0, 1.0, size=2_000)
    labels = (rng.uniform(0.0, 1.0, size=2_000) < probs).astype(np.int64)
    curve = reliability_curve(probs, labels, n_bins=10)
    assert all(0.0 <= c[0] <= 1.0 for c in curve)
    assert all(0.0 <= c[1] <= 1.0 for c in curve)
    assert sum(c[2] for c in curve) == 2_000


def test_temperature_scaling_recovers_known_t() -> None:
    rng = np.random.default_rng(seed=4)
    n = 4_000
    k = 3
    true_t = 2.0
    base_logits = rng.normal(0.0, 1.0, size=(n, k))
    scaled = base_logits / true_t
    exp = np.exp(scaled - scaled.max(axis=1, keepdims=True))
    probs = exp / exp.sum(axis=1, keepdims=True)
    cumulative = np.cumsum(probs, axis=1)
    u = rng.uniform(0.0, 1.0, size=n)[:, None]
    labels = (cumulative > u).argmax(axis=1).astype(np.int64)
    learned_t = fit_temperature(base_logits, labels)
    assert 1.5 < learned_t < 2.5


def test_apply_temperature_sums_to_one() -> None:
    logits = np.array([[1.0, 2.0, 3.0], [-1.0, 0.0, 1.0]])
    probs = apply_temperature(logits, temperature=1.5)
    assert probs.shape == (2, 3)
    assert np.allclose(probs.sum(axis=1), 1.0)


def test_isotonic_monotone_output() -> None:
    rng = np.random.default_rng(seed=5)
    probs = rng.uniform(0.0, 1.0, size=1_000)
    labels = (rng.uniform(0.0, 1.0, size=1_000) < probs).astype(np.int64)
    knots = fit_isotonic(probs, labels)
    xs = np.linspace(0.0, 1.0, 50)
    ys = apply_isotonic(xs, knots)
    assert np.all(np.diff(ys) >= -1e-9)


def test_brier_shape_mismatch_raises() -> None:
    with pytest.raises(ValueError):
        _ = brier_score([0.1, 0.5], [1])
