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
    multiclass_expected_calibration_error,
    multiclass_reliability_curve,
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
    labels = (rng.uniform(0.0, 1.0, size=1_000) < probs).astype(np.float64)
    knots = fit_isotonic(probs, labels)
    xs = np.linspace(0.0, 1.0, 50)
    ys = apply_isotonic(xs, knots)
    assert np.all(np.diff(ys) >= -1e-9)


def test_brier_shape_mismatch_raises() -> None:
    with pytest.raises(ValueError):
        _ = brier_score([0.1, 0.5], [1])


def test_multiclass_ece_confident_and_correct_is_zero() -> None:
    probs = np.array([[0.99, 0.01, 0.0], [0.0, 0.02, 0.98], [0.05, 0.9, 0.05]])
    labels = np.array([0, 2, 1], dtype=np.int64)
    ece = multiclass_expected_calibration_error(probs, labels, n_bins=15)
    assert ece < 0.1


def test_multiclass_ece_hand_computed_single_bin() -> None:
    probs = np.array([[0.7, 0.3], [0.7, 0.3]])
    labels = np.array([0, 1], dtype=np.int64)
    ece = multiclass_expected_calibration_error(probs, labels, n_bins=15)
    assert ece == pytest.approx(0.2, abs=1e-12)


def test_multiclass_ece_overconfident_wrong_is_large() -> None:
    probs = np.tile(np.array([[0.95, 0.05]]), (200, 1))
    labels = np.ones(200, dtype=np.int64)
    ece = multiclass_expected_calibration_error(probs, labels, n_bins=15)
    assert ece == pytest.approx(0.95, abs=1e-9)


def test_multiclass_ece_empty_is_zero() -> None:
    ece = multiclass_expected_calibration_error(
        np.zeros((0, 3)), np.zeros(0, dtype=np.int64)
    )
    assert ece == 0.0


def test_multiclass_ece_rejects_1d_probs() -> None:
    with pytest.raises(ValueError):
        _ = multiclass_expected_calibration_error(np.array([0.3, 0.7]), np.array([0, 1]))


def test_multiclass_reliability_curve_single_bin_row() -> None:
    probs = np.array([[0.7, 0.3], [0.7, 0.3]])
    labels = np.array([0, 1], dtype=np.int64)
    curve = multiclass_reliability_curve(probs, labels, n_bins=15)
    assert len(curve) == 1
    mean_conf, empirical_acc, count = curve[0]
    assert mean_conf == pytest.approx(0.7)
    assert empirical_acc == pytest.approx(0.5)
    assert count == 2
