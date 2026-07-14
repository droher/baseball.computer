"""ECE, Brier, log loss, reliability curves, temperature scaling, isotonic."""

from __future__ import annotations

import math
from collections.abc import Sequence

import numpy as np
import numpy.typing as npt
from scipy.optimize import minimize_scalar

FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]


def brier_score(probs: Sequence[float] | FloatArray, labels: Sequence[int] | IntArray) -> float:
    """Brier score for binary probabilities (mean squared error)."""
    p = np.asarray(probs, dtype=np.float64)
    y = np.asarray(labels, dtype=np.float64)
    if p.shape != y.shape:
        raise ValueError(f"shape mismatch: probs {p.shape} vs labels {y.shape}")
    return float(np.mean((p - y) ** 2))


def log_loss(probs: Sequence[float] | FloatArray, labels: Sequence[int] | IntArray, *, eps: float = 1e-15) -> float:
    p = np.asarray(probs, dtype=np.float64)
    y = np.asarray(labels, dtype=np.float64)
    if p.shape != y.shape:
        raise ValueError(f"shape mismatch: probs {p.shape} vs labels {y.shape}")
    p = np.clip(p, eps, 1.0 - eps)
    return float(-np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))


def expected_calibration_error(
    probs: Sequence[float] | FloatArray,
    labels: Sequence[int] | IntArray,
    *,
    n_bins: int = 15,
) -> float:
    """Equal-width binning ECE for binary probabilities."""
    p = np.asarray(probs, dtype=np.float64)
    y = np.asarray(labels, dtype=np.float64)
    if p.shape != y.shape:
        raise ValueError(f"shape mismatch: probs {p.shape} vs labels {y.shape}")
    if p.size == 0:
        return 0.0
    bin_edges = np.linspace(0.0, 1.0, n_bins + 1)
    bin_ids = np.clip(np.digitize(p, bin_edges[1:-1], right=False), 0, n_bins - 1)
    total = float(p.size)
    ece = 0.0
    for b in range(n_bins):
        mask = bin_ids == b
        if not np.any(mask):
            continue
        confidence = float(np.mean(p[mask]))
        accuracy = float(np.mean(y[mask]))
        weight = float(np.sum(mask)) / total
        ece += weight * abs(confidence - accuracy)
    return ece


def _confidence_and_correctness(
    probs: FloatArray, labels: IntArray
) -> tuple[FloatArray, FloatArray]:
    if probs.ndim != 2:
        raise ValueError(f"probs must be 2-D (n, k); got shape {probs.shape}")
    if labels.shape != (probs.shape[0],):
        raise ValueError(
            f"labels shape {labels.shape} != ({probs.shape[0]},)"
        )
    confidence = probs.max(axis=1)
    correct = (probs.argmax(axis=1) == labels).astype(np.float64)
    return confidence, correct


def multiclass_expected_calibration_error(
    probs: Sequence[Sequence[float]] | FloatArray,
    labels: Sequence[int] | IntArray,
    *,
    n_bins: int = 15,
) -> float:
    """Top-label (max-probability) ECE for multiclass probability vectors.

    Bins events by the confidence of the argmax class and accumulates the
    gap between mean confidence and empirical top-1 accuracy per bin.
    """
    p = np.asarray(probs, dtype=np.float64)
    y = np.asarray(labels, dtype=np.int64)
    if p.size == 0:
        return 0.0
    confidence, correct = _confidence_and_correctness(p, y)
    bin_edges = np.linspace(0.0, 1.0, n_bins + 1)
    bin_ids = np.clip(np.digitize(confidence, bin_edges[1:-1], right=False), 0, n_bins - 1)
    total = float(confidence.size)
    ece = 0.0
    for b in range(n_bins):
        mask = bin_ids == b
        if not np.any(mask):
            continue
        conf = float(np.mean(confidence[mask]))
        acc = float(np.mean(correct[mask]))
        weight = float(np.sum(mask)) / total
        ece += weight * abs(conf - acc)
    return ece


def multiclass_reliability_curve(
    probs: Sequence[Sequence[float]] | FloatArray,
    labels: Sequence[int] | IntArray,
    *,
    n_bins: int = 15,
) -> list[tuple[float, float, int]]:
    """Return ``(mean_confidence, empirical_accuracy, count)`` per non-empty bin.

    The multiclass analogue of :func:`reliability_curve`, binned on the
    argmax-class confidence.
    """
    p = np.asarray(probs, dtype=np.float64)
    y = np.asarray(labels, dtype=np.int64)
    if p.size == 0:
        return []
    confidence, correct = _confidence_and_correctness(p, y)
    bin_edges = np.linspace(0.0, 1.0, n_bins + 1)
    bin_ids = np.clip(np.digitize(confidence, bin_edges[1:-1], right=False), 0, n_bins - 1)
    out: list[tuple[float, float, int]] = []
    for b in range(n_bins):
        mask = bin_ids == b
        n = int(np.sum(mask))
        if n == 0:
            continue
        out.append((float(np.mean(confidence[mask])), float(np.mean(correct[mask])), n))
    return out


def reliability_curve(
    probs: Sequence[float] | FloatArray,
    labels: Sequence[int] | IntArray,
    *,
    n_bins: int = 15,
) -> list[tuple[float, float, int]]:
    """Return ``(mean_predicted, empirical_rate, count)`` per bin."""
    p = np.asarray(probs, dtype=np.float64)
    y = np.asarray(labels, dtype=np.float64)
    if p.shape != y.shape:
        raise ValueError(f"shape mismatch: probs {p.shape} vs labels {y.shape}")
    bin_edges = np.linspace(0.0, 1.0, n_bins + 1)
    bin_ids = np.clip(np.digitize(p, bin_edges[1:-1], right=False), 0, n_bins - 1)
    out: list[tuple[float, float, int]] = []
    for b in range(n_bins):
        mask = bin_ids == b
        n = int(np.sum(mask))
        if n == 0:
            continue
        out.append((float(np.mean(p[mask])), float(np.mean(y[mask])), n))
    return out


def fit_temperature(
    logits: FloatArray,
    labels: IntArray,
    *,
    t_bounds: tuple[float, float] = (1e-3, 1e3),
) -> float:
    """Fit a single scalar temperature for multiclass softmax via bounded NLL minimization."""
    n, k = logits.shape
    if labels.shape != (n,):
        raise ValueError(f"labels shape {labels.shape} != ({n},)")
    if k < 2:
        raise ValueError(f"need at least 2 classes; got {k}")
    rows = np.arange(n)

    def nll(t: float) -> float:
        z = logits / t
        z = z - z.max(axis=1, keepdims=True)
        log_norm = np.log(np.exp(z).sum(axis=1))
        return float(np.mean(log_norm - z[rows, labels]))

    result = minimize_scalar(nll, bounds=t_bounds, method="bounded", options={"xatol": 1e-6})
    t_final = float(getattr(result, "x"))
    if not math.isfinite(t_final) or t_final <= 0:
        raise RuntimeError(f"temperature scaling diverged; t={t_final}")
    return t_final


def apply_temperature(logits: FloatArray, temperature: float) -> FloatArray:
    scaled = logits / temperature
    scaled = scaled - scaled.max(axis=1, keepdims=True)
    exp = np.exp(scaled)
    return exp / exp.sum(axis=1, keepdims=True)


def fit_isotonic(probs: FloatArray, labels: FloatArray) -> tuple[FloatArray, FloatArray]:
    """Pool-adjacent-violators isotonic regression of ``labels ~ probs``.

    Returns ``(sorted_probs, calibrated_means)`` suitable for stepwise
    interpolation when applying the calibrator at scoring time.
    """
    if probs.shape != labels.shape:
        raise ValueError(f"shape mismatch: probs {probs.shape} vs labels {labels.shape}")
    order = np.argsort(probs)
    x = probs[order].astype(np.float64)
    y = labels[order].astype(np.float64)
    weights = np.ones_like(y)
    means = y.copy()
    n = means.size
    i = 0
    while i < n - 1:
        if means[i] <= means[i + 1]:
            i += 1
            continue
        w = weights[i] + weights[i + 1]
        m = (weights[i] * means[i] + weights[i + 1] * means[i + 1]) / w
        means[i] = m
        means = np.delete(means, i + 1)
        weights[i] = w
        weights = np.delete(weights, i + 1)
        x = np.delete(x, i + 1)
        n -= 1
        if i > 0:
            i -= 1
    return x, means


def apply_isotonic(probs: FloatArray, knots: tuple[FloatArray, FloatArray]) -> FloatArray:
    x, y = knots
    return np.interp(probs, x, y)
