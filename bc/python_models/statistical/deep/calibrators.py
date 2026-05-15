"""Calibrators wired around the shared ``statistical.calibration`` utils.

Three entry points:

- ``fit_multiclass_temperature(oof_logits, oof_labels) -> float`` — wraps
  ``calibration.fit_temperature`` so a deep multiclass head can lift its
  per-row logits to calibrated probabilities via a single scalar T.
- ``fit_isotonic_per_class(oof_probs, oof_labels) -> dict[int, IsotonicKnots]``
  — one-vs-rest isotonic regression keyed by class index. Falls back to
  ``calibration.fit_isotonic`` per class.
- ``fit_platt_per_slice(...)`` — net-new helper for sliced binary
  calibration; per-slice Platt scaling with a parent-slice fallback when
  a slice has fewer than ``min_rows_per_slice`` rows (doc-04 sketch).

Temperature scaling fit set: union of all ``fold_count`` OOF predictions
on TRAIN — each TRAIN row appears once with its OOF logit. Evaluation
set for the on-artifact calibration report is VALIDATE predictions from
the full-fit model. TEST predictions exist in
``probabilities.parquet`` (Bayes needs them) but never feed a
calibration metric in the published report.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

from python_models.statistical.calibration import (
    FloatArray,
    IntArray,
    fit_isotonic,
    fit_temperature,
)

PlattCoefficients = tuple[float, float]


@dataclass(frozen=True)
class IsotonicKnots:
    sorted_probs: FloatArray
    calibrated_means: FloatArray


def fit_multiclass_temperature(
    oof_logits: FloatArray,
    oof_labels: IntArray,
    *,
    t_bounds: tuple[float, float] = (1e-3, 1e3),
) -> float:
    if oof_logits.ndim != 2:
        raise ValueError(
            f"oof_logits must be 2-d (N x K); got shape {oof_logits.shape}"
        )
    return fit_temperature(oof_logits, oof_labels, t_bounds=t_bounds)


def fit_isotonic_per_class(
    oof_probs: FloatArray,
    oof_labels: IntArray,
) -> dict[int, IsotonicKnots]:
    if oof_probs.ndim != 2:
        raise ValueError(
            f"oof_probs must be 2-d (N x K); got shape {oof_probs.shape}"
        )
    n, k = oof_probs.shape
    if oof_labels.shape != (n,):
        raise ValueError(
            f"oof_labels shape {oof_labels.shape} != ({n},)"
        )
    if k < 2:
        raise ValueError(f"need >= 2 classes; got {k}")

    out: dict[int, IsotonicKnots] = {}
    labels_f = oof_labels.astype(np.float64)
    for c in range(k):
        binary_y = (labels_f == float(c)).astype(np.float64)
        sorted_probs, calibrated_means = fit_isotonic(
            oof_probs[:, c].astype(np.float64), binary_y
        )
        out[c] = IsotonicKnots(
            sorted_probs=sorted_probs, calibrated_means=calibrated_means
        )
    return out


def fit_platt_per_slice(
    oof_probs: FloatArray,
    oof_labels: IntArray,
    slice_keys: npt.NDArray[np.object_],
    *,
    parent_slice_fn: Callable[[str], str] | None = None,
    min_rows_per_slice: int = 30,
) -> dict[str, PlattCoefficients]:
    """Per-slice Platt scaling on binary probabilities.

    Parameters
    ----------
    oof_probs : (N,) float in [0, 1]
        Raw probabilities (e.g. argmax-class softmax score for OvR).
    oof_labels : (N,) int in {0, 1}
        Binary outcomes aligned with ``oof_probs``.
    slice_keys : (N,) object
        Slice membership key per row (e.g. concatenated era×source_family).
    parent_slice_fn : optional callable
        Maps a sparse slice key to its parent slice (e.g. drop the most
        granular axis). When a slice has fewer than ``min_rows_per_slice``
        rows, the parent slice's coefficients are used instead. If the
        parent is also too sparse, the recursion bubbles up; a slice that
        runs out of parents falls through to the global fit.
    """
    if oof_probs.ndim != 1:
        raise ValueError(
            f"oof_probs must be 1-d for Platt scaling; got shape {oof_probs.shape}"
        )
    n = oof_probs.shape[0]
    if oof_labels.shape != (n,):
        raise ValueError(
            f"oof_labels shape {oof_labels.shape} != ({n},)"
        )
    if slice_keys.shape != (n,):
        raise ValueError(
            f"slice_keys shape {slice_keys.shape} != ({n},)"
        )

    eps = 1e-6
    probs = np.clip(oof_probs.astype(np.float64), eps, 1.0 - eps)
    logits = np.log(probs / (1.0 - probs))
    labels = oof_labels.astype(np.float64)

    global_coeffs = _fit_platt_scalar(logits, labels)

    unique_slices = sorted({str(k) for k in slice_keys.tolist()})
    raw: dict[str, PlattCoefficients] = {}
    for slc in unique_slices:
        mask = np.asarray([str(k) == slc for k in slice_keys.tolist()], dtype=bool)
        n_slice = int(mask.sum())
        if n_slice >= min_rows_per_slice:
            raw[slc] = _fit_platt_scalar(logits[mask], labels[mask])

    resolved: dict[str, PlattCoefficients] = {}
    for slc in unique_slices:
        if slc in raw:
            resolved[slc] = raw[slc]
            continue
        if parent_slice_fn is None:
            resolved[slc] = global_coeffs
            continue
        cursor = slc
        seen: set[str] = {slc}
        coeffs = global_coeffs
        while True:
            parent = parent_slice_fn(cursor)
            if parent in seen:
                break
            seen.add(parent)
            if parent in raw:
                coeffs = raw[parent]
                break
            cursor = parent
        resolved[slc] = coeffs
    return resolved


def _fit_platt_scalar(
    logits: FloatArray, labels: FloatArray
) -> PlattCoefficients:
    from scipy.optimize import minimize

    def nll(theta: npt.NDArray[np.float64]) -> float:
        a, b = float(theta[0]), float(theta[1])
        z = a * logits + b
        z = np.clip(z, -50.0, 50.0)
        log_sig = -np.logaddexp(0.0, -z)
        log_one_minus_sig = -np.logaddexp(0.0, z)
        return float(-(labels * log_sig + (1.0 - labels) * log_one_minus_sig).mean())

    result = minimize(nll, x0=np.array([1.0, 0.0]), method="L-BFGS-B")
    a, b = float(result.x[0]), float(result.x[1])
    return a, b
