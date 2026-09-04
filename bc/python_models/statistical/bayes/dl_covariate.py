"""Logit covariates derived from in-dataset DL class probabilities."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAny=false

from __future__ import annotations

import numpy as np
import numpy.typing as npt
import polars as pl

FloatArray = npt.NDArray[np.float64]
BoolArray = npt.NDArray[np.bool_]


def compute_dl_logits(
    df: pl.DataFrame,
    *,
    dl_p_class_column: str = "dl_p_class",
    collapse_positive_classes: tuple[str, ...] = (),
    class_labels: list[str] | None = None,
    clip: float = 1e-7,
) -> FloatArray:
    probs = df.get_column(dl_p_class_column).to_list()

    if collapse_positive_classes:
        if class_labels is None:
            raise ValueError(
                "class_labels required when collapse_positive_classes is non-empty"
            )
        missing = [c for c in collapse_positive_classes if c not in class_labels]
        if missing:
            raise ValueError(
                f"collapse classes {missing!r} not in class_labels {class_labels!r}"
            )
        positive_indices = tuple(
            class_labels.index(c) for c in collapse_positive_classes
        )
    else:
        positive_indices = ()

    out = np.zeros(len(probs), dtype=np.float64)
    for row_idx, row in enumerate(probs):
        if row is None or len(row) == 0:
            continue
        if positive_indices:
            p = float(sum(float(row[i]) for i in positive_indices))
        else:
            p = float(max(row))
        p_clipped = min(max(p, clip), 1.0 - clip)
        out[row_idx] = float(np.log(p_clipped / (1.0 - p_clipped)))
    return out


def compute_dl_log_probs_per_class_with_mask(
    df: pl.DataFrame,
    *,
    dl_p_class_column: str = "dl_p_class",
    n_classes: int,
    clip: float = 1e-7,
) -> tuple[FloatArray, BoolArray]:
    """Per-class clipped log-probabilities plus a per-row presence mask.

    A NULL or empty ``dl_p_class`` row yields an all-zero log-probability
    row and ``False`` in the mask, so callers can keep absent rows exactly
    zero through any downstream centering.
    """
    probs = df.get_column(dl_p_class_column).to_list()

    out = np.zeros((len(probs), n_classes), dtype=np.float64)
    present = np.zeros(len(probs), dtype=np.bool_)
    for row_idx, row in enumerate(probs):
        if row is None or len(row) == 0:
            continue
        if len(row) != n_classes:
            raise ValueError(
                f"{dl_p_class_column} row {row_idx} has {len(row)} entries; "
                f"expected n_classes={n_classes}"
            )
        present[row_idx] = True
        for class_idx in range(n_classes):
            p = float(row[class_idx])
            p_clipped = min(max(p, clip), 1.0 - clip)
            out[row_idx, class_idx] = float(np.log(p_clipped))
    return out, present


def compute_dl_log_probs_per_class(
    df: pl.DataFrame,
    *,
    dl_p_class_column: str = "dl_p_class",
    n_classes: int,
    clip: float = 1e-7,
) -> FloatArray:
    log_probs, _ = compute_dl_log_probs_per_class_with_mask(
        df, dl_p_class_column=dl_p_class_column, n_classes=n_classes, clip=clip
    )
    return log_probs


def center_dl_log_probs(
    log_probs: FloatArray, present: BoolArray, class_means: FloatArray
) -> FloatArray:
    """Subtract ``class_means`` from present rows; absent rows stay exactly zero."""
    if log_probs.shape[0] != present.shape[0]:
        raise ValueError(
            f"log_probs has {log_probs.shape[0]} rows but present mask has "
            f"{present.shape[0]}"
        )
    if class_means.shape != (log_probs.shape[1],):
        raise ValueError(
            f"class_means shape {class_means.shape} does not match "
            f"n_classes={log_probs.shape[1]}"
        )
    centered = log_probs - class_means[None, :]
    return np.where(present[:, None], centered, 0.0).astype(np.float64)
