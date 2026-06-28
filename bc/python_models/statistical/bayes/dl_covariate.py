"""Logit covariates derived from in-dataset DL class probabilities."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAny=false

from __future__ import annotations

import numpy as np
import numpy.typing as npt
import polars as pl

FloatArray = npt.NDArray[np.float64]


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


def compute_dl_log_probs_per_class(
    df: pl.DataFrame,
    *,
    dl_p_class_column: str = "dl_p_class",
    n_classes: int,
    clip: float = 1e-7,
) -> FloatArray:
    probs = df.get_column(dl_p_class_column).to_list()

    out = np.zeros((len(probs), n_classes), dtype=np.float64)
    for row_idx, row in enumerate(probs):
        if row is None or len(row) == 0:
            continue
        if len(row) != n_classes:
            raise ValueError(
                f"{dl_p_class_column} row {row_idx} has {len(row)} entries; "
                f"expected n_classes={n_classes}"
            )
        for class_idx in range(n_classes):
            p = float(row[class_idx])
            p_clipped = min(max(p, clip), 1.0 - clip)
            out[row_idx, class_idx] = float(np.log(p_clipped))
    return out
