"""Invariants for the DL-probability logit covariate helpers."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import math

import numpy as np
import polars as pl
import pytest

from python_models.statistical.bayes.dl_covariate import (
    compute_dl_logits,
    compute_dl_logits_per_class,
)


def _frame(rows: list[list[float] | None]) -> pl.DataFrame:
    return pl.DataFrame(
        {"dl_p_class": rows}, schema={"dl_p_class": pl.List(pl.Float64)}
    )


def test_null_and_empty_rows_yield_zero_logit() -> None:
    df = _frame([None, [], [0.6, 0.4]])
    out = compute_dl_logits(df)
    assert out.shape == (3,)
    assert out[0] == 0.0
    assert out[1] == 0.0
    assert out[2] != 0.0


def test_max_mode_uses_dominant_class() -> None:
    df = _frame([[0.7, 0.2, 0.1]])
    out = compute_dl_logits(df)
    p = 0.7
    expected = math.log(p / (1.0 - p))
    assert out[0] == pytest.approx(expected)
    assert out[0] > 0.0


def test_collapse_picks_listed_classes() -> None:
    labels = ["a", "b", "c"]
    df = _frame([[0.5, 0.3, 0.2]])
    out = compute_dl_logits(
        df, collapse_positive_classes=("a", "b"), class_labels=labels
    )
    p = 0.5 + 0.3
    expected = math.log(p / (1.0 - p))
    assert out[0] == pytest.approx(expected)
    assert out[0] > 0.0


def test_max_mode_equals_collapse_of_argmax_singleton() -> None:
    labels = ["a", "b", "c"]
    rows: list[list[float] | None] = [[0.1, 0.65, 0.25], [0.8, 0.15, 0.05]]
    df = _frame(rows)
    max_out = compute_dl_logits(df)
    argmax_labels = tuple(labels[int(np.argmax(np.asarray(r)))] for r in rows if r)
    collapse_out = np.array(
        [
            compute_dl_logits(
                _frame([r]),
                collapse_positive_classes=(lbl,),
                class_labels=labels,
            )[0]
            for r, lbl in zip(rows, argmax_labels)
        ]
    )
    assert np.allclose(max_out, collapse_out)


def test_collapse_requires_class_labels() -> None:
    df = _frame([[0.5, 0.5]])
    with pytest.raises(ValueError):
        _ = compute_dl_logits(df, collapse_positive_classes=("a",))


def test_collapse_rejects_unknown_label() -> None:
    df = _frame([[0.5, 0.5]])
    with pytest.raises(ValueError):
        _ = compute_dl_logits(
            df, collapse_positive_classes=("z",), class_labels=["a", "b"]
        )


def test_clipping_prevents_infinities_at_extremes() -> None:
    clip = 1e-7
    df = _frame([[1.0, 0.0], [0.0, 1.0]])
    out_max = compute_dl_logits(df, clip=clip)
    assert np.all(np.isfinite(out_max))
    upper = math.log((1.0 - clip) / clip)
    assert out_max[0] == pytest.approx(upper)
    assert out_max[1] == pytest.approx(upper)

    out_collapse = compute_dl_logits(
        df, collapse_positive_classes=("b",), class_labels=["a", "b"], clip=clip
    )
    assert np.all(np.isfinite(out_collapse))
    lower = math.log(clip / (1.0 - clip))
    assert out_collapse[0] == pytest.approx(lower)
    assert out_collapse[1] == pytest.approx(upper)


def test_per_class_shape_and_values() -> None:
    clip = 1e-7
    rows: list[list[float] | None] = [[0.7, 0.2, 0.1], None, [0.0, 1.0, 0.0]]
    df = _frame(rows)
    out = compute_dl_logits_per_class(df, n_classes=3, clip=clip)
    assert out.shape == (3, 3)
    assert np.all(out[1] == 0.0)
    expected_row0 = np.log(np.clip(np.array([0.7, 0.2, 0.1]), clip, 1.0 - clip))
    assert np.allclose(out[0], expected_row0)
    expected_row2 = np.log(np.clip(np.array([0.0, 1.0, 0.0]), clip, 1.0 - clip))
    assert np.allclose(out[2], expected_row2)
    assert np.all(np.isfinite(out))
