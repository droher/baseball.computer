"""Invariants for the DL-probability logit covariate helpers."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import math

import numpy as np
import polars as pl
import pytest

from python_models.statistical.bayes.dl_covariate import (
    center_dl_log_probs,
    compute_dl_log_probs_per_class,
    compute_dl_log_probs_per_class_with_mask,
    compute_dl_logits,
)


def test_with_mask_marks_null_and_empty_rows_absent() -> None:
    rows: list[list[float] | None] = [[0.7, 0.2, 0.1], None, [], [0.2, 0.3, 0.5]]
    df = _frame(rows)
    log_probs, present = compute_dl_log_probs_per_class_with_mask(df, n_classes=3)
    assert present.dtype == np.bool_
    assert present.tolist() == [True, False, False, True]
    assert np.all(log_probs[~present] == 0.0)
    assert np.all(log_probs[present] < 0.0)
    np.testing.assert_array_equal(
        log_probs, compute_dl_log_probs_per_class(df, n_classes=3)
    )


def test_center_keeps_absent_rows_exactly_zero_and_centers_present_rows() -> None:
    rows: list[list[float] | None] = [[0.7, 0.2, 0.1], None, [0.2, 0.3, 0.5]]
    df = _frame(rows)
    log_probs, present = compute_dl_log_probs_per_class_with_mask(df, n_classes=3)
    means = np.array([-1.0, 0.5, -2.0])
    centered = center_dl_log_probs(log_probs, present, means)
    assert centered.shape == log_probs.shape
    assert np.all(centered[~present] == 0.0)
    np.testing.assert_allclose(centered[present], log_probs[present] - means[None, :])


def test_center_rejects_mismatched_shapes() -> None:
    log_probs = np.zeros((2, 3))
    present = np.ones(2, dtype=np.bool_)
    with pytest.raises(ValueError):
        _ = center_dl_log_probs(log_probs, present, np.zeros(2))
    with pytest.raises(ValueError):
        _ = center_dl_log_probs(log_probs, np.ones(3, dtype=np.bool_), np.zeros(3))


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


def test_per_class_row_length_mismatch_raises() -> None:
    df = _frame([[0.7, 0.2, 0.1], [0.5, 0.5]])
    with pytest.raises(ValueError, match="expected n_classes=3"):
        _ = compute_dl_log_probs_per_class(df, n_classes=3)


def test_per_class_shape_and_values() -> None:
    clip = 1e-7
    rows: list[list[float] | None] = [[0.7, 0.2, 0.1], None, [0.0, 1.0, 0.0]]
    df = _frame(rows)
    out = compute_dl_log_probs_per_class(df, n_classes=3, clip=clip)
    assert out.shape == (3, 3)
    assert np.all(out[1] == 0.0)
    expected_row0 = np.log(np.clip(np.array([0.7, 0.2, 0.1]), clip, 1.0 - clip))
    assert np.allclose(out[0], expected_row0)
    expected_row2 = np.log(np.clip(np.array([0.0, 1.0, 0.0]), clip, 1.0 - clip))
    assert np.allclose(out[2], expected_row2)
    assert np.all(np.isfinite(out))
