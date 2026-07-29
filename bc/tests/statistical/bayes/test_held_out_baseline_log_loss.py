"""Held-out marginal-entropy baseline emitted alongside ``baseline_top1_accuracy``."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

import math

import arviz as az
import numpy as np
import pytest

from python_models.statistical.bayes.training import (
    _empirical_class_entropy,
    _evaluate_held_out,
)
from python_models.statistical.models._credit_data import (
    EventCreditInputs,
    FixedEffectDesign,
    HeldOutSet,
)
from python_models.statistical.validate import _derive_baseline_log_loss

N_CLASSES = 4
N_FE_LEVELS = 3


def _entropy_of(labels: np.ndarray, *, n_classes: int) -> float:
    n = int(labels.shape[0])
    shares = [float((labels == k).sum()) / n for k in range(n_classes)]
    return -sum(p * math.log(p) for p in shares if p > 0.0)


def _idata(rng: np.random.Generator) -> az.InferenceData:
    n_chain, n_draw = 2, 5
    return az.from_dict(
        posterior={
            "alpha_position": rng.normal(size=(n_chain, n_draw, N_CLASSES)),
            "delta_result_family": rng.normal(
                size=(n_chain, n_draw, N_FE_LEVELS, N_CLASSES)
            ),
        }
    )


def _inputs(labels: np.ndarray) -> EventCreditInputs:
    n = int(labels.shape[0])
    rng = np.random.default_rng(11)
    zeros = np.zeros(n, dtype=np.int64)
    fe = {
        "result_family": FixedEffectDesign(
            levels=tuple("abc"),
            codes=rng.integers(0, N_FE_LEVELS, size=n).astype(np.int64),
        )
    }
    held = HeldOutSet(
        event_keys=np.arange(n, dtype=np.int64),
        true_position=labels.astype(np.int64),
        U=np.ones(n, dtype=np.int64),
        season_idx=zeros.copy(),
        scorer_idx=zeros.copy(),
        park_idx=zeros.copy(),
        source_idx=zeros.copy(),
        fixed_effects=fe,
    )
    return EventCreditInputs(
        U=np.ones(0, dtype=np.int64),
        event_keys=np.zeros(0, dtype=np.int64),
        credit_type="putout",
        n_positions=N_CLASSES,
        season_idx=np.zeros(0, dtype=np.int64),
        scorer_idx=np.zeros(0, dtype=np.int64),
        park_idx=np.zeros(0, dtype=np.int64),
        source_idx=np.zeros(0, dtype=np.int64),
        fixed_effects={},
        global_effects={},
        coords={"position": [str(k + 1) for k in range(N_CLASSES)]},
        is_masked=np.zeros(0, dtype=np.bool_),
        Y_supervised_event_idx=np.zeros(0, dtype=np.int64),
        Y_supervised_counts=np.zeros((0, N_CLASSES), dtype=np.int64),
        Y_supervised_U=np.zeros(0, dtype=np.int64),
        aggregate_targets=np.zeros(0, dtype=np.float64),
        aggregate_sigma=np.zeros(0, dtype=np.float64),
        aggregate_event_idx=np.zeros(0, dtype=np.int64),
        aggregate_position_idx=np.zeros(0, dtype=np.int64),
        aggregate_row_idx=np.zeros(0, dtype=np.int64),
        held_out=held,
    )


def test_entropy_of_uniform_labels_is_log_k() -> None:
    labels = np.arange(4 * N_CLASSES, dtype=np.int64) % N_CLASSES
    got = _empirical_class_entropy(labels, n_classes=N_CLASSES)
    assert got == pytest.approx(math.log(N_CLASSES))


def test_entropy_matches_independently_computed_shares() -> None:
    labels = np.array([0] * 40 + [1] * 30 + [2] * 20 + [3] * 10, dtype=np.int64)
    got = _empirical_class_entropy(labels, n_classes=N_CLASSES)
    assert got == pytest.approx(_entropy_of(labels, n_classes=N_CLASSES))
    assert got < math.log(N_CLASSES)


def test_entropy_of_degenerate_label_sets_is_zero() -> None:
    single = np.full(9, 2, dtype=np.int64)
    assert _empirical_class_entropy(single, n_classes=N_CLASSES) == 0.0
    empty = np.zeros(0, dtype=np.int64)
    assert _empirical_class_entropy(empty, n_classes=N_CLASSES) == 0.0


def test_evaluate_held_out_emits_baseline_log_loss() -> None:
    rng = np.random.default_rng(5)
    labels = rng.choice(
        np.arange(N_CLASSES), size=200, p=[0.55, 0.25, 0.15, 0.05]
    ).astype(np.int64)
    result = _evaluate_held_out(_inputs(labels), _idata(rng))

    baseline = result["baseline_log_loss"]
    assert isinstance(baseline, float)
    assert baseline == pytest.approx(_entropy_of(labels, n_classes=N_CLASSES))
    assert 0.0 < baseline <= math.log(N_CLASSES)
    assert result["baseline_top1_accuracy"] == pytest.approx(
        float((labels == 0).mean())
    )


def test_baseline_log_loss_matches_distribution_calibration_shares() -> None:
    """The validator's fallback must recover the emitted value from real output.

    ``validate._derive_baseline_log_loss`` reads the literal
    ``distribution_calibration.per_position[*].empirical_share`` keys this
    producer writes; calling it here pins that cross-module contract.
    """
    rng = np.random.default_rng(6)
    labels = rng.choice(np.arange(N_CLASSES), size=180, p=[0.4, 0.3, 0.2, 0.1]).astype(
        np.int64
    )
    result = _evaluate_held_out(_inputs(labels), _idata(rng))

    derived = _derive_baseline_log_loss(result)
    assert derived is not None
    assert derived == pytest.approx(result["baseline_log_loss"])


def test_degenerate_held_out_is_unusable_on_both_baseline_paths() -> None:
    labels = np.full(120, 2, dtype=np.int64)
    result = _evaluate_held_out(_inputs(labels), _idata(np.random.default_rng(7)))

    emitted = result["baseline_log_loss"]
    assert isinstance(emitted, float)
    assert emitted <= 0.0
    assert _derive_baseline_log_loss(result) is None
