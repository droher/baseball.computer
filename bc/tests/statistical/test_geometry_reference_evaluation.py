from __future__ import annotations

import math
import json
from collections.abc import Sequence
from typing import cast

import polars as pl
import pytest

from python_models.statistical.backtests.geometry_reference_evaluation import (
    evaluate_reference,
)


CLASS_LABELS = ("Left", "Right")


def reference_frame() -> pl.DataFrame:
    labels = ["Left", "Right"] * 6
    model = [[0.98, 0.02] if label == "Left" else [0.02, 0.98] for label in labels]
    contextual = [[0.70, 0.30] if label == "Left" else [0.30, 0.70] for label in labels]
    full = [[0.85, 0.15] if label == "Left" else [0.15, 0.85] for label in labels]
    return pl.DataFrame(
        {
            "event_key": list(range(12)),
            "game_id": [f"game-{index // 2}" for index in range(12)],
            "season": [1975, 1975, 1985, 1985, 1995, 1995] * 2,
            "target_class": labels,
            "model_probability": model,
            "budget_marginal_probability": [[0.5, 0.5]] * 12,
            "budget_contextual_probability": contextual,
            "full_contextual_probability": full,
        }
    )


def test_reference_metrics_bootstrap_reliability_and_decision() -> None:
    result = evaluate_reference(
        reference_frame(), CLASS_LABELS, repetitions=100, seed=7
    )
    assert json.loads(json.dumps(result))["row_count"] == 12

    metrics = cast(
        dict[str, dict[str, dict[str, float | int | None]]], result["metrics"]
    )
    overall = metrics["overall"]
    assert overall["model"]["log_loss"] == pytest.approx(-math.log(0.98))
    assert overall["model"]["brier"] == pytest.approx(2 * 0.02**2)
    assert overall["model"]["classwise_ece_15_bin"] == pytest.approx(0.02)
    assert metrics["pre_1988_observed_only"]["model"]["row_count"] == 8

    bootstrap = cast(list[dict[str, object]], result["paired_game_bootstrap"])
    assert len(bootstrap) == 6
    assert all(row["train_fit_during_bootstrap"] == "fixed" for row in bootstrap)
    assert all(cast(list[float], row["log_loss_gain_ci95"])[0] > 0 for row in bootstrap)
    assert all(cast(list[float], row["brier_gain_ci95"])[0] > 0 for row in bootstrap)

    reliability = cast(list[dict[str, object]], result["reliability_bins"])
    assert reliability
    assert {row["slice"] for row in reliability} == {
        "overall",
        "pre_1988_observed_only",
    }
    assert cast(dict[str, bool], result["decision_flags"]) == {
        "log_loss_improvement_ci_lower_above_zero_vs_budget_contextual": True,
        "brier_improvement_ci_lower_above_zero_vs_budget_contextual": True,
        "model_classwise_ece_at_most_0_05": True,
        "no_pre_1988_observed_only_log_loss_regression": True,
        "passed": True,
    }


@pytest.mark.parametrize(
    ("column", "replacement", "message"),
    [
        ("event_key", [0] * 12, "event_key must be unique"),
        ("target_class", ["Unknown"] * 12, "outside class_labels"),
        ("model_probability", [[0.4, 0.4]] * 12, "not normalized"),
        ("model_probability", [[-0.1, 1.1]] * 12, "negative"),
        ("model_probability", [[0.5, 0.5, 0.0]] * 12, "shape"),
    ],
)
def test_invalid_frames_are_rejected(
    column: str, replacement: Sequence[object], message: str
) -> None:
    frame = reference_frame().with_columns(pl.Series(column, replacement))

    with pytest.raises(ValueError, match=message):
        _ = evaluate_reference(frame, CLASS_LABELS, repetitions=10)


def test_nonfinite_probabilities_and_nonpositive_repetitions_are_rejected() -> None:
    frame = reference_frame().with_columns(
        pl.Series("model_probability", [[float("nan"), 0.0]] * 12)
    )
    with pytest.raises(ValueError, match="non-finite"):
        _ = evaluate_reference(frame, CLASS_LABELS, repetitions=10)

    with pytest.raises(ValueError, match="repetitions must be positive"):
        _ = evaluate_reference(reference_frame(), CLASS_LABELS, repetitions=0)


def test_missing_historical_slice_fails_historical_decision_only() -> None:
    frame = reference_frame().with_columns(pl.lit(2000).alias("season"))

    result = evaluate_reference(frame, CLASS_LABELS, repetitions=20, seed=3)

    metrics = cast(
        dict[str, dict[str, dict[str, float | int | None]]], result["metrics"]
    )
    pre_metrics = metrics["pre_1988_observed_only"]
    assert all(values["row_count"] == 0 for values in pre_metrics.values())
    flags = cast(dict[str, bool], result["decision_flags"])
    assert flags["no_pre_1988_observed_only_log_loss_regression"] is False
    assert flags["passed"] is False
