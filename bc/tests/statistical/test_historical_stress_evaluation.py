from __future__ import annotations

from typing import cast

import numpy as np
import polars as pl
import pytest

from python_models.statistical.backtests.historical_stress_evaluation import (
    evaluate_stress,
)


def _frame() -> pl.DataFrame:
    rows = 600
    target = ["Left" if index % 2 == 0 else "Right" for index in range(rows)]
    model = [[0.8, 0.2] if label == "Left" else [0.2, 0.8] for label in target]
    baseline = [[0.55, 0.45] if label == "Left" else [0.45, 0.55] for label in target]
    return pl.DataFrame(
        {
            "event_key": range(rows),
            "game_id": [f"game-{index // 10:03d}" for index in range(rows)],
            "season": [1980 + index % 2 for index in range(rows)],
            "scorer": [f"scorer-{index % 3}" for index in range(rows)],
            "mask_pattern": ["source_block" for _ in range(rows)],
            "target_class": target,
            "model_probability": model,
            "baseline_probability": baseline,
        }
    )


def _slices(result: dict[str, object]) -> dict[str, dict[str, object]]:
    return cast(dict[str, dict[str, object]], result["slices"])


def test_probability_validation_and_unique_events() -> None:
    frame = _frame()
    with pytest.raises(ValueError, match="normalized"):
        evaluate_stress(
            frame.with_columns(
                pl.when(pl.col("event_key") == 0)
                .then(pl.lit([0.8, 0.8]))
                .otherwise(pl.col("model_probability"))
                .alias("model_probability")
            ),
            ("Left", "Right"),
            repetitions=2,
        )
    with pytest.raises(ValueError, match="non-finite"):
        evaluate_stress(
            frame.with_columns(
                pl.when(pl.col("event_key") == 0)
                .then(pl.lit([float("nan"), 0.0]))
                .otherwise(pl.col("baseline_probability"))
                .alias("baseline_probability")
            ),
            ("Left", "Right"),
            repetitions=2,
        )
    with pytest.raises(ValueError, match="event_key must be unique"):
        evaluate_stress(
            frame.with_columns(pl.lit(1).alias("event_key")),
            ("Left", "Right"),
            repetitions=2,
        )


def test_evaluation_is_invariant_to_row_permutation() -> None:
    frame = _frame()
    expected = evaluate_stress(frame, ("Left", "Right"), repetitions=20, seed=17)
    actual = evaluate_stress(
        frame.sample(fraction=1.0, shuffle=True, seed=99),
        ("Left", "Right"),
        repetitions=20,
        seed=17,
    )
    assert actual == expected


def test_expected_and_observed_counts_conserve_rows() -> None:
    result = evaluate_stress(_frame(), ("Left", "Right"), repetitions=2)
    overall = _slices(result)["overall"]
    predictors = cast(dict[str, dict[str, object]], overall["predictors"])
    for metrics in predictors.values():
        total_expected = metrics["total_expected_count"]
        assert isinstance(total_expected, float)
        assert np.isclose(total_expected, 600.0)
        assert metrics["total_observed_count"] == 600
        expected = cast(dict[str, float], metrics["expected_class_counts"])
        observed = cast(dict[str, int], metrics["observed_class_counts"])
        assert np.isclose(sum(expected.values()), 600.0)
        assert sum(observed.values()) == 600


def test_empty_historical_slice_is_graceful() -> None:
    frame = _frame().with_columns(pl.lit(2000).alias("season"))
    result = evaluate_stress(frame, ("Left", "Right"), repetitions=2)
    historical = _slices(result)["pre_1988"]
    assert historical["supported"] is False
    assert historical["unsupported_reason"] == "empty"
    assert historical["predictors"] is None
    assert historical["paired_game_bootstrap"] is None


def test_tiny_decade_is_marked_unsupported() -> None:
    result = evaluate_stress(_frame().head(40), ("Left", "Right"), repetitions=2)
    decade = _slices(result)["decade_1980"]
    assert decade["supported"] is False
    assert decade["row_count"] == 40
    assert decade["game_count"] == 4


def test_mask_and_context_support_groups_are_reported() -> None:
    frame = _frame().with_columns(
        pl.when(pl.col("event_key") < 500)
        .then(pl.lit("000000"))
        .otherwise(pl.lit("000001"))
        .alias("mask_pattern"),
        (pl.col("event_key") < 500).alias("model_context_supported"),
    )
    slices = _slices(evaluate_stress(frame, ("Left", "Right"), repetitions=2, seed=17))
    assert slices["mask_pattern_000000"]["supported"] is True
    assert slices["mask_pattern_000001"]["supported"] is False
    assert slices["context_supported"]["supported"] is True
    assert slices["context_unsupported"]["supported"] is False


def test_context_support_must_be_non_null_boolean() -> None:
    frame = _frame().with_columns(pl.lit("yes").alias("model_context_supported"))
    with pytest.raises(ValueError, match="non-null Boolean"):
        evaluate_stress(frame, ("Left", "Right"), repetitions=2)


def test_game_cluster_bootstrap_moves_duplicate_rows_together() -> None:
    frame = pl.DataFrame(
        {
            "event_key": range(20),
            "game_id": ["a"] * 10 + ["b"] * 10,
            "season": [1980] * 20,
            "scorer": ["s"] * 20,
            "mask_pattern": ["m"] * 20,
            "target_class": ["Left"] * 10 + ["Right"] * 10,
            "model_probability": [[0.9, 0.1]] * 20,
            "baseline_probability": [[0.5, 0.5]] * 20,
        }
    )
    result = evaluate_stress(frame, ("Left", "Right"), repetitions=100, seed=7)
    bootstrap = cast(
        dict[str, object], _slices(result)["overall"]["paired_game_bootstrap"]
    )
    assert bootstrap["cluster_unit"] == "game_id"
    assert bootstrap["game_count"] == 2
    log_interval = cast(list[float], bootstrap["log_loss_gain_ci95"])
    game_gains = np.array([np.log(0.9 / 0.5), np.log(0.1 / 0.5)])
    sampled_games = np.random.default_rng(7).integers(0, 2, size=(100, 2))
    expected_draws = game_gains[sampled_games].mean(axis=1)
    assert np.allclose(log_interval, np.quantile(expected_draws, [0.025, 0.975]))


def test_acceptance_flags_use_only_frozen_overall_thresholds() -> None:
    frame = _frame().with_columns(
        pl.lit([0.5, 0.5]).alias("model_probability"),
        pl.when(pl.col("target_class") == "Left")
        .then(pl.lit([0.1, 0.9]))
        .otherwise(pl.lit([0.9, 0.1]))
        .alias("baseline_probability"),
    )
    result = evaluate_stress(frame, ("Left", "Right"), repetitions=20)
    flags = cast(dict[str, bool], result["decision_flags"])
    assert set(flags) == {
        "overall_log_loss_gain_ci_lower_above_zero",
        "overall_brier_gain_ci_lower_above_zero",
        "overall_model_ece_at_most_0_05",
        "overall_model_max_class_share_bias_at_most_0_02",
        "passed",
    }
    assert flags["passed"]
