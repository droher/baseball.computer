from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from python_models.statistical.backtests.geometry_air_regime import (
    AIR_CLASSES,
    AirRegimePrediction,
    SharedCellDraws,
    fit_air_regime,
    predict_air_regime,
)
from python_models.statistical.backtests.geometry_air_regime_predictive import (
    predictive_counts,
    summarize_counts,
)


def frames() -> tuple[pl.DataFrame, pl.DataFrame]:
    rows: list[dict[str, object]] = []
    for index in range(24):
        rows.append(
            {
                "event_key": index,
                "game_id": f"game-{index // 4}",
                "season": 2015 if index < 12 else 2025,
                "recorded_broad_type": "Air",
                "recorded_air_subtype": AIR_CLASSES[index % 3],
                "result_family": "out" if index % 2 else "hit",
                "target_class": AIR_CLASSES[(index + 1) % 3],
                "known_air_evaluation_eligible": True,
            }
        )
    frame = pl.DataFrame(rows)
    return frame, frame.select("event_key", "game_id", "season", "target_class")


def nested_array(value: object) -> np.ndarray[tuple[int, ...], np.dtype[np.int64]]:
    if isinstance(value, pl.Series):
        value = value.to_list()
    return np.asarray(value, dtype=np.int64)


def test_counts_conserve_and_season_draws_are_exact_game_sums() -> None:
    frame, evaluation = frames()
    fit = fit_air_regime(frame, predictor="recorded_result", fit_id="counts", draws=16)
    counts = predictive_counts(fit, predict_air_regime(fit, frame), evaluation)
    games = counts.filter(pl.col("cohort_type") == "game")
    seasons = counts.filter(pl.col("cohort_type") == "season")
    assert games.height == evaluation["game_id"].n_unique()
    assert seasons.height == evaluation["season"].n_unique()
    for row in counts.iter_rows(named=True):
        observed = np.asarray(row["observed_counts"])
        replicated = np.asarray(row["replicated_counts"])
        assert observed.sum() == row["events"]
        np.testing.assert_array_equal(replicated.sum(axis=1), row["events"])
    for season_row in seasons.iter_rows(named=True):
        season = int(season_row["season"])
        game_rows = games.filter(pl.col("season") == season)
        game_draws = sum(
            (nested_array(value) for value in game_rows["replicated_counts"]),
            start=np.zeros((fit.draws, 3), dtype=np.int64),
        )
        game_observed = sum(
            (nested_array(value) for value in game_rows["observed_counts"]),
            start=np.zeros(3, dtype=np.int64),
        )
        np.testing.assert_array_equal(game_draws, season_row["replicated_counts"])
        np.testing.assert_array_equal(game_observed, season_row["observed_counts"])


def test_replay_row_order_and_targets_cannot_change_replicated_draws() -> None:
    frame, evaluation = frames()
    fit = fit_air_regime(frame, predictor="result", fit_id="replay", draws=16)
    first = predictive_counts(fit, predict_air_regime(fit, frame), evaluation)
    reversed_frame = frame.reverse()
    replay = predictive_counts(
        fit,
        predict_air_regime(fit, reversed_frame),
        evaluation.reverse(),
    )
    assert first.equals(replay)
    changed_targets = evaluation.with_columns(pl.lit("Fly").alias("target_class"))
    changed = predictive_counts(fit, predict_air_regime(fit, frame), changed_targets)
    assert first.select(
        "fit_id",
        "predictor",
        "concentration",
        "cohort_type",
        "cohort_id",
        "season",
        "events",
        "replicated_counts",
    ).equals(
        changed.select(
            "fit_id",
            "predictor",
            "concentration",
            "cohort_type",
            "cohort_id",
            "season",
            "events",
            "replicated_counts",
        )
    )


def test_shared_cell_draws_induce_game_covariance_and_season_keeps_draw_index() -> None:
    frame, evaluation = frames()
    frame = frame.head(8).with_columns(
        pl.lit(2015).alias("season"),
        pl.when(pl.int_range(pl.len()) < 4)
        .then(pl.lit("game-a"))
        .otherwise(pl.lit("game-b"))
        .alias("game_id"),
    )
    evaluation = frame.select("event_key", "game_id", "season", "target_class")
    fit = fit_air_regime(frame, predictor="marginal", fit_id="shared", draws=16)
    prediction = predict_air_regime(fit, frame)
    shared = prediction.cell_draws[0]
    probabilities = np.zeros((16, 3), dtype=np.float64)
    probabilities[::2, 0] = 1.0
    probabilities[1::2, 1] = 1.0
    controlled = AirRegimePrediction(
        events=prediction.events,
        cell_draws=(
            SharedCellDraws(
                cell_id=shared.cell_id,
                cell_values=shared.cell_values,
                regime=shared.regime,
                status=shared.status,
                probabilities=probabilities,
            ),
        ),
    )
    counts = predictive_counts(fit, controlled, evaluation)
    games = counts.filter(pl.col("cohort_type") == "game").sort("cohort_id")
    first = nested_array(games["replicated_counts"][0])
    second = nested_array(games["replicated_counts"][1])
    np.testing.assert_array_equal(first, second)
    assert np.var(first[:, 0]) > 0
    season = nested_array(
        counts.filter(pl.col("cohort_type") == "season")["replicated_counts"][0]
    )
    np.testing.assert_array_equal(season, first + second)


def test_summary_has_two_levels_and_split_batch_diagnostics() -> None:
    frame, evaluation = frames()
    fit = fit_air_regime(frame, predictor="marginal", fit_id="summary", draws=16)
    counts = predictive_counts(fit, predict_air_regime(fit, frame), evaluation)
    summary = summarize_counts(counts)
    assert summary.height == counts.height * 3 * 2
    assert set(summary["class_label"]) == set(AIR_CLASSES)
    assert set(summary["interval_level"]) == {0.9, 0.95}
    assert (summary["lower"] <= summary["upper"]).all()
    assert (summary["width"] >= 0).all()
    assert (summary["mc_lower_span"] >= 0).all()
    assert (summary["mc_upper_span"] >= 0).all()
    assert summary["mc_coverage_disagreement_fraction"].is_between(0, 1).all()


def test_empty_inputs_have_explicit_schemas() -> None:
    frame, _ = frames()
    fit = fit_air_regime(frame, predictor="marginal", fit_id="empty", draws=8)
    prediction = predict_air_regime(fit, frame.head(0))
    evaluation = pl.DataFrame(
        schema={
            "event_key": pl.Int64,
            "game_id": pl.String,
            "season": pl.Int64,
            "target_class": pl.String,
        }
    )
    counts = predictive_counts(fit, prediction, evaluation)
    assert counts.is_empty()
    assert summarize_counts(counts).is_empty()


def test_misaligned_unsupported_and_cross_fit_predictions_fail_closed() -> None:
    frame, evaluation = frames()
    fit = fit_air_regime(frame, predictor="marginal", fit_id="fit-a", draws=8)
    prediction = predict_air_regime(fit, frame)
    with pytest.raises(ValueError):
        predictive_counts(fit, prediction, evaluation.head(evaluation.height - 1))
    unsupported = predict_air_regime(
        fit, frame.with_columns(pl.lit(None).alias("recorded_broad_type"))
    )
    with pytest.raises(ValueError):
        predictive_counts(fit, unsupported, evaluation)
    other_fit = fit_air_regime(frame, predictor="marginal", fit_id="fit-b", draws=8)
    with pytest.raises(ValueError):
        predictive_counts(other_fit, prediction, evaluation)


def test_invalid_shared_draws_and_count_summaries_fail_closed() -> None:
    frame, evaluation = frames()
    fit = fit_air_regime(frame, predictor="marginal", fit_id="invalid", draws=8)
    prediction = predict_air_regime(fit, frame)
    shared = prediction.cell_draws[0]
    malformed = AirRegimePrediction(
        events=prediction.events,
        cell_draws=(
            SharedCellDraws(
                cell_id=shared.cell_id,
                cell_values=shared.cell_values,
                regime=shared.regime,
                status=shared.status,
                probabilities=np.ones((8, 3), dtype=np.float64),
            ),
        ),
    )
    with pytest.raises(ValueError):
        predictive_counts(fit, malformed, evaluation)
    counts = predictive_counts(fit, prediction, evaluation)
    mixed = pl.concat([counts, counts.with_columns(pl.lit("other").alias("fit_id"))])
    with pytest.raises(ValueError):
        summarize_counts(mixed)
    corrupt = counts.with_columns(pl.lit(999).alias("events"))
    with pytest.raises(ValueError):
        summarize_counts(corrupt)
