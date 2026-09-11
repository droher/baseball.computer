from __future__ import annotations

import numpy as np
import polars as pl

from python_models.statistical.backtests.trajectory_development import (
    FEATURE_COLUMNS,
    fit_logit,
    predict_logit,
    baseline_probabilities,
    score_predictions,
    split_games,
)


def _frame() -> pl.DataFrame:
    rows: list[dict[str, str | int]] = []
    for game in range(100):
        for event in range(2):
            rows.append(
                {
                    "event_key": game * 2 + event,
                    "game_id": f"game-{game}",
                    "era": "1990",
                    "result_family": "hit",
                    "base_state_start": "000",
                    "outs_start": "0",
                    "alignment_regime": "standard",
                    "batter_hand": "R",
                    "target_class": "Fly" if event == 0 else "GroundBall",
                }
            )
    return pl.DataFrame(rows)


def test_internal_split_is_deterministic_and_game_disjoint() -> None:
    first_train, first_development = split_games(_frame(), seed=17)
    second_train, second_development = split_games(_frame(), seed=17)
    assert first_train.equals(second_train)
    assert first_development.equals(second_development)
    assert set(first_train["game_id"]).isdisjoint(first_development["game_id"])
    assert first_train.height + first_development.height == _frame().height


def test_baselines_are_smoothed_normalized_and_uniform_for_unseen_cells() -> None:
    train = _frame().head(20)
    test = pl.DataFrame(
        {
            "era": ["1990", "2000"],
            "result_family": ["hit", "new"],
        }
    )
    labels = ("Fly", "GroundBall")
    marginal = baseline_probabilities(train, test, labels, conditional=False)
    contextual = baseline_probabilities(train, test, labels, conditional=True)
    np.testing.assert_allclose(marginal.sum(axis=1), 1.0)
    np.testing.assert_allclose(contextual.sum(axis=1), 1.0)
    np.testing.assert_allclose(contextual[1], [0.5, 0.5])


def test_score_predictions_matches_direct_binary_calculation() -> None:
    probabilities = np.asarray([[0.8, 0.2], [0.25, 0.75]], dtype=np.float64)
    truth = np.asarray([0, 1], dtype=np.int64)
    metrics = score_predictions(probabilities, truth)
    assert np.isclose(metrics["log_loss"], -np.log([0.8, 0.75]).mean())
    assert np.isclose(metrics["brier"], (0.08 + 0.125) / 2)
    assert 0.0 <= metrics["classwise_ece_15_bin"] <= 1.0


def test_logit_vocab_is_train_only_and_unknown_features_predict() -> None:
    train = _frame()
    fitted = fit_logit(train, columns=FEATURE_COLUMNS)
    unseen = train.head(3).with_columns(pl.lit("unseen-hand").alias("batter_hand"))
    predicted = predict_logit(fitted, unseen, ("Fly", "GroundBall"))
    assert fitted.levels["batter_hand"] == tuple(sorted(train["batter_hand"].unique()))
    assert "unseen-hand" not in fitted.levels["batter_hand"]
    assert np.isfinite(predicted).all()
    np.testing.assert_allclose(predicted.sum(axis=1), 1.0)
