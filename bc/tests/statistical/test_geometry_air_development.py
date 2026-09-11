from __future__ import annotations

from pathlib import Path
from typing import cast

import numpy as np
import polars as pl

from python_models.statistical.backtests.geometry_air_development import (
    AIR_CLASSES,
    PREDICTORS,
    metadata_smoke_games,
    build_oof_predictions,
    development_decision,
    evaluate_oof_predictions,
)
from python_models.statistical.backtests.geometry_air_standard_data import (
    DEVELOPMENT_SEASONS,
    game_fold,
    park_fold,
)


def _development_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    event_key = 1
    seasons = sorted(DEVELOPMENT_SEASONS)
    for season_index, season in enumerate(seasons):
        for game_index in range(25):
            game_id = f"G{season}{game_index:03d}"
            park_id = f"P{season_index}{game_index % 10:02d}"
            for class_index, target_class in enumerate(AIR_CLASSES):
                recorded_class = AIR_CLASSES[(class_index + game_index) % 3]
                rows.append(
                    {
                        "event_key": event_key,
                        "game_id": game_id,
                        "season": season,
                        "park_id": park_id,
                        "game_header_scorer_proxy": f"S{game_index % 7}",
                        "result_family": f"R{(class_index + game_index) % 4}",
                        "recorded_air_subtype": recorded_class,
                        "recorded_broad_type": "Air",
                        "target_class": target_class,
                        "target_status": "air_angle_standardized",
                        "recorded_bunt": False,
                        "known_air_evaluation_eligible": True,
                    }
                )
                event_key += 1
            for recorded_broad_type, target_class, target_status in (
                ("Ground", "GroundBall", "ground_preserved"),
                (None, None, "broad_source_conflict"),
            ):
                rows.append(
                    {
                        "event_key": event_key,
                        "game_id": game_id,
                        "season": season,
                        "park_id": park_id,
                        "game_header_scorer_proxy": f"S{game_index % 7}",
                        "result_family": "R0",
                        "recorded_air_subtype": None,
                        "recorded_broad_type": recorded_broad_type,
                        "target_class": target_class,
                        "target_status": target_status,
                        "recorded_bunt": False,
                        "known_air_evaluation_eligible": False,
                    }
                )
                event_key += 1
    return pl.DataFrame(rows)


def test_oof_predictions_cover_identical_eligible_rows_once_per_family(
    tmp_path: Path,
) -> None:
    frame = _development_frame()
    predictions, fits = build_oof_predictions(
        frame,
        prior_strengths=(30.0,),
        checkpoint_root=tmp_path,
    )

    eligible = frame.filter(pl.col("known_air_evaluation_eligible"))
    ground = frame.filter(pl.col("recorded_broad_type") == "Ground")
    assert predictions.height == eligible.height * 3
    for family in ("game", "park", "season"):
        family_frame = predictions.filter(pl.col("split_family") == family)
        assert family_frame.height == eligible.height
        assert family_frame["event_key"].n_unique() == eligible.height
        assert set(family_frame["event_key"].to_list()) == set(
            eligible["event_key"].to_list()
        )
        for predictor in PREDICTORS:
            probabilities = np.asarray(
                family_frame[f"probability_{predictor}"].to_list(), dtype=np.float64
            )
            assert probabilities.shape == (eligible.height, len(AIR_CLASSES))
            assert np.allclose(probabilities.sum(axis=1), 1.0)

    assert len(fits) == 14
    for fit in fits:
        training = set(cast(list[str], fit["training_game_ids"]))
        evaluation = set(cast(list[str], fit["evaluation_game_ids"]))
        assert training.isdisjoint(evaluation)
    assert sum(cast(int, fit["ground_events_verified"]) for fit in fits) == (
        ground.height * 3
    )
    assert len(list(tmp_path.rglob("fold-*.json"))) == len(fits)


def test_evaluation_conserves_game_scores_and_support() -> None:
    frame = _development_frame()
    primary_predictions, _ = build_oof_predictions(frame, prior_strengths=(30.0,))
    predictions = pl.concat(
        [
            primary_predictions,
            primary_predictions.with_columns(pl.lit(3.0).alias("prior_strength")),
        ]
    )
    report, metrics, game_scores, descriptive_groups = evaluate_oof_predictions(
        predictions,
        repetitions=20,
    )

    assert metrics.height == 2 * 3 * 5 * len(PREDICTORS)
    assert game_scores["events"].sum() == predictions.height
    assert (
        descriptive_groups.filter(pl.col("slice_type") == "park_id")["events"].sum()
        == predictions.height
    )
    assert (
        descriptive_groups.filter(pl.col("slice_type") == "game_header_scorer_proxy")[
            "events"
        ].sum()
        == predictions.height
    )
    assert set(descriptive_groups["predictor"].to_list()) == {"recorded_result"}
    for predictor in PREDICTORS:
        for index, label in enumerate(AIR_CLASSES):
            expected = float(
                np.asarray(
                    predictions[f"probability_{predictor}"].to_list(),
                    dtype=np.float64,
                )[:, index].sum()
            )
            assert np.isclose(
                float(game_scores[f"predicted_{predictor}_{label}"].sum()), expected
            )
    bootstrap = cast(list[dict[str, object]], report["paired_bootstrap"])
    assert len(bootstrap) == 12
    assert all(row["fits_during_bootstrap"] == "fixed" for row in bootstrap)
    assert all(row["repetitions"] == 20 for row in bootstrap)
    permuted = predictions.sample(fraction=1.0, shuffle=True, seed=9821)
    permuted_report, _, _, _ = evaluate_oof_predictions(
        permuted,
        repetitions=20,
    )
    assert permuted_report["paired_bootstrap"] == report["paired_bootstrap"]


def test_smoke_game_selection_is_metadata_only() -> None:
    frame = _development_frame()
    selected = metadata_smoke_games(frame)
    altered = frame.with_columns(
        pl.lit("PopUp").alias("target_class"),
        pl.lit(False).alias("known_air_evaluation_eligible"),
    )

    assert metadata_smoke_games(altered) == selected
    selected_frame = frame.filter(pl.col("game_id").is_in(selected))
    assert selected_frame.group_by("season").agg(pl.col("game_id").n_unique()).sort(
        "season"
    )["game_id"].to_list() == [3, 3, 3, 3]
    assert {game_fold(value) for value in frame["game_id"].unique().to_list()} == set(
        range(5)
    )
    assert {park_fold(value) for value in frame["park_id"].unique().to_list()} == set(
        range(5)
    )


def test_decision_requires_primary_thresholds_and_sensitivity_stability() -> None:
    metric_rows: list[dict[str, object]] = []
    bootstrap: list[dict[str, object]] = []
    for strength in (3.0, 30.0, 300.0):
        for family in ("game", "park", "season"):
            for slice_type, slice_value in (("overall", "all"), ("season", "2025")):
                metric_rows.append(
                    {
                        "split_family": family,
                        "prior_strength": strength,
                        "slice_type": slice_type,
                        "slice_value": slice_value,
                        "predictor": "recorded_result",
                        "events": 600,
                        "games": 40,
                        "classwise_ece_15_bin": 0.04,
                        "max_absolute_class_share_bias": 0.01,
                    }
                )
            for comparator in ("result", "recorded"):
                bootstrap.append(
                    {
                        "split_family": family,
                        "prior_strength": strength,
                        "comparator": comparator,
                        "log_loss_gain_ci95": [0.01, 0.03],
                        "brier_gain_ci95": [0.01, 0.03],
                    }
                )
    metrics = pl.DataFrame(metric_rows)

    assert development_decision(metrics, bootstrap)["pass"] is True
    unstable = [dict(row) for row in bootstrap]
    unstable[0]["log_loss_gain_ci95"] = [-0.01, 0.03]
    result = development_decision(metrics, unstable)
    assert result["pass"] is False
    assert result["sensitivity_verdict_changed"] is True
