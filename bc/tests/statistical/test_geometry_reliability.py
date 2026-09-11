from __future__ import annotations

import polars as pl
import pytest

from python_models.statistical.backtests.geometry_reliability import (
    cohort_calibration,
    development_decision,
    select_frames,
    verify_development_split,
)
from python_models.statistical.backtests.geometry_reliability_data import (
    inner_fold,
    train_query,
    validate_frames,
)


def frames() -> pl.DataFrame:
    games = [f"game-{index}" for index in range(100)]
    return pl.DataFrame(
        {
            "event_key": list(range(100)),
            "game_id": games,
            "season": [1955 if index % 2 else 2005 for index in range(100)],
            "era": ["1950" if index % 2 else "2000" for index in range(100)],
            "result_family": ["out"] * 100,
            "target_class": [
                "GroundBall" if index % 3 else "Fly" for index in range(100)
            ],
            "primary_fold": ["TRAIN"] * 100,
            "inner_fold": [inner_fold(game) for game in games],
        }
    )


def test_folds_and_temporal_blocks_do_not_depend_on_labels_or_row_order() -> None:
    full = frames()
    changed = full.reverse().with_columns(pl.lit("other_truth").alias("target_class"))
    for scenario in ("game", "backward_1988"):
        train, evaluation = select_frames(full, full.lazy(), scenario)
        alternate_train, alternate_evaluation = select_frames(
            changed, changed.lazy(), scenario
        )
        assert train["event_key"].to_list() == alternate_train["event_key"].to_list()
        assert (
            evaluation["event_key"].to_list()
            == alternate_evaluation["event_key"].to_list()
        )
        assert verify_development_split(train, evaluation, set())["game_overlap"] == 0
        if scenario == "backward_1988":
            assert min(train["season"]) > max(evaluation["season"])


def test_rejects_reserved_wrong_partition_and_forged_inner_assignments() -> None:
    full = frames()
    train, evaluation = select_frames(full, full.lazy(), "game")
    with pytest.raises(ValueError, match="reserve"):
        verify_development_split(
            train, evaluation, {str(evaluation.item(0, "game_id"))}
        )
    with pytest.raises(ValueError, match="non-TRAIN"):
        verify_development_split(
            train, evaluation.with_columns(pl.lit("TEST").alias("primary_fold")), set()
        )
    forged = train.head(1).with_columns(pl.lit("inner_evaluation").alias("inner_fold"))
    with pytest.raises(ValueError, match="frozen fold rule"):
        verify_development_split(train, forged, set())
    with pytest.raises(ValueError, match="duplicate"):
        verify_development_split(
            train, pl.concat([evaluation, evaluation.head(1)]), set()
        )


def test_source_parity_checks_joint_rows_and_complete_class_counts() -> None:
    full = frames().drop("inner_fold")
    selected = full.head(10)
    keys = ["era", "result_family", "target_class"]
    counts = full.group_by(keys).len(name="n")
    assert (
        validate_frames(full, selected, counts, set())["selected_row_mismatches"] == 0
    )
    changed = selected.with_columns(pl.lit("incorrect").alias("target_class"))
    with pytest.raises(ValueError, match="selected TRAIN differs"):
        validate_frames(full, changed, counts, set())
    with pytest.raises(ValueError, match="class counts differ"):
        validate_frames(
            full, selected, counts.with_columns((pl.col("n") + 1).alias("n")), set()
        )
    with pytest.raises(ValueError, match="reserve"):
        validate_frames(full, selected, counts, {str(full.item(0, "game_id"))})
    with pytest.raises(ValueError, match="not unique"):
        validate_frames(pl.concat([full, full.head(1)]), selected, counts, set())


def test_query_conversion_requires_the_frozen_partition_boundary() -> None:
    query = "SELECT event_key FROM main_models.model_input_geometry WHERE primary_fold IN ('TRAIN', 'TEST')"
    converted = train_query(query)
    assert "primary_fold = 'TRAIN'" in converted
    assert "TEST" not in converted
    assert "main_models__geometry_v2_research_20260911" in converted
    with pytest.raises(ValueError, match="partition boundary"):
        train_query(query.replace("'TEST'", "'VALIDATE'"))
    with pytest.raises(ValueError, match="TRAIN-only"):
        train_query(
            query.replace("main_models.model_input_geometry", "main_models.other")
        )


def test_cohort_calibration_cannot_be_rescued_by_an_overall_average() -> None:
    good: dict[str, object] = {
        "row_count": 10000,
        "game_count": 1000,
        "predictors": {
            "model": {"classwise_ece_15_bin": 0.0, "max_absolute_class_share_bias": 0.0}
        },
    }
    failed = {
        **good,
        "predictors": {
            "model": {"classwise_ece_15_bin": 0.5, "max_absolute_class_share_bias": 0.5}
        },
    }
    empty = {"row_count": 0, "game_count": 0, "predictors": None}
    report = cohort_calibration(
        {
            "slices": {
                "overall": good,
                "pre_1988": good,
                "decade_1950": failed,
                "decade_1910": empty,
            }
        }
    )
    assert report["pre_1988"] == {
        "status": "passed",
        "classwise_ece": 0.0,
        "max_class_share_bias": 0.0,
        "events": 10000,
        "games": 1000,
    }
    assert isinstance(report["decade_1950"], dict)
    assert report["decade_1950"]["status"] == "failed"
    assert report["decade_1910"] == {"status": "unsupported", "events": 0, "games": 0}


def test_development_decision_requires_every_gate_and_never_claims_reliability() -> (
    None
):
    cohort = {
        "row_count": 10000,
        "game_count": 1000,
        "predictors": {
            "model": {"classwise_ece_15_bin": 0.0, "max_absolute_class_share_bias": 0.0}
        },
    }
    metrics: dict[str, object] = {
        "decision_flags": {"passed": True},
        "slices": {"pre_1988": cohort},
    }
    result = development_decision(True, metrics)
    assert result["status"] == "passed"
    assert result["full_reliability_established"] is False
    assert development_decision(False, metrics)["status"] == "failed"
    assert development_decision(True, None)["status"] == "unsupported"
    assert (
        development_decision(True, {**metrics, "slices": {}})["status"] == "unsupported"
    )
    assert (
        development_decision(True, {**metrics, "decision_flags": {"passed": False}})[
            "status"
        ]
        == "failed"
    )
    unsupported = {"row_count": 499, "game_count": 50, "predictors": None}
    assert (
        development_decision(
            True,
            {**metrics, "slices": {"pre_1988": cohort, "decade_1900": unsupported}},
        )["status"]
        == "unsupported"
    )
    failed = {
        **cohort,
        "predictors": {
            "model": {
                "classwise_ece_15_bin": 0.051,
                "max_absolute_class_share_bias": 0.0,
            }
        },
    }
    assert (
        development_decision(
            True,
            {**metrics, "slices": {"pre_1988": cohort, "decade_1950": failed}},
        )["status"]
        == "failed"
    )
