from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.backtests.historical_stress import (
    load_profile,
    select_scenario,
)
from python_models.statistical.evidence_binding import file_digest
from python_models.statistical.backtests.historical_stress_data import (
    FEATURES,
    apply_joint_masks,
    contextual_baseline,
    feature_frame,
    game_digest,
    verify_exclusions,
)


def frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "event_key": [1, 2, 3, 4],
            "game_id": ["a", "a", "b", "c"],
            "season": [1980, 1980, 1990, 2000],
            "era": ["1980", "1980", "1990", "2000"],
            "scorer": ["one", "one", "two", "three"],
            "cleaned_scorers": [["one", "co"], ["one", "co"], ["two"], ["three"]],
            "scorer_holdout_selected": [True, True, False, False],
            "primary_fold": ["TRAIN"] * 4,
            "target_class": ["Fly", "GroundBall", "Fly", "GroundBall"],
            "result_family": ["out", "hit", "out", "hit"],
            "base_state_start": ["0", "0", "1", "2"],
            "outs_start": ["0", "1", "2", "1"],
            "alignment_regime": ["standard"] * 4,
            "batter_hand": ["L", "R", "__MISSING__", "R"],
        }
    )


def test_joint_masks_preserve_game_blocks_truth_and_existing_missingness() -> None:
    data = frame()
    profile = pl.DataFrame(
        {
            "era": ["1980", "1980", "1980", "1980"],
            "scorer": ["one", "one", "*", "*"],
            "mask_pattern": ["010001", "000000", "010001", "000000"],
            "rows": [0.5, 0.5, 1.0, 1.0],
        }
    )
    masked = apply_joint_masks(data, profile, seed=19, target="trajectory")
    reverse = apply_joint_masks(
        data.reverse(), profile.reverse(), seed=19, target="trajectory"
    ).sort("event_key")
    assert masked.equals(reverse)
    assert masked["target_class"].equals(data["target_class"])
    assert masked.filter(pl.col("game_id") == "a")["mask_pattern"].n_unique() == 1
    assert (
        masked.filter(pl.col("game_id") == "b")["batter_hand"].item() == "__MISSING__"
    )
    for row in masked.iter_rows(named=True):
        for index, field in enumerate(FEATURES):
            if str(row["mask_pattern"])[index] == "1":
                assert row[field] == "__MISSING__"
    prepared, columns = feature_frame(masked, "trajectory")
    assert "era_result" in columns
    assert prepared["era_result"].to_list() == [
        f"{era}|{result}"
        for era, result in masked.select("era", "result_family").iter_rows()
    ]


def test_masks_never_depend_on_test_truth() -> None:
    data = frame()
    profile = pl.DataFrame(
        {"era": ["1980"], "scorer": ["*"], "mask_pattern": ["000001"], "rows": [2]}
    )
    first = apply_joint_masks(data, profile, seed=19, target="trajectory")
    changed = apply_joint_masks(
        data.with_columns(pl.lit("unread").alias("target_class")),
        profile,
        seed=19,
        target="trajectory",
    )
    assert first.select(*FEATURES, "mask_pattern").equals(
        changed.select(*FEATURES, "mask_pattern")
    )


def test_baseline_falls_back_to_available_context_and_ignores_test_labels() -> None:
    train = frame()
    test = train.with_columns(pl.lit("unseen_era").alias("era"))
    labels = ("Fly", "GroundBall")
    first = contextual_baseline(train, test, labels)
    second = contextual_baseline(
        train.reverse(),
        test.with_columns(pl.lit("unread").alias("target_class")),
        labels,
    )
    np.testing.assert_allclose(first.probabilities, second.probabilities)
    np.testing.assert_allclose(first.probabilities.sum(axis=1), 1)
    assert set(first.routes) == {"result"}
    missing = test.with_columns(pl.lit("__MISSING__").alias("result_family"))
    assert set(contextual_baseline(train, missing, labels).routes) == {"marginal"}
    assert first.probabilities[0, 0] > first.probabilities[0, 1]


def test_reserve_and_primary_fold_exclusions_fail_closed() -> None:
    train = frame().head(2)
    test = frame().tail(2).with_columns(pl.lit("TEST").alias("primary_fold"))
    assert verify_exclusions(train, test, {"reserved"})["reserve_overlap"] == 0
    with pytest.raises(ValueError, match="reserved"):
        verify_exclusions(train, test, {"b"})
    with pytest.raises(ValueError, match="TEST"):
        verify_exclusions(
            train, test.with_columns(pl.lit("VALIDATE").alias("primary_fold")), set()
        )
    with pytest.raises(ValueError, match="overlap"):
        verify_exclusions(
            train, train.with_columns(pl.lit("TEST").alias("primary_fold")), set()
        )
    assert game_digest(["a", "b", "a"]) == game_digest(["b", "a"])


def test_scenarios_exclude_entire_scorer_groups_and_historical_training() -> None:
    data = frame()
    train, test = select_scenario(data, data, "scorer", ["one"])
    assert set(train["game_id"]) == {"b", "c"}
    assert set(test["game_id"]) == {"a"}
    train, test = select_scenario(data, data, "backward", [])
    assert train.filter(pl.col("season") < 1988).is_empty()
    assert test.filter(pl.col("season") >= 1988).is_empty()


def test_profile_binding_and_natural_status_boundary(tmp_path: Path) -> None:
    pl.DataFrame({"game_id": ["a"]}).write_parquet(
        tmp_path / "frozen_game_metadata.parquet"
    )
    pl.DataFrame(
        {
            "profile_level": ["era_pool"] * 3,
            "target": ["trajectory"] * 3,
            "era": [1980] * 3,
            "scorer": ["__all__"] * 3,
            "observed_status": ["unknown_code", "derived", "default_code"],
            "joint_feature_mask": ["000000", "111111", "000001"],
            "weighted_event_count": [2.5, 1000.0, 0.5],
        }
    ).write_parquet(tmp_path / "train_joint_feature_patterns.parquet")
    (tmp_path / "profile.json").write_text("{}")
    inventory = {path.name: file_digest(path) for path in tmp_path.iterdir()}
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"files_sha256": inventory}))
    expected = file_digest(manifest)
    _, profile, _ = load_profile(tmp_path, "trajectory", expected)
    assert set(profile["mask_pattern"]) == {"000000", "000001"}
    assert profile["rows"].sum() == 3.0
    assert set(profile["scorer"]) == {"*"}
    (tmp_path / "profile.json").write_text('{"tampered":true}')
    with pytest.raises(ValueError, match="profile artifact changed"):
        load_profile(tmp_path, "trajectory", expected)
    inventory["profile.json"] = file_digest(tmp_path / "profile.json")
    manifest.write_text(json.dumps({"files_sha256": inventory}))
    with pytest.raises(ValueError, match="frozen input contract"):
        load_profile(tmp_path, "trajectory", expected)
