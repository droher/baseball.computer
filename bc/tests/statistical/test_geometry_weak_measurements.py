from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl
import pytest

from python_models.statistical.backtests.geometry_weak_measurements import (
    CANDIDATE_SQL,
    FORBIDDEN_CANDIDATE_INPUTS,
    binary_summary,
    game_digest,
    inner_fold,
    normalized_recorded_target,
    recorded_broad_class,
    verify_no_reserve_overlap,
    verify_reserve,
    wilson_interval,
)
from python_models.statistical.evidence_binding import file_digest


def test_candidate_query_is_label_blind() -> None:
    lowered = CANDIDATE_SQL.lower()
    assert not [name for name in FORBIDDEN_CANDIDATE_INPUTS if name in lowered]
    assert "batted_to_fielder between 1 and 6" in lowered
    assert "assisted_putouts" in lowered
    assert "plate_appearance_result = 'homerun'" in lowered
    assert "category_depth = 'outfield'" in lowered


def test_inner_fold_matches_frozen_game_hash_rule() -> None:
    games = [f"game-{index}" for index in range(100)]
    expected = [
        "inner_evaluation"
        if int.from_bytes(
            hashlib.sha256(f"geometry-reliability-inner-v1:{game}".encode()).digest()[
                :8
            ],
            "big",
        )
        % 5
        == 0
        else "inner_fit"
        for game in games
    ]
    assert [inner_fold(game) for game in games] == expected
    assert set(expected) == {"inner_fit", "inner_evaluation"}


@pytest.mark.parametrize(
    ("raw_value", "target", "broad"),
    [
        ("GroundBall", "GroundBall", "GroundBall"),
        ("Fly", "Fly", "AirBall"),
        ("LineDrive", "LineDrive", "AirBall"),
        ("PopUp", "PopUp", "AirBall"),
        ("GroundBallBunt", "Bunt", "GroundBall"),
        ("LineDriveBunt", "Bunt", "AirBall"),
        ("PopUpBunt", "Bunt", "AirBall"),
        ("UnspecifiedBunt", "Bunt", None),
        ("FoulBunt", "Bunt", None),
    ],
)
def test_bunt_target_and_recorded_broad_class_are_distinct(
    raw_value: str, target: str, broad: str | None
) -> None:
    assert normalized_recorded_target(raw_value) == target
    assert recorded_broad_class(raw_value) == broad


def test_binary_summary_reports_recorded_label_agreement_and_unscorable_mass() -> None:
    frame = pl.DataFrame(
        {
            "candidate_positive": [True, True, False, False, True],
            "recorded_broad_class": [
                "GroundBall",
                "AirBall",
                "GroundBall",
                "AirBall",
                None,
            ],
            "rows": [8, 2, 2, 8, 3],
        }
    )
    result = binary_summary(frame, "exact_ground", "GroundBall")
    assert result["candidate_positive_recorded_positive_rows"] == 8
    assert result["candidate_positive_recorded_other_rows"] == 2
    assert result["candidate_negative_recorded_positive_rows"] == 2
    assert result["unscorable_recorded_rows"] == 3
    assert result["observed_rows"] == 23
    assert result["candidate_positive_rows"] == 13
    assert result["candidate_coverage"] == 13 / 23
    assert result["recorded_label_positive_agreement"] == 0.8
    assert result["recorded_positive_capture"] == 0.8
    assert (
        wilson_interval(8, 10)
        == result["recorded_label_positive_agreement_wilson95_descriptive_row_iid"]
    )
    assert "not inferential accuracy intervals" in str(result["interval_scope"])


def test_reserve_binding_and_overlap_guard(tmp_path: Path) -> None:
    reserve_path = tmp_path / "reserve.parquet"
    games = ["GAME3", "GAME1", "GAME2"]
    pl.DataFrame({"game_id": games}).write_parquet(reserve_path)
    reserve = verify_reserve(
        reserve_path, file_digest(reserve_path), game_digest(games)
    )
    assert reserve == set(games)
    assert verify_no_reserve_overlap(["TRAIN1", "TRAIN2"], reserve) == 0
    with pytest.raises(ValueError, match="selected games overlap reserve"):
        _ = verify_no_reserve_overlap(["TRAIN1", "GAME2"], reserve)


def test_reserve_binding_rejects_content_and_id_digest_mismatch(
    tmp_path: Path,
) -> None:
    reserve_path = tmp_path / "reserve.parquet"
    pl.DataFrame({"game_id": ["GAME1", "GAME2"]}).write_parquet(reserve_path)
    digest = file_digest(reserve_path)
    with pytest.raises(ValueError, match="reserve file digest mismatch"):
        _ = verify_reserve(reserve_path, "0" * 64, game_digest(["GAME1", "GAME2"]))
    with pytest.raises(ValueError, match="reserve game ID digest mismatch"):
        _ = verify_reserve(reserve_path, digest, "0" * 64)


def test_invalid_domains_and_counts_are_rejected() -> None:
    with pytest.raises(ValueError, match="unknown observed trajectory"):
        _ = normalized_recorded_target("Unknown")
    with pytest.raises(ValueError, match="unknown observed trajectory"):
        _ = recorded_broad_class("Unknown")
    with pytest.raises(ValueError, match="invalid binomial counts"):
        _ = wilson_interval(2, 1)
