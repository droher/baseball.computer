from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Literal

import polars as pl
import pytest
import duckdb
from hypothesis import given, strategies as st

from python_models.statistical.backtests.geometry_statcast_acquire import (
    PA_INCREMENT_SQL,
    validate_mechanics,
)
from python_models.statistical.backtests.geometry_statcast_bridge import (
    Ball,
    SelectedGame,
    angle_class,
    pair_game,
    read_balls,
    resolve_game,
)


def selected() -> SelectedGame:
    return SelectedGame(
        game_id="HOU202504190",
        season=2025,
        park_id="HOU03",
        date="2025-04-19",
        home_team_id="HOU",
        away_team_id="SDN",
        primary_fold="TRAIN",
        acquisition_fold="fitting",
        is_smoke=True,
    )


def sample() -> tuple[pl.DataFrame, list[Ball], dict[str, set[str]]]:
    frame = pl.DataFrame(
        {
            "event_key": [1, 2],
            "retrosheet_pa_ordinal": [2, 5],
            "batter_id": ["b1", "b2"],
            "pitcher_id": ["p1", "p1"],
            "inning_start": [1, 2],
            "frame_start": ["Top", "Bottom"],
            "recorded_class": ["GroundBallBunt", "LineDrive"],
        }
    )
    cases: list[tuple[int, int, Literal["Top", "Bot"], str, str, str]] = [
        (2, 1, "Top", "10", "ground_ball", "22"),
        (5, 2, "Bot", "20", "line_drive", ""),
    ]
    balls = [
        Ball(
            game_pk=778259,
            game_date="2025-04-19",
            at_bat_number=ordinal,
            inning=inning,
            inning_topbot=half,
            batter=batter,
            pitcher="30",
            bb_type=kind,
            launch_angle=angle,
            launch_speed="80",
            events="single",
            description="hit_into_play",
            type="X",
        )
        for ordinal, inning, half, batter, kind, angle in cases
    ]
    return frame, balls, {"b1": {"10"}, "b2": {"20"}, "p1": {"30"}}


@given(
    st.lists(
        st.floats(min_value=-90, max_value=90, allow_nan=False, allow_infinity=False),
        min_size=1,
    )
)
def test_angle_classes_are_ordered_and_partition_the_domain(
    values: list[float],
) -> None:
    order = {"GroundBall": 0, "LineDrive": 1, "Fly": 2, "PopUp": 3}
    classes = [angle_class(value) for value in sorted(values)]
    assert all(value in order for value in classes)
    ranks = [order[str(value)] for value in classes]
    assert ranks == sorted(ranks)


@pytest.mark.parametrize("angle", [float("nan"), float("inf"), -91, 91])
def test_invalid_tracking_values_are_not_classes(angle: float) -> None:
    with pytest.raises(ValueError, match="launch angle"):
        angle_class(angle)
    assert angle_class(None) is None


def test_paired_rows_preserve_bunt_ontology_missing_angles_and_order() -> None:
    local, balls, crosswalk = sample()
    paired, report = pair_game(local, balls, crosswalk)
    reversed_frame, reversed_report = pair_game(
        local.reverse(), list(reversed(balls)), crosswalk
    )
    assert paired.sort("event_key").equals(reversed_frame.sort("event_key"))
    assert report == reversed_report
    assert report["game_pairing_complete"] is True
    assert report["local_events"] == paired.height
    assert report["matched_angle_available"] == 1
    assert paired.item(0, "recorded_class") == "GroundBallBunt"
    assert paired.item(0, "angle_standardized_class") == "LineDrive"
    assert paired.item(1, "angle_standardized_class") is None


def test_ambiguous_or_missing_identity_never_supplies_a_target() -> None:
    local, balls, crosswalk = sample()
    crosswalk["b1"] = {"10", "11"}
    frame, report = pair_game(local, balls, crosswalk)
    assert frame.item(0, "match_status") == "identity_mismatch_or_missing"
    assert frame.item(0, "angle_standardized_class") is None
    assert report["game_pairing_complete"] is False
    assert frame.height == local.height


def test_pa_duplicates_and_remote_only_rows_remain_explicit() -> None:
    local, balls, crosswalk = sample()
    duplicate_frame, duplicate_report = pair_game(local, [*balls, balls[0]], crosswalk)
    assert duplicate_frame.item(0, "match_status") == "ambiguous_pa"
    assert duplicate_report["unique_remote_pa"] is False
    assert duplicate_report["game_pairing_complete"] is False
    remote_only = balls[0].model_copy(update={"at_bat_number": 99})
    frame, report = pair_game(local, [balls[1], remote_only], crosswalk)
    assert frame.item(0, "match_status") == "missing_remote"
    assert report["remote_only_pa_ordinals"] == [99]
    assert report["game_pairing_complete"] is False
    duplicated_local = local.with_columns(pl.lit(2).alias("retrosheet_pa_ordinal"))
    frame, report = pair_game(duplicated_local, balls, crosswalk)
    assert frame["match_status"].to_list() == ["ambiguous_pa", "ambiguous_pa"]
    assert report["unique_local_pa"] is False


def test_game_resolution_requires_both_clubs_date_and_single_game() -> None:
    game: dict[str, object] = {
        "gamePk": 778259,
        "gameNumber": 1,
        "doubleHeader": "N",
        "gameType": "R",
        "teams": {"home": {"team": {"id": 117}}, "away": {"team": {"id": 135}}},
    }

    def payload(games: list[dict[str, object]]) -> bytes:
        return json.dumps({"dates": [{"date": "2025-04-19", "games": games}]}).encode()

    assert resolve_game(payload([game]), selected()) == game["gamePk"]
    for games in ([], [game, game], [{**game, "doubleHeader": "Y"}]):
        with pytest.raises(ValueError, match="resolution"):
            resolve_game(payload(games), selected())


def test_csv_retains_unclassified_in_play_rows_and_rejects_other_games() -> None:
    local, balls, crosswalk = sample()
    del local, crosswalk
    row = balls[0].model_dump()
    row["bb_type"] = ""
    csv = ",".join(row) + "\n" + ",".join(str(value) for value in row.values())
    loaded = read_balls(csv.encode(), selected(), balls[0].game_pk)
    assert len(loaded) == 1 and loaded[0].bb_type == ""
    with pytest.raises(ValueError, match="unexpected"):
        read_balls(csv.encode(), selected(), balls[0].game_pk + 1)


def test_full_acquisition_rejects_unverified_mechanics(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(json.dumps({"files_sha256": {}}))
    games = pl.DataFrame({"game_id": ["game"], "is_smoke": [True]})
    games.write_parquet(tmp_path / "selected_fitting_games.parquet")
    (tmp_path / "report.json").write_text(
        json.dumps({"reports": [], "modern_angle_evaluation_acquired": False})
    )
    digest = hashlib.sha256((tmp_path / "manifest.json").read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="mechanics acceptance evidence"):
        validate_mechanics(tmp_path, expected_manifest_sha256=digest, crosswalk={})
    with pytest.raises(ValueError, match="manifest differs"):
        validate_mechanics(
            tmp_path, expected_manifest_sha256="unaccepted", crosswalk={}
        )
    (tmp_path / "manifest.json").write_text(
        json.dumps({"files_sha256": {"selected_fitting_games.parquet": "x"}})
    )
    digest = hashlib.sha256((tmp_path / "manifest.json").read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="declared smoke games"):
        validate_mechanics(
            tmp_path,
            expected_manifest_sha256=digest,
            crosswalk={},
            declared_games={"other"},
        )


def test_pa_counter_includes_inning_ending_partial_appearances_only_once() -> None:
    events = pl.DataFrame(
        {
            "event_id": list(range(6)),
            "plate_appearance_result": [
                "Single",
                None,
                None,
                "StrikeOut",
                None,
                "InPlayOut",
            ],
            "no_play_flag": [False, False, False, False, True, False],
            "outs": [0, 1, 2, 2, 2, 0],
            "outs_on_play": [0, 1, 1, 1, 1, 1],
        }
    )
    with duckdb.connect() as connection:
        connection.register("events", events)
        actual = connection.execute(
            f"SELECT sum({PA_INCREMENT_SQL}) OVER (ORDER BY event_id) FROM events ORDER BY event_id"
        ).fetchall()
    assert [row[0] for row in actual] == [1, 1, 2, 3, 3, 4]
