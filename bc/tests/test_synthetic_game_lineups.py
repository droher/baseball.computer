from __future__ import annotations

import polars as pl

from python_models.synthetic_box_scores import (
    build_synthetic_batting_core,
    build_synthetic_fielding_core,
)


def _games(use_dh: bool = False) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "game_id": "AAA188504010",
                "season": 1885,
                "use_dh": use_dh,
                "home_team_id": "HOM",
                "away_team_id": "AWY",
                "home_starting_pitcher_id": "home_pitcher",
                "away_starting_pitcher_id": "rf_modal",
            }
        ]
    )


def _lineups() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "season": 1885,
                "team_id": "AWY",
                "fielding_position": position,
                "player_id": player_id,
            }
            for position, player_id in (
                (1, "modal_pitcher"),
                (2, "catcher"),
                (3, "first_base"),
                (4, "second_base"),
                (5, "third_base"),
                (6, "shortstop"),
                (7, "left_field"),
                (8, "center_field"),
                (9, "rf_modal"),
            )
        ]
    )


def _candidates() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for position, player_id in (
        (1, "modal_pitcher"),
        (2, "catcher"),
        (3, "first_base"),
        (4, "second_base"),
        (5, "third_base"),
        (6, "shortstop"),
        (7, "left_field"),
        (8, "center_field"),
        (9, "rf_modal"),
        (9, "center_field"),
        (9, "right_backup"),
    ):
        rows.append(
            {
                "season": 1885,
                "team_id": "AWY",
                "player_id": player_id,
                "fielding_position": position,
                "games_at_position": {
                    "rf_modal": 100,
                    "center_field": 90,
                    "right_backup": 80,
                }.get(player_id, 70),
                "plate_appearances": {
                    "rf_modal": 500,
                    "right_backup": 300,
                    "modal_pitcher": 50,
                }.get(player_id, 400),
                "games_played": 100,
            }
        )

    return pl.DataFrame(rows)


def test_fielding_pitcher_swap_uses_next_unoccupied_candidate() -> None:
    out = build_synthetic_fielding_core(_games(), _lineups(), _candidates())

    away = out.filter(pl.col("side") == "Away")

    assert away.shape[0] == 9
    assert away["fielder_id"].n_unique() == 9
    assert away["fielding_position"].n_unique() == 9
    assert (
        away.filter(pl.col("fielding_position") == 1).item(0, "fielder_id")
        == "rf_modal"
    )
    assert (
        away.filter(pl.col("fielding_position") == 9).item(0, "fielder_id")
        == "right_backup"
    )


def test_batting_pitcher_swap_uses_same_repaired_lineup() -> None:
    out = build_synthetic_batting_core(_games(), _lineups(), _candidates())

    away = out.filter(pl.col("side") == "Away")

    assert away.shape[0] == 9
    assert away["batter_id"].n_unique() == 9
    assert away["lineup_position"].n_unique() == 9
    assert "rf_modal" in away["batter_id"].to_list()
    assert "right_backup" in away["batter_id"].to_list()
    assert "modal_pitcher" not in away["batter_id"].to_list()


def test_batting_dh_game_does_not_insert_pitcher() -> None:
    out = build_synthetic_batting_core(_games(use_dh=True), _lineups(), _candidates())

    away = out.filter(pl.col("side") == "Away")

    assert away.shape[0] == 9
    assert "modal_pitcher" in away["batter_id"].to_list()
    assert "right_backup" not in away["batter_id"].to_list()
