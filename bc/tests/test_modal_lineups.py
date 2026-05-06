"""Unit tests for compute_modal_lineups + the line-score parser."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import duckdb
import polars as pl
import pytest

from python_models.synthetic_box_scores import compute_modal_lineups


def _appearances(rows: Sequence[Mapping[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "season": pl.Int16,
            "team_id": pl.String,
            "player_id": pl.String,
            "fielding_position": pl.UInt8,
            "games_at_position": pl.UInt16,
        },
    )


def _batting(rows: Sequence[Mapping[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "season": pl.Int16,
            "team_id": pl.String,
            "player_id": pl.String,
            "plate_appearances": pl.UInt32,
            "games_played": pl.UInt32,
        },
    )


def _full_team_appearances(
    season: int,
    team_id: str,
    players: Mapping[int, Sequence[tuple[str, int]]],
) -> pl.DataFrame:
    """Helper: players is fielding_position -> [(player_id, games_at_position), ...]."""
    rows: list[dict[str, object]] = []
    for fp, entries in players.items():
        for player_id, games in entries:
            rows.append(
                {
                    "season": season,
                    "team_id": team_id,
                    "player_id": player_id,
                    "fielding_position": fp,
                    "games_at_position": games,
                }
            )
    return _appearances(rows)


def _all_positions_filled(season: int, team_id: str, base: int = 100) -> pl.DataFrame:
    """Helper: produce a default 9-position roster with one player at each."""
    return _full_team_appearances(
        season,
        team_id,
        {pos: [(f"p{pos:02d}player", base)] for pos in range(1, 10)},
    )


def test_modal_fielder_tie_breaks_by_pa_then_player_id() -> None:
    season, team_id = 1885, "CHN"
    apps = _full_team_appearances(
        season,
        team_id,
        {
            1: [("zzpitcher", 50), ("aapitcher", 50)],
            2: [("p02player", 100)],
            3: [("p03player", 100)],
            4: [("p04player", 100)],
            5: [("p05player", 100)],
            6: [("p06player", 100)],
            7: [("p07player", 100)],
            8: [("p08player", 100)],
            9: [("p09player", 100)],
        },
    )
    bat = _batting(
        [
            {
                "season": season,
                "team_id": team_id,
                "player_id": "zzpitcher",
                "plate_appearances": 200,
                "games_played": 50,
            },
            {
                "season": season,
                "team_id": team_id,
                "player_id": "aapitcher",
                "plate_appearances": 100,
                "games_played": 50,
            },
        ]
    )
    out = compute_modal_lineups(apps, bat)
    pitcher_row = out.filter(pl.col("fielding_position") == 1)
    assert pitcher_row.shape[0] == 1
    assert pitcher_row.item(0, "player_id") == "zzpitcher"


def test_modal_fielder_tie_breaks_alphabetically_when_pa_equal() -> None:
    season, team_id = 1885, "BSN"
    apps = _full_team_appearances(
        season,
        team_id,
        {
            1: [("zzpitcher", 50), ("aapitcher", 50)],
            2: [("p02player", 100)],
            3: [("p03player", 100)],
            4: [("p04player", 100)],
            5: [("p05player", 100)],
            6: [("p06player", 100)],
            7: [("p07player", 100)],
            8: [("p08player", 100)],
            9: [("p09player", 100)],
        },
    )
    bat = _batting(
        [
            {
                "season": season,
                "team_id": team_id,
                "player_id": "zzpitcher",
                "plate_appearances": 100,
                "games_played": 50,
            },
            {
                "season": season,
                "team_id": team_id,
                "player_id": "aapitcher",
                "plate_appearances": 100,
                "games_played": 50,
            },
        ]
    )
    out = compute_modal_lineups(apps, bat)
    pitcher_row = out.filter(pl.col("fielding_position") == 1)
    assert pitcher_row.shape[0] == 1
    assert pitcher_row.item(0, "player_id") == "aapitcher"


def test_batting_order_uses_pa_per_game_not_raw_pa() -> None:
    season, team_id = 1885, "PHI"
    apps = _full_team_appearances(
        season,
        team_id,
        {
            1: [("p01player", 100)],
            2: [("p02player", 100)],
            3: [("p03player", 100)],
            4: [("p04player", 100)],
            5: [("p05player", 100)],
            6: [("p06player", 100)],
            7: [("p07player", 100)],
            8: [("p08player", 100)],
            9: [("p09player", 100)],
        },
    )
    bat = _batting(
        [
            {
                "season": season,
                "team_id": team_id,
                "player_id": "p01player",
                "plate_appearances": 1,
                "games_played": 1,
            },
            {
                "season": season,
                "team_id": team_id,
                "player_id": "p02player",
                "plate_appearances": 100,
                "games_played": 162,
            },
        ]
        + [
            {
                "season": season,
                "team_id": team_id,
                "player_id": f"p{pos:02d}player",
                "plate_appearances": 50,
                "games_played": 100,
            }
            for pos in range(3, 10)
        ]
    )
    out = compute_modal_lineups(apps, bat).sort("lineup_position")
    assert out.shape[0] == 9
    first_two = out.head(2)["player_id"].to_list()
    assert first_two[0] == "p01player", (
        f"PA/G=1.0 should outrank PA/G=0.617, got {first_two}"
    )
    assert "p02player" in out["player_id"].to_list()


def test_typical_pre_1901_team_pitcher_bats_ninth() -> None:
    season, team_id = 1885, "BOS"
    apps = _full_team_appearances(
        season,
        team_id,
        {pos: [(f"p{pos:02d}player", 100)] for pos in range(1, 10)},
    )
    bat_rows: list[dict[str, object]] = [
        {
            "season": season,
            "team_id": team_id,
            "player_id": "p01player",
            "plate_appearances": 80,
            "games_played": 40,
        },
    ]
    for pos in range(2, 10):
        bat_rows.append(
            {
                "season": season,
                "team_id": team_id,
                "player_id": f"p{pos:02d}player",
                "plate_appearances": 400,
                "games_played": 100,
            }
        )
    bat = _batting(bat_rows)
    out = compute_modal_lineups(apps, bat).sort("lineup_position")
    pitcher_row = out.filter(pl.col("fielding_position") == 1)
    assert pitcher_row.shape[0] == 1
    assert pitcher_row.item(0, "lineup_position") == 9


def test_empty_appearances_returns_empty_output() -> None:
    apps = _appearances([])
    bat = _batting([])
    out = compute_modal_lineups(apps, bat)
    assert out.is_empty()
    assert set(out.columns) == {
        "season",
        "team_id",
        "lineup_position",
        "fielding_position",
        "player_id",
    }


def test_team_seasons_with_fewer_than_nine_positions_dropped() -> None:
    season, team_id = 1885, "PIT"
    apps = _full_team_appearances(
        season,
        team_id,
        {pos: [(f"p{pos:02d}player", 100)] for pos in range(1, 9)},
    )
    bat = _batting(
        [
            {
                "season": season,
                "team_id": team_id,
                "player_id": f"p{pos:02d}player",
                "plate_appearances": 100,
                "games_played": 50,
            }
            for pos in range(1, 9)
        ]
    )
    out = compute_modal_lineups(apps, bat)
    assert out.is_empty()


def test_zero_games_at_position_filtered_out() -> None:
    season, team_id = 1885, "CIN"
    apps = _appearances(
        [
            {
                "season": season,
                "team_id": team_id,
                "player_id": "ghost",
                "fielding_position": 1,
                "games_at_position": 0,
            },
            {
                "season": season,
                "team_id": team_id,
                "player_id": "p01player",
                "fielding_position": 1,
                "games_at_position": 30,
            },
        ]
        + [
            {
                "season": season,
                "team_id": team_id,
                "player_id": f"p{pos:02d}player",
                "fielding_position": pos,
                "games_at_position": 100,
            }
            for pos in range(2, 10)
        ]
    )
    bat = _batting(
        [
            {
                "season": season,
                "team_id": team_id,
                "player_id": f"p{pos:02d}player",
                "plate_appearances": 100,
                "games_played": 50,
            }
            for pos in range(1, 10)
        ]
    )
    out = compute_modal_lineups(apps, bat)
    assert out.shape[0] == 9
    assert "ghost" not in out["player_id"].to_list()


def test_duplicate_primary_player_uses_next_best_backup() -> None:
    season, team_id = 1885, "BRO"
    apps = _full_team_appearances(
        season,
        team_id,
        {
            1: [("p01player", 100)],
            2: [("shared", 90), ("catcher_backup", 80)],
            3: [("p03player", 100)],
            4: [("shared", 95), ("second_backup", 70)],
            5: [("p05player", 100)],
            6: [("p06player", 100)],
            7: [("p07player", 100)],
            8: [("p08player", 100)],
            9: [("p09player", 100)],
        },
    )
    bat = _batting(
        [
            {
                "season": season,
                "team_id": team_id,
                "player_id": "shared",
                "plate_appearances": 200,
                "games_played": 100,
            },
            {
                "season": season,
                "team_id": team_id,
                "player_id": "catcher_backup",
                "plate_appearances": 150,
                "games_played": 100,
            },
            {
                "season": season,
                "team_id": team_id,
                "player_id": "second_backup",
                "plate_appearances": 50,
                "games_played": 100,
            },
        ]
        + [
            {
                "season": season,
                "team_id": team_id,
                "player_id": f"p{pos:02d}player",
                "plate_appearances": 120,
                "games_played": 100,
            }
            for pos in (1, 3, 5, 6, 7, 8, 9)
        ]
    )
    out = compute_modal_lineups(apps, bat)
    assert out.shape[0] == 9
    assert out["player_id"].n_unique() == 9
    assert (
        out.filter(pl.col("fielding_position") == 2).item(0, "player_id")
        == "catcher_backup"
    )
    assert out.filter(pl.col("fielding_position") == 4).item(0, "player_id") == "shared"


def test_missing_positions_pull_in_best_replacements() -> None:
    season, team_id = 1885, "CLV"
    apps = _full_team_appearances(
        season,
        team_id,
        {
            1: [("p01player", 100)],
            2: [("p02player", 100)],
            3: [("p03player", 100)],
            4: [("second_base_replacement", 60), ("dup4a", 40)],
            5: [("p05player", 100), ("dup4a", 95)],
            6: [("dup6a", 92), ("dup8a", 90), ("shortstop_replacement", 70)],
            7: [("p07player", 100)],
            8: [("center_replacement", 75), ("dup8a", 30)],
            9: [("dup9a", 88), ("right_replacement", 80)],
        },
    )
    bat = _batting(
        [
            {
                "season": season,
                "team_id": team_id,
                "player_id": player_id,
                "plate_appearances": plate_appearances,
                "games_played": 100,
            }
            for player_id, plate_appearances in (
                ("p01player", 120),
                ("p02player", 120),
                ("p03player", 120),
                ("dup4a", 110),
                ("p05player", 120),
                ("dup6a", 105),
                ("p07player", 120),
                ("dup8a", 100),
                ("dup9a", 95),
                ("second_base_replacement", 90),
                ("shortstop_replacement", 92),
                ("center_replacement", 94),
                ("right_replacement", 96),
            )
        ]
    )
    out = compute_modal_lineups(apps, bat)
    assert out.shape[0] == 9
    assert out["player_id"].n_unique() == 9
    assert set(out["fielding_position"].to_list()) == set(range(1, 10))
    assert (
        out.filter(pl.col("fielding_position") == 4).item(0, "player_id")
        == "second_base_replacement"
    )
    assert (
        out.filter(pl.col("fielding_position") == 8).item(0, "player_id")
        == "center_replacement"
    )


def test_missing_pa_falls_back_to_zero_for_ranking() -> None:
    season, team_id = 1872, "BS1"
    apps = _full_team_appearances(
        season,
        team_id,
        {pos: [(f"p{pos:02d}player", 30)] for pos in range(1, 10)},
    )
    bat = _batting([])
    out = compute_modal_lineups(apps, bat).sort("lineup_position")
    assert out.shape[0] == 9
    assert out["player_id"].to_list() == [f"p{pos:02d}player" for pos in range(1, 10)]


def test_team_season_without_nine_distinct_players_dropped() -> None:
    season, team_id = 1872, "PH2"
    apps = _full_team_appearances(
        season,
        team_id,
        {
            1: [("shared_a", 30)],
            2: [("shared_a", 30)],
            3: [("p03player", 30)],
            4: [("p04player", 30)],
            5: [("p05player", 30)],
            6: [("p06player", 30)],
            7: [("p07player", 30)],
            8: [("p08player", 30)],
            9: [("p09player", 30)],
        },
    )
    bat = _batting(
        [
            {
                "season": season,
                "team_id": team_id,
                "player_id": player_id,
                "plate_appearances": 30,
                "games_played": 30,
            }
            for player_id in (
                "shared_a",
                "p03player",
                "p04player",
                "p05player",
                "p06player",
                "p07player",
                "p08player",
                "p09player",
            )
        ]
    )
    out = compute_modal_lineups(apps, bat)
    assert out.is_empty()


def test_lineup_position_unique_per_team_season() -> None:
    apps = pl.concat(
        [
            _all_positions_filled(1885, "CHN"),
            _all_positions_filled(1885, "BSN"),
            _all_positions_filled(1886, "CHN"),
        ]
    )
    bat_rows: list[dict[str, object]] = []
    for season, team_id in [(1885, "CHN"), (1885, "BSN"), (1886, "CHN")]:
        for pos in range(1, 10):
            bat_rows.append(
                {
                    "season": season,
                    "team_id": team_id,
                    "player_id": f"p{pos:02d}player",
                    "plate_appearances": 100 + pos,
                    "games_played": 50,
                }
            )
    bat = _batting(bat_rows)
    out = compute_modal_lineups(apps, bat)
    grain = out.group_by(["season", "team_id"]).agg(
        pl.col("lineup_position").n_unique().alias("n_unique"),
        pl.col("fielding_position").n_unique().alias("n_field"),
        pl.len().alias("n_rows"),
    )
    assert (grain["n_unique"] == 9).all()
    assert (grain["n_field"] == 9).all()
    assert (grain["n_rows"] == 9).all()


@pytest.mark.parametrize(
    "line_score, expected",
    [
        (
            "010200000",
            [(1, 0), (2, 1), (3, 0), (4, 2), (5, 0), (6, 0), (7, 0), (8, 0), (9, 0)],
        ),
        (
            "001(10)2000X",
            [(1, 0), (2, 0), (3, 1), (4, 10), (5, 2), (6, 0), (7, 0), (8, 0)],
        ),
        ("", []),
        (None, []),
    ],
)
def test_line_score_parser(
    line_score: str | None, expected: list[tuple[int, int]]
) -> None:
    """Tokenization mirrors box_score_line_scores.sql.

    The regex matches either a parenthesized multi-digit run total
    `\\((\\d+)\\)` or a single digit `\\d`. Anything else — including the
    'X' sentinel that means the winning home team didn't bat in the
    bottom of the last inning — is dropped, so that inning produces no
    output row.
    """
    sql = """
        WITH parsed AS (
            SELECT
                t.token,
                t.idx AS inning
            FROM UNNEST(
                regexp_extract_all($line, '\\((\\d+)\\)|\\d')
            ) WITH ORDINALITY AS t(token, idx)
        )
        SELECT
            inning::UTINYINT AS inning,
            TRIM(token, '()')::UTINYINT AS runs
        FROM parsed
        ORDER BY inning
    """
    rows = duckdb.connect().execute(sql, {"line": line_score}).fetchall()
    assert rows == expected
