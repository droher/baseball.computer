from __future__ import annotations

from collections.abc import Generator
from datetime import date
from pathlib import Path
from typing import cast

import duckdb
import pytest
import sqlglot


MODEL_PATH = Path(__file__).parents[1] / "models/metrics/standings.sql"
StandingsValue = int | float | str | date
COUNT_COLUMNS = (
    "wins",
    "losses",
    "runs_scored",
    "runs_allowed",
    "home_wins",
    "home_losses",
    "away_wins",
    "away_losses",
    "interleague_wins",
    "interleague_losses",
    "east_wins",
    "east_losses",
    "central_wins",
    "central_losses",
    "west_wins",
    "west_losses",
    "one_run_wins",
    "one_run_losses",
    "last_10_wins",
    "last_10_losses",
)


def _model_sql() -> str:
    source = MODEL_PATH.read_text()
    query = source.split(");\n", 1)[1]
    return sqlglot.parse_one(query, read="duckdb").sql(dialect="duckdb")


def _connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    _ = con.execute("CREATE SCHEMA main_models")
    _ = con.execute("""
        CREATE TABLE main_models.game_start_info (
            date DATE
        )
    """)
    _ = con.execute("""
        CREATE TABLE main_models.game_results (
            season INTEGER,
            game_type VARCHAR,
            game_finish_date DATE
        )
    """)
    _ = con.execute("""
        CREATE TABLE main_models.team_game_start_info (
            season INTEGER,
            team_id VARCHAR,
            league VARCHAR,
            team_name VARCHAR,
            division VARCHAR,
            game_type VARCHAR
        )
    """)
    _ = con.execute("""
        CREATE TABLE main_models.team_game_results (
            season INTEGER,
            game_id VARCHAR,
            game_finish_date DATE,
            team_id VARCHAR,
            game_type VARCHAR,
            season_game_number INTEGER,
            wins INTEGER,
            losses INTEGER,
            win_streak_length INTEGER,
            loss_streak_length INTEGER,
            runs_scored INTEGER,
            runs_allowed INTEGER,
            home_wins INTEGER,
            home_losses INTEGER,
            away_wins INTEGER,
            away_losses INTEGER,
            interleague_wins INTEGER,
            interleague_losses INTEGER,
            east_wins INTEGER,
            east_losses INTEGER,
            central_wins INTEGER,
            central_losses INTEGER,
            west_wins INTEGER,
            west_losses INTEGER,
            one_run_wins INTEGER,
            one_run_losses INTEGER
        )
    """)
    return con


@pytest.fixture
def con() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    connection = _connection()
    try:
        yield connection
    finally:
        connection.close()


def _insert_game(
    con: duckdb.DuckDBPyConnection,
    *,
    season: int,
    team_id: str,
    game_id: str,
    finished: date,
    game_number: int,
    outcome: str,
    win_streak: int,
    loss_streak: int,
    game_type: str = "RegularSeason",
) -> None:
    wins = int(outcome == "W")
    losses = int(outcome == "L")
    home = game_number % 2 == 1
    one_run = game_number % 3 == 0
    interleague = game_number % 4 == 0
    division = game_number % 3
    split_wins = wins if home else 0, wins if not home else 0
    split_losses = losses if home else 0, losses if not home else 0
    division_wins = tuple(
        wins if not interleague and division == index else 0 for index in range(3)
    )
    division_losses = tuple(
        losses if not interleague and division == index else 0 for index in range(3)
    )
    runs_scored = 5 if wins else 3 if losses else 4
    runs_allowed = 3 if wins else 5 if losses else 4
    _ = con.execute("INSERT INTO main_models.game_start_info VALUES (?)", [finished])
    _ = con.execute(
        "INSERT INTO main_models.game_results VALUES (?, ?, ?)",
        [season, game_type, finished],
    )
    _ = con.execute(
        "INSERT INTO main_models.team_game_start_info VALUES (?, ?, 'AL', ?, 'E', ?)",
        [season, team_id, team_id, game_type],
    )
    _ = con.execute(
        """
        INSERT INTO main_models.team_game_results VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            season,
            game_id,
            finished,
            team_id,
            game_type,
            game_number,
            wins,
            losses,
            win_streak,
            loss_streak,
            runs_scored,
            runs_allowed,
            split_wins[0],
            split_losses[0],
            split_wins[1],
            split_losses[1],
            wins if interleague else 0,
            losses if interleague else 0,
            division_wins[0],
            division_losses[0],
            division_wins[1],
            division_losses[1],
            division_wins[2],
            division_losses[2],
            wins if one_run else 0,
            losses if one_run else 0,
        ],
    )


def _rows(
    con: duckdb.DuckDBPyConnection,
) -> dict[tuple[int, date, str], dict[str, StandingsValue]]:
    cursor = con.execute(_model_sql())
    names = [item[0] for item in cursor.description]
    records = cursor.fetchall()
    rows = {
        (cast(int, row[0]), cast(date, row[1]), cast(str, row[4])): cast(
            dict[str, StandingsValue], dict(zip(names, row, strict=True))
        )
        for row in records
    }
    assert len(rows) == len(records)
    return rows


def _int(row: dict[str, StandingsValue], column: str) -> int:
    return cast(int, row[column])


def _assert_state_carried(
    game_day: dict[str, StandingsValue], offday: dict[str, StandingsValue]
) -> None:
    for column in COUNT_COLUMNS:
        assert _int(offday, column) == _int(game_day, column)
    assert _int(offday, "win_streak_length") == _int(game_day, "win_streak_length")
    assert _int(offday, "loss_streak_length") == _int(game_day, "loss_streak_length")


def test_standings_use_completed_games_and_carry_end_of_day_state(
    con: duckdb.DuckDBPyConnection,
) -> None:
    for number in range(1, 10):
        _insert_game(
            con,
            season=2025,
            team_id="NYA",
            game_id=f"g{number}",
            finished=date(2025, 4, number),
            game_number=number,
            outcome="W",
            win_streak=number,
            loss_streak=0,
        )
    _insert_game(
        con,
        season=2025,
        team_id="NYA",
        game_id="suspended",
        finished=date(2025, 4, 10),
        game_number=2,
        outcome="L",
        win_streak=0,
        loss_streak=1,
    )
    _insert_game(
        con,
        season=2025,
        team_id="NYA",
        game_id="g10",
        finished=date(2025, 4, 11),
        game_number=10,
        outcome="L",
        win_streak=0,
        loss_streak=2,
    )
    for number, outcome, win_streak, loss_streak in (
        (11, "W", 1, 0),
        (12, "W", 2, 0),
        (13, "L", 0, 1),
    ):
        _insert_game(
            con,
            season=2025,
            team_id="NYA",
            game_id=f"triple{number}",
            finished=date(2025, 4, 12),
            game_number=number,
            outcome=outcome,
            win_streak=win_streak,
            loss_streak=loss_streak,
        )
    for number, outcome, win_streak, loss_streak in (
        (14, "L", 0, 2),
        (15, "W", 1, 0),
        (16, "T", 0, 0),
    ):
        _insert_game(
            con,
            season=2025,
            team_id="NYA",
            game_id=f"late{number}",
            finished=date(2025, 4, 13 if number < 16 else 14),
            game_number=number,
            outcome=outcome,
            win_streak=win_streak,
            loss_streak=loss_streak,
        )
    _insert_game(
        con,
        season=2025,
        team_id="NYA",
        game_id="postseason",
        finished=date(2025, 4, 15),
        game_number=17,
        outcome="W",
        win_streak=1,
        loss_streak=0,
        game_type="Postseason",
    )
    _insert_game(
        con,
        season=2025,
        team_id="CLE",
        game_id="cle1",
        finished=date(2025, 4, 4),
        game_number=1,
        outcome="W",
        win_streak=1,
        loss_streak=0,
    )
    _insert_game(
        con,
        season=2024,
        team_id="NYA",
        game_id="old",
        finished=date(2024, 4, 1),
        game_number=1,
        outcome="L",
        win_streak=0,
        loss_streak=1,
    )

    rows = _rows(con)
    before_first = rows[2025, date(2025, 3, 31), "NYA"]
    tripleheader = rows[2025, date(2025, 4, 12), "NYA"]
    doubleheader = rows[2025, date(2025, 4, 13), "NYA"]
    final_day = rows[2025, date(2025, 4, 14), "NYA"]
    offday = rows[2025, date(2025, 4, 15), "NYA"]
    cle_game_day = rows[2025, date(2025, 4, 4), "CLE"]
    cle_offday = rows[2025, date(2025, 4, 5), "CLE"]
    prior_loss_day = rows[2024, date(2024, 4, 1), "NYA"]
    prior_loss_offday = rows[2024, date(2024, 4, 2), "NYA"]

    assert before_first["wins"] == before_first["losses"] == 0
    assert tripleheader["wins"] == 11
    assert tripleheader["losses"] == 3
    assert tripleheader["win_streak_length"] == 0
    assert tripleheader["loss_streak_length"] == 1
    assert tripleheader["last_10_wins"] == 7
    assert tripleheader["last_10_losses"] == 3
    assert doubleheader["wins"] == 12
    assert doubleheader["losses"] == 4
    assert doubleheader["win_streak_length"] == 1
    assert doubleheader["loss_streak_length"] == 0
    assert final_day["wins"] == 12
    assert final_day["losses"] == 4
    assert final_day["last_10_wins"] == 5
    assert final_day["last_10_losses"] == 4
    assert _int(final_day, "home_wins") + _int(final_day, "away_wins") == _int(
        final_day, "wins"
    )
    assert _int(final_day, "home_losses") + _int(final_day, "away_losses") == _int(
        final_day, "losses"
    )
    assert _int(final_day, "east_wins") + _int(final_day, "central_wins") + _int(
        final_day, "west_wins"
    ) == _int(final_day, "wins") - _int(final_day, "interleague_wins")
    assert _int(final_day, "east_losses") + _int(final_day, "central_losses") + _int(
        final_day, "west_losses"
    ) == _int(final_day, "losses") - _int(final_day, "interleague_losses")
    _assert_state_carried(final_day, offday)
    assert _int(cle_game_day, "win_streak_length") == 1
    assert _int(cle_game_day, "last_10_wins") == 1
    assert _int(cle_game_day, "last_10_losses") == 0
    _assert_state_carried(cle_game_day, cle_offday)
    assert _int(prior_loss_day, "loss_streak_length") == 1
    assert _int(prior_loss_day, "last_10_wins") == 0
    assert _int(prior_loss_day, "last_10_losses") == 1
    _assert_state_carried(prior_loss_day, prior_loss_offday)
    assert rows[2025, date(2025, 4, 14), "CLE"]["wins"] == 1
    assert rows[2024, date(2024, 4, 2), "NYA"]["losses"] == 1
