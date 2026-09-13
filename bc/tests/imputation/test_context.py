from __future__ import annotations

import duckdb
import pytest

from python_models.imputation.context import (
    ContextCompletionConfig,
    build_context_completion_sql,
)


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA main_models")
    connection.execute(
        "CREATE TABLE main_models.game_results (game_id VARCHAR, duration_minutes USMALLINT)"
    )
    connection.execute("INSERT INTO main_models.game_results VALUES ('donor', 150)")
    connection.execute("""CREATE TABLE main_models.game_start_info (
        game_id VARCHAR, season INTEGER, date DATE, source_type VARCHAR,
        park_id VARCHAR, away_league VARCHAR, home_league VARCHAR,
        time_of_day VARCHAR, sky VARCHAR, field_condition VARCHAR, precipitation VARCHAR,
        wind_direction VARCHAR, temperature_fahrenheit DOUBLE, attendance DOUBLE,
        wind_speed_mph DOUBLE, start_time TIMESTAMP
    )""")
    connection.execute("""INSERT INTO main_models.game_start_info VALUES
        ('early', 1903, '1903-07-01', 'PlayByPlay', 'park', 'NL', 'NL',
        'Day', 'Unknown', NULL, 'None', NULL, NULL, NULL, NULL, NULL),
        ('donor', 2003, '2003-07-10', 'PlayByPlay', 'park', 'NL', 'NL',
        'Day', 'Sunny', 'Dry', 'None', 'ToCf', 75, 0, 0, '2003-07-10 13:05:00'),
        ('excluded', 1903, '1903-07-01', 'BoxScore', 'park', 'NL', 'NL',
        'Day', 'Dome', 'Wet', 'Rain', 'ToLf', 90, 10000, 50, '1903-07-01 15:00:00')
    """)
    return connection


def test_complete_early_pbp_preserves_known_values_and_real_zero() -> None:
    with _connection() as connection:
        query = build_context_completion_sql(ContextCompletionConfig())
        result = connection.execute(
            f"SELECT game_id, sky, precipitation, attendance, wind_speed_mph, start_time::VARCHAR, sky_method, attendance_method FROM ({query}) ORDER BY game_id"
        ).fetchall()
    assert [row[0] for row in result] == ["donor", "early"]
    assert result[0][3:5] == (0.0, 0.0)
    assert result[0][-1] == "observed"
    assert result[1][1:5] == ("Sunny", "None", 0.0, 0.0)
    assert result[1][5] == "1903-07-01 13:05:00"
    assert result[1][6].startswith("empirical_")
    with _connection() as connection:
        durations = connection.execute(
            f"SELECT duration_minutes FROM ({build_context_completion_sql(ContextCompletionConfig())})"
        ).fetchall()
    assert all(row == (150.0,) for row in durations)


def test_sample_and_target_year_do_not_change_prior_population() -> None:
    with _connection() as connection:
        config = ContextCompletionConfig(
            start_season=1903,
            end_season=1903,
            sample_games=1,
            sample_seed="literal'quote",
        )
        query = build_context_completion_sql(config)
        first = connection.execute(query).fetchall()
        second = connection.execute(query).fetchall()
    assert first == second
    assert len(first) == 1
    assert first[0][0] == "early"


def test_absent_donor_is_explicit_and_not_invented() -> None:
    with _connection() as connection:
        connection.execute(
            "UPDATE main_models.game_start_info SET temperature_fahrenheit = NULL"
        )
        query = build_context_completion_sql(ContextCompletionConfig())
        rows = connection.execute(
            f"SELECT temperature_fahrenheit, temperature_fahrenheit_method, temperature_fahrenheit_donor_count FROM ({query})"
        ).fetchall()
    assert all(row == (None, "unsupported_no_donors", 0) for row in rows)


def test_invalid_season_span_is_rejected() -> None:
    with pytest.raises(ValueError, match="end_season"):
        ContextCompletionConfig(start_season=2025, end_season=1903)
