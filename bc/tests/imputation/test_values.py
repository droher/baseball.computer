from __future__ import annotations

from typing import cast

import duckdb
import pytest
from pydantic import ValidationError

from python_models.imputation.values import (
    EVENT_VALUES_OUTPUT_SCHEMA,
    LINEAR_WEIGHTS_OUTPUT_SCHEMA,
    OUTPUT_SCHEMAS,
    PARK_FACTORS_OUTPUT_SCHEMA,
    RUN_EXPECTANCY_OUTPUT_SCHEMA,
    STATE_TRANSITIONS_OUTPUT_SCHEMA,
    Config,
    PARK_METRICS,
    build_values_component_sql,
)


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA main_models")
    connection.execute("CREATE SCHEMA main_seeds")
    connection.execute(
        "CREATE TABLE main_models.game_start_info "
        "(game_id VARCHAR, season SMALLINT, source_type VARCHAR)"
    )
    connection.executemany(
        "INSERT INTO main_models.game_start_info VALUES (?, ?, ?)",
        (
            ("early", 1903, "PlayByPlay"),
            ("donor", 1910, "PlayByPlay"),
            ("box", 1903, "BoxScore"),
        ),
    )
    connection.execute(
        """
        CREATE TABLE main_models.event_states_full (
            event_key UINTEGER, game_id VARCHAR, season SMALLINT, league VARCHAR,
            league_group VARCHAR, park_id VARCHAR, run_expectancy_start_key VARCHAR,
            run_expectancy_end_key VARCHAR, win_expectancy_start_key VARCHAR,
            win_expectancy_end_key VARCHAR, outs_start TINYINT, base_state_start TINYINT,
            outs_end TINYINT, base_state_end TINYINT, game_end_flag BOOLEAN,
            truncated_home_margin_end TINYINT, runs_on_play TINYINT,
            batting_side VARCHAR, season_group SMALLINT
        )
        """
    )
    connection.executemany(
        "INSERT INTO main_models.event_states_full VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            (
                1,
                "early",
                1903,
                "AL",
                "AL",
                "PARK",
                "1914_AL_0_0",
                "1914_AL_1_0",
                "start",
                "end",
                0,
                0,
                1,
                0,
                False,
                0,
                0,
                "Away",
                1914,
            ),
            (
                2,
                "donor",
                1910,
                "AL",
                "AL",
                "PARK",
                "1914_AL_0_0",
                "1914_AL_1_0",
                "start",
                "end",
                0,
                0,
                1,
                0,
                False,
                0,
                1,
                "Home",
                1914,
            ),
            (
                3,
                "box",
                1903,
                "AL",
                "AL",
                "PARK",
                "1914_AL_0_0",
                "1914_AL_1_0",
                "start",
                "end",
                0,
                0,
                1,
                0,
                False,
                0,
                0,
                "Away",
                1914,
            ),
        ),
    )
    connection.execute(
        "CREATE TABLE main_models.event_transition_values "
        "(event_key UINTEGER, expected_runs_change DOUBLE, "
        "expected_home_win_change DOUBLE, expected_batting_win_change DOUBLE)"
    )
    connection.execute(
        "INSERT INTO main_models.event_transition_values VALUES (2, 1.25, 0.05, 0.05)"
    )
    connection.execute(
        "CREATE TABLE main_models.run_expectancy_matrix "
        "(run_expectancy_key VARCHAR, league_group VARCHAR, season_group SMALLINT, "
        "outs TINYINT, base_state TINYINT, avg_runs_scored DOUBLE, sample_size BIGINT)"
    )
    connection.executemany(
        "INSERT INTO main_models.run_expectancy_matrix VALUES (?, 'AL', 1914, ?, 0, ?, 100)",
        (("1914_AL_0_0", 0, 0.5), ("1914_AL_1_0", 1, 0.2)),
    )
    connection.execute(
        "CREATE TABLE main_models.win_expectancy_matrix "
        "(win_expectancy_key VARCHAR, home_win_rate DOUBLE)"
    )
    connection.executemany(
        "INSERT INTO main_models.win_expectancy_matrix VALUES (?, ?)",
        (("start", 0.5), ("end", 0.4)),
    )
    connection.execute(
        "CREATE TABLE main_models.run_expectancy_summary "
        "(season SMALLINT, league VARCHAR, base_state TINYINT, outs TINYINT, "
        "outcome VARCHAR, re_value_mean DOUBLE, re_value_sd DOUBLE)"
    )
    connection.execute(
        "INSERT INTO main_models.run_expectancy_summary "
        "VALUES (1910, 'AL', 0, 0, 'runs_to_end', 0.55, 0.03)"
    )
    connection.execute(
        "CREATE TABLE main_models.state_transition_summary "
        "(start_state VARCHAR, season SMALLINT, league VARCHAR, end_class VARCHAR, "
        "outcome VARCHAR, prob_mean DOUBLE, prob_sd DOUBLE)"
    )
    connection.executemany(
        "INSERT INTO main_models.state_transition_summary VALUES "
        "('0_0', 1910, 'AL', ?, 'end_state', ?, 0.01)",
        (("0_0", 0.3), ("1_0", 0.7)),
    )
    park_columns = ", ".join(f"{metric} DOUBLE" for metric in PARK_METRICS)
    connection.execute(
        "CREATE TABLE main_models.park_factors "
        f"(season SMALLINT, park_id VARCHAR, league VARCHAR, {park_columns})"
    )
    early_values = [0.9, None, float("nan"), *([None] * (len(PARK_METRICS) - 4)), 0.9]
    donor_values = [1.1] * len(PARK_METRICS)
    placeholders = ", ".join("?" for _ in range(len(PARK_METRICS) + 3))
    connection.executemany(
        f"INSERT INTO main_models.park_factors VALUES ({placeholders})",
        ((1903, "PARK", "AL", *early_values), (1910, "PARK", "AL", *donor_values)),
    )
    connection.execute(
        "CREATE TABLE main_models.linear_weights "
        "(season SMALLINT, league VARCHAR, play VARCHAR, play_category VARCHAR, "
        "average_run_value DOUBLE, std_dev_run_value DOUBLE, is_imputed BOOLEAN)"
    )
    connection.executemany(
        "INSERT INTO main_models.linear_weights VALUES (?, 'AL', 'Single', "
        "'BATTING', ?, 0.2, ?)",
        ((1903, 0.45, True), (1910, 0.5, False)),
    )
    connection.execute(
        "CREATE TABLE main_models.linear_weights_estimated "
        "(season SMALLINT, league VARCHAR, play VARCHAR, run_value_mean DOUBLE, "
        "run_value_sd DOUBLE, is_imputed BOOLEAN)"
    )
    connection.execute(
        "INSERT INTO main_models.linear_weights_estimated "
        "VALUES (1910, 'AL', 'Single', 0.51, 0.02, FALSE)"
    )
    connection.execute("CREATE TABLE main_models.stg_events (event_key UINTEGER)")
    connection.execute(
        "CREATE TABLE main_models.stg_event_baserunners (event_key UINTEGER)"
    )
    connection.execute(
        "CREATE TABLE main_seeds.seed_plate_appearance_result_types (x INTEGER)"
    )
    connection.execute(
        "CREATE TABLE main_seeds.seed_baserunning_play_types (x INTEGER)"
    )
    return connection


def _rows(
    connection: duckdb.DuckDBPyConnection, component: str
) -> duckdb.DuckDBPyRelation:
    return connection.sql(build_values_component_sql(component, Config()))


def test_event_values_cover_only_actual_pbp_and_preserve_existing_values() -> None:
    with _connection() as connection:
        result = _rows(connection, "event_values").order("event_key")
        assert tuple(column[0] for column in result.description) == tuple(
            EVENT_VALUES_OUTPUT_SCHEMA
        )
        rows = result.fetchall()

    assert [row[0] for row in rows] == [1, 2]
    early = rows[0]
    donor = rows[1]
    assert early[9] is None
    assert early[10] == pytest.approx(early[6] - early[5])
    assert early[12] is None
    assert early[13] == pytest.approx(early[8] - early[7])
    assert early[15] == pytest.approx(early[7] - early[8])
    assert donor[10] == 1.25
    assert donor[11] == "existing_event_transition_value"


def test_transport_preserves_whole_transition_vectors_and_normalization() -> None:
    with _connection() as connection:
        rows = _rows(connection, "state_transitions").fetchall()

    assert len(rows) == len({tuple(row[:4]) for row in rows})
    grouped: dict[tuple[int, str, str], float] = {}
    for row in rows:
        key = cast(tuple[int, str, str], tuple(row[:3]))
        grouped[key] = grouped.get(key, 0.0) + row[5]
    assert grouped.keys() == {(1903, "AL", "0_0"), (1910, "AL", "0_0")}
    assert all(total == pytest.approx(1.0) for total in grouped.values())
    early = [row for row in rows if row[0] == 1903]
    assert {row[3]: row[5] for row in early} == pytest.approx({"0_0": 0.3, "1_0": 0.7})
    assert all(row[4] is None for row in early)
    assert all(
        row[7] == "conditional_transport_nearest_season_same_league_state"
        for row in early
    )
    current = [row for row in rows if row[0] == 1910]
    assert all(row[4] == row[5] for row in current)


def test_compact_value_surfaces_preserve_source_and_fill_early_contexts() -> None:
    with _connection() as connection:
        parks = _rows(connection, "park_factors")
        expectancy = _rows(connection, "run_expectancy")
        weights = _rows(connection, "linear_weights")
        assert tuple(column[0] for column in parks.description) == tuple(
            PARK_FACTORS_OUTPUT_SCHEMA
        )
        assert tuple(column[0] for column in expectancy.description) == tuple(
            RUN_EXPECTANCY_OUTPUT_SCHEMA
        )
        assert tuple(column[0] for column in weights.description) == tuple(
            LINEAR_WEIGHTS_OUTPUT_SCHEMA
        )
        park_rows = parks.fetchall()
        expectancy_rows = expectancy.fetchall()
        weight_rows = weights.fetchall()

    assert not any(row[5] is None for row in park_rows)
    early_basic = next(
        row for row in park_rows if row[:4] == (1903, "PARK", "AL", "basic_park_factor")
    )
    early_single = next(
        row
        for row in park_rows
        if row[:4] == (1903, "PARK", "AL", "singles_park_factor")
    )
    assert early_basic[4:7] == (0.9, 0.9, "existing_park_factor")
    assert early_single[4] is None
    assert early_single[5] == 1.1
    assert early_single[6] == "nearest_season_same_park_league"
    early_double = next(
        row
        for row in park_rows
        if row[:4] == (1903, "PARK", "AL", "doubles_park_factor")
    )
    assert early_double[4] != early_double[4]
    assert early_double[5] == 1.1
    assert early_double[6] == "nearest_season_same_park_league"
    assert not any(row[5] is None for row in expectancy_rows)
    early_weight = next(row for row in weight_rows if row[0] == 1903)
    donor_weight = next(row for row in weight_rows if row[0] == 1910)
    assert early_weight[4] is None
    assert early_weight[5] == 0.45
    assert early_weight[7] == "existing_deterministic_linear_weight"
    assert donor_weight[4:6] == (0.51, 0.51)


def test_component_contract_and_config_validation() -> None:
    assert set(OUTPUT_SCHEMAS) == {
        "event_values",
        "park_factors",
        "run_expectancy",
        "state_transitions",
        "linear_weights",
    }
    with pytest.raises(ValueError, match="unknown values component"):
        build_values_component_sql("missing")
    with pytest.raises(ValidationError):
        Config(start_season=2025, end_season=1903)
    with pytest.raises(ValidationError):
        Config(states_relation="main_models.states;drop")


def test_state_transition_schema_is_stable() -> None:
    with _connection() as connection:
        relation = _rows(connection, "state_transitions")
    assert tuple(column[0] for column in relation.description) == tuple(
        STATE_TRANSITIONS_OUTPUT_SCHEMA
    )
