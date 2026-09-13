from __future__ import annotations

from typing import cast

import duckdb
import pytest
from pydantic import ValidationError

from python_models.imputation.runners import (
    OUTPUT_SCHEMA,
    Config,
    build_runner_completion_sql,
)


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA main_models")
    connection.execute(
        "CREATE TABLE main_models.game_start_info "
        "(game_id VARCHAR, season SMALLINT, source_type VARCHAR)"
    )
    connection.execute(
        """
        CREATE TABLE main_models.stg_events (
            event_key UINTEGER,
            game_id VARCHAR,
            event_id UTINYINT,
            pitcher_id VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE main_models.stg_event_baserunners (
            event_key UINTEGER,
            game_id VARCHAR,
            event_id UTINYINT,
            baserunner VARCHAR,
            runner_id VARCHAR,
            runner_lineup_position UTINYINT,
            attempted_advance_to_base VARCHAR,
            baserunning_play_type VARCHAR,
            is_out BOOLEAN,
            run_scored_flag BOOLEAN,
            base_end VARCHAR,
            explicit_charged_pitcher_id VARCHAR,
            charge_event_key UINTEGER
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE main_models.event_base_out_states (
            event_key UINTEGER,
            frame_end_flag BOOLEAN,
            truncated_frame_flag BOOLEAN,
            game_end_flag BOOLEAN,
            outs_on_play UTINYINT,
            runner_first_id_end VARCHAR,
            runner_second_id_end VARCHAR,
            runner_third_id_end VARCHAR
        )
        """
    )
    connection.executemany(
        "INSERT INTO main_models.game_start_info VALUES (?, ?, ?)",
        (
            ("PBP190301010", 1903, "PlayByPlay"),
            ("BOX190301010", 1903, "BoxScore"),
        ),
    )
    connection.executemany(
        "INSERT INTO main_models.stg_events VALUES (?, ?, ?, ?)",
        (
            (1, "PBP190301010", 1, "pitcher1"),
            (2, "PBP190301010", 2, "pitcher2"),
            (3, "PBP190301010", 3, "pitcher3"),
            (4, "PBP190301010", 4, "pitcher4"),
            (5, "PBP190301010", 5, "pitcher5"),
            (6, "BOX190301010", 1, "boxpitch"),
        ),
    )
    connection.executemany(
        "INSERT INTO main_models.stg_event_baserunners VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            (
                1,
                "PBP190301010",
                1,
                "First",
                "runner1",
                1,
                None,
                None,
                False,
                False,
                "Second",
                None,
                1,
            ),
            (
                2,
                "PBP190301010",
                2,
                "First",
                "runner2",
                2,
                None,
                "PickedOff",
                False,
                False,
                None,
                None,
                1,
            ),
            (
                3,
                "PBP190301010",
                3,
                "First",
                "runner3",
                3,
                None,
                "PickedOff",
                False,
                False,
                None,
                None,
                3,
            ),
            (
                4,
                "PBP190301010",
                4,
                "Second",
                "runner4",
                4,
                "Third",
                "CaughtStealing",
                True,
                False,
                None,
                None,
                4,
            ),
            (
                5,
                "PBP190301010",
                5,
                "Third",
                "runner5",
                5,
                "Home",
                None,
                False,
                True,
                "Home",
                "override",
                1,
            ),
            (
                6,
                "BOX190301010",
                1,
                "First",
                "boxrunner",
                1,
                None,
                None,
                False,
                False,
                "First",
                None,
                6,
            ),
        ),
    )
    connection.executemany(
        "INSERT INTO main_models.event_base_out_states VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            (1, False, False, False, 0, None, "runner1", None),
            (2, False, False, False, 0, None, "runner2", None),
            (3, True, False, False, 1, None, None, None),
            (4, False, False, False, 1, None, None, None),
            (5, False, False, False, 0, None, None, None),
            (6, False, False, False, 0, "boxrunner", None, None),
        ),
    )
    return connection


def _relation(
    connection: duckdb.DuckDBPyConnection, config: Config | None = None
) -> duckdb.DuckDBPyRelation:
    return connection.sql(build_runner_completion_sql(config or Config()))


def _row(relation: duckdb.DuckDBPyRelation) -> dict[str, object]:
    values = relation.fetchone()
    if values is None:
        raise AssertionError("query returned no row")
    columns = [str(column[0]) for column in relation.description]
    return {column: value for column, value in zip(columns, values, strict=True)}


def test_output_schema_and_pbp_scope() -> None:
    relation = _relation(_connection())

    assert tuple(column[0] for column in relation.description) == tuple(OUTPUT_SCHEMA)
    total = relation.count("*").fetchone()
    box = relation.filter("game_id = 'BOX190301010'").count("*").fetchone()
    assert total is not None and total[0] == 5
    assert box is not None and box[0] == 0


def test_observed_destination_and_explicit_pitcher_are_preserved() -> None:
    observed = _row(_relation(_connection()).filter("event_key = 1"))
    scored = _row(_relation(_connection()).filter("event_key = 5"))

    assert observed["raw_base_end"] == "Second"
    assert observed["completed_destination"] == "Second"
    assert observed["p_second"] == 1.0
    assert observed["completed_charged_pitcher_id"] == "pitcher1"
    assert observed["charged_pitcher_method"] == "derived_charge_event_pitcher"
    assert scored["completed_destination"] == "Home"
    assert scored["completed_base_end"] == scored["raw_base_end"]
    assert scored["p_home"] == 1.0
    assert scored["completed_charged_pitcher_id"] == "override"
    assert scored["charged_pitcher_method"] == "observed_explicit_override"


def test_missing_base_end_uses_next_event_runner_identity() -> None:
    row = _row(_relation(_connection()).filter("event_key = 2"))

    assert row["raw_base_end"] is None
    assert row["completed_base_end"] == "Second"
    assert row["completed_destination"] == "Second"
    assert row["destination_method"] == "derived_next_event_runner_identity"
    assert row["destination_status"] == "derived_consistent"
    assert row["p_second"] == 1.0


def test_frame_end_disappearance_remains_an_explicit_conflict() -> None:
    row = _row(_relation(_connection()).filter("event_key = 3"))
    probability_sum = sum(
        cast(float, row[column])
        for column in ("p_first", "p_second", "p_third", "p_home", "p_out")
    )

    assert row["completed_base_end"] == "First"
    assert row["destination_method"] == "constrained_conflict_fallback"
    assert row["destination_status"] == "runner_absent_at_frame_end_conflict"
    assert (
        row["constraint_disposition"]
        == "probability_support_retains_source_not_out_and_frame_end_out_conflict"
    )
    assert row["destination_support"] == "First|Out"
    assert probability_sum == pytest.approx(1.0)


def test_recorded_out_has_one_hot_out_destination() -> None:
    row = _row(_relation(_connection()).filter("event_key = 4"))

    assert row["is_out"] is True
    assert row["completed_base_end"] is None
    assert row["completed_destination"] == "Out"
    assert row["p_out"] == 1.0


def test_recorded_base_marker_is_preserved_even_for_an_out() -> None:
    with _connection() as connection:
        connection.execute(
            "UPDATE main_models.stg_event_baserunners SET base_end = 'First' WHERE event_key = 4"
        )
        row = _row(_relation(connection).filter("event_key = 4"))
        assert row["completed_base_end"] == "First"
        assert row["completed_destination"] == "Out"


def test_sampling_and_validation() -> None:
    connection = _connection()
    config = Config(sample_games=1, sample_seed="same")
    first = _relation(connection, config).project("game_id").distinct().fetchall()
    second = _relation(connection, config).project("game_id").distinct().fetchall()

    assert first == second
    with pytest.raises(ValidationError):
        _ = Config(runners_relation="main_models.runners;drop")
    with pytest.raises(ValidationError):
        _ = Config(start_season=2025, end_season=1903)
