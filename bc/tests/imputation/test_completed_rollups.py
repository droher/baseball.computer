from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from sqlmesh.core.dialect import parse

from python_models.imputation.pitches import PITCH_COUNTERS


MODELS = Path(__file__).parents[2] / "models" / "intermediate" / "coverage"


def test_completed_rollup_models_parse_as_sqlmesh_models() -> None:
    for name in (
        "pbp_completed_pitch_items.sql",
        "pbp_completed_pitch_totals.sql",
        "pbp_completed_fielding_totals.sql",
    ):
        expressions = parse((MODELS / name).read_text())
        assert len(expressions) == 2


def test_completed_consumer_grains_use_composite_unique_audits() -> None:
    expected = {
        "pbp_completed_events.sql": "unique_grain(columns := (event_key))",
        "pbp_completed_games.sql": "unique_grain(columns := (game_id))",
        "pbp_completed_pitch_items.sql": "unique_grain(columns := (event_key, sequence_index))",
        "pbp_completed_pitch_totals.sql": "unique_grain(columns := (game_id, pitcher_id, batter_id))",
        "pbp_completed_fielding_totals.sql": "unique_grain(columns := (game_id, player_id, completed_fielding_position, credit_type))",
    }
    for name, audit in expected.items():
        assert audit in (MODELS / name).read_text()


def test_completed_events_exposes_values_and_preserves_raw_values() -> None:
    source = (MODELS / "pbp_completed_events.sql").read_text()
    expressions = parse(source)
    assert len(expressions) == 2

    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA main_models")
        connection.execute(
            """
            CREATE TABLE main_models.stg_events AS
            SELECT * FROM (VALUES
                (1, 'G-ORDINARY', 1, 2024, 'Single', 0, 0, 'Fly', 8, 'Outfield', 'Deep', 'Center', 'Hard'),
                (2, 'G-EARLY', 1, 1903, 'Single', 0, 0, NULL, NULL, NULL, NULL, NULL, NULL)
            ) AS values_(
                event_key, game_id, event_id, season, plate_appearance_result,
                count_balls, count_strikes, batted_trajectory, batted_to_fielder,
                batted_location_general, batted_location_depth, batted_location_angle,
                batted_contact_strength
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE main_models.pbp_completed_pitches AS
            SELECT * FROM (VALUES
                (1, 'G-ORDINARY', 1, 2024, 'Single', 0, 0, 1, 1, 3, 'pitch-artifact', 'pbp_completed_pitches', '1', 'source', 'completion', 'estimated', 'exploratory', TRUE),
                (2, 'G-EARLY', 1, 1903, 'Single', 0, 0, 1, 1, 2, 'pitch-artifact', 'pbp_completed_pitches', '1', 'source', 'completion', 'estimated', 'exploratory', TRUE)
            ) AS values_(
                event_key, game_id, event_id, season, plate_appearance_result,
                count_balls, count_strikes, completed_count_balls,
                completed_count_strikes, completed_pitches, artifact_id, model_name,
                model_version, source_snapshot_id, method, observed_status,
                confidence_status, weak_identification_flag
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE main_models.pbp_completed_geometry AS
            SELECT * FROM (VALUES
                (1, 'Fly', 8, 'Outfield', 'Deep', 'Center', 'Hard', 'Center', 'Deep', 'Middle', 'fly', 0.0, 1.0, 0.0, 0.0, 'observed', 'observed', 'observed', 'observed', 'observed', 'observed', 'observed', 'geometry-artifact'),
                (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'geometry-artifact')
            ) AS values_(
                event_key, trajectory, handler_position, general_location,
                location_depth_modifier, location_angle, contact_strength,
                location_side, location_depth, location_edge, standardized_trajectory,
                p_standardized_ground_ball, p_standardized_fly,
                p_standardized_line_drive, p_standardized_pop_up,
                trajectory_method, general_location_method, location_angle_method,
                location_depth_modifier_method, contact_strength_method,
                handler_position_method, standardized_trajectory_method, artifact_id
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE main_models.pbp_completed_event_values AS
            SELECT * FROM (VALUES
                (1, 4.2, 3.7, 0.55, 0.62, 1.25, 1.25, 0.2, 0.07, 0.2, -0.07, 'existing_event_transition_value', 'existing_event_transition_value', 'value-artifact', 'pbp_completed_event_values', '1', 'source', 'completion', 'estimated', 'exploratory', TRUE),
                (2, 2.1, 1.6, 0.5, 0.45, 0.0, 0.5, 0.0, -0.05, 0.0, 0.05, 'derived_pooled_base_out_run_expectancy', 'derived_neutral_win_expectancy_prior', 'value-artifact', 'pbp_completed_event_values', '1', 'source', 'completion', 'estimated', 'exploratory', TRUE)
            ) AS values_(
                event_key, run_expectancy_start, run_expectancy_end,
                home_win_expectancy_start, home_win_expectancy_end,
                expected_runs_change_raw, expected_runs_change,
                expected_home_win_change_raw, expected_home_win_change,
                expected_batting_win_change_raw, expected_batting_win_change,
                expected_runs_change_method, expected_win_change_method,
                artifact_id, model_name, model_version, source_snapshot_id, method,
                observed_status, confidence_status, weak_identification_flag
            )
            """
        )
        rendered = expressions[1].sql(dialect="duckdb")
        relation = connection.sql(rendered)
        rows = (
            relation.project(
                "event_key, raw_expected_runs_change::DOUBLE, "
                "expected_runs_change::DOUBLE, raw_expected_home_win_change::DOUBLE, "
                "expected_home_win_change::DOUBLE, "
                "raw_expected_batting_win_change::DOUBLE, "
                "expected_batting_win_change::DOUBLE, run_expectancy_start::DOUBLE, "
                "run_expectancy_end::DOUBLE, home_win_expectancy_start::DOUBLE, "
                "home_win_expectancy_end::DOUBLE, "
                "expected_runs_change_method, expected_win_change_method, "
                "value_artifact_id, value_method"
            )
            .order("event_key")
            .fetchall()
        )

    assert [row[:11] for row in rows] == pytest.approx(
        [
            (1, 1.25, 1.25, 0.2, 0.07, 0.2, -0.07, 4.2, 3.7, 0.55, 0.62),
            (2, 0.0, 0.5, 0.0, -0.05, 0.0, 0.05, 2.1, 1.6, 0.5, 0.45),
        ]
    )
    assert [row[11:] for row in rows] == [
        (
            "existing_event_transition_value",
            "existing_event_transition_value",
            "value-artifact",
            "completion",
        ),
        (
            "derived_pooled_base_out_run_expectancy",
            "derived_neutral_win_expectancy_prior",
            "value-artifact",
            "completion",
        ),
    ]


def test_pitch_total_columns_partition_each_completed_counter() -> None:
    source = (MODELS / "pbp_completed_pitch_totals.sql").read_text()

    for counter in PITCH_COUNTERS:
        assert f"SUM(completed_{counter})" in source
        assert f"AS observed_{counter}" in source
        assert f"AS estimated_{counter}" in source

    assert "observed_incremental', 'observed_baserunning_event" in source
    assert "completed_plate_appearances" in source
    assert "interrupted_appearances" in source

    with duckdb.connect() as connection:
        rows = connection.execute(
            """
            WITH event_rows AS (
                SELECT * FROM (VALUES
                    ('G1', 'pitcher', 'batter', 1, 'Walk', 3, 'observed_incremental'),
                    ('G1', 'pitcher', 'batter', 2, NULL, 2, 'observed_incremental'),
                    ('G1', 'pitcher', 'batter', 2, NULL, 1, 'observed_incremental_with_declared_token_fallback')
                ) AS values_(game_id, pitcher_id, batter_id, appearance_id, result, pitches, pitches_method)
            )
            SELECT
                SUM(pitches) AS completed_pitches,
                SUM(CASE WHEN pitches_method IN ('observed_incremental', 'observed_baserunning_event')
                    THEN pitches ELSE 0 END) AS observed_pitches,
                SUM(CASE WHEN pitches_method NOT IN ('observed_incremental', 'observed_baserunning_event')
                    THEN pitches ELSE 0 END) AS estimated_pitches,
                COUNT(DISTINCT appearance_id) AS pitching_appearances,
                COUNT(DISTINCT appearance_id) FILTER (WHERE result IS NOT NULL)
                    AS completed_plate_appearances,
                COUNT(DISTINCT appearance_id) FILTER (WHERE result IS NULL)
                    AS interrupted_appearances
            FROM event_rows
            """
        ).fetchone()

    assert rows == (6, 5, 1, 2, 1, 1)
    assert rows is not None
    assert rows[0] == rows[1] + rows[2]


def test_pitch_totals_do_not_count_terminal_appearances_as_interrupted() -> None:
    source = (MODELS / "pbp_completed_pitch_totals.sql").read_text()
    assert "COUNT(DISTINCT appearance_start_event_id)\n            - COUNT(" in source

    with duckdb.connect() as connection:
        row = connection.execute(
            """
            SELECT
                COUNT(DISTINCT appearance_start_event_id) AS appearances,
                COUNT(DISTINCT appearance_start_event_id)
                    FILTER (WHERE plate_appearance_result IS NOT NULL)
                    AS completed_appearances,
                COUNT(DISTINCT appearance_start_event_id)
                    - COUNT(DISTINCT appearance_start_event_id)
                        FILTER (WHERE plate_appearance_result IS NOT NULL)
                    AS interrupted_appearances
            FROM (VALUES
                (10, NULL),
                (10, 'Walk')
            ) AS events(appearance_start_event_id, plate_appearance_result)
            """
        ).fetchone()

    assert row == (1, 1, 0)


def test_pitch_items_omit_structural_zero_and_keep_indexed_source_evidence() -> None:
    source = (MODELS / "pbp_completed_pitch_items.sql").read_text()
    assert "WHERE completed.completed_pitch_sequence <> ''" in source
    assert (
        "LEFT JOIN source_items AS source USING (event_key, item_ordinality)" in source
    )
    assert "source_runners_going_flag" in source
    assert "source_blocked_by_catcher_flag" in source
    assert "source_catcher_pickoff_attempt_at_base" in source
    assert "completed_runners_going_flag" in source
    assert "completed_blocked_by_catcher_flag" in source
    assert "completed_catcher_pickoff_attempt_at_base" in source
    assert "declared_token_fallback" in source

    with duckdb.connect() as connection:
        rows = connection.execute(
            """
            SELECT item, sequence_index
            FROM (VALUES ('', 'structural_zero'), ('Ball|CalledStrike', 'prior'))
                AS completed(completed_pitch_sequence, item_method)
            CROSS JOIN UNNEST(STRING_SPLIT(completed_pitch_sequence, '|'))
                WITH ORDINALITY AS sequence(item, sequence_index)
            WHERE completed_pitch_sequence <> ''
            ORDER BY sequence_index
            """
        ).fetchall()

    assert rows == [("Ball", 1), ("CalledStrike", 2)]

    with duckdb.connect() as connection:
        indexed_rows = connection.execute(
            """
            WITH source_items AS (
                SELECT sequence_id,
                    ROW_NUMBER() OVER (ORDER BY sequence_id) AS item_ordinality
                FROM (VALUES (0), (1)) AS source(sequence_id)
            ), completed_items AS (
                SELECT item_ordinality
                FROM UNNEST(['Ball', 'CalledStrike']) WITH ORDINALITY AS item(token, item_ordinality)
            )
            SELECT COALESCE(sequence_id, item_ordinality - 1) AS sequence_index
            FROM completed_items
            LEFT JOIN source_items USING (item_ordinality)
            ORDER BY item_ordinality
            """
        ).fetchall()

    assert indexed_rows == [(0,), (1,)]


def test_fielding_rollup_has_explicit_unresolved_and_disposition_counts() -> None:
    source = (MODELS / "pbp_completed_fielding_totals.sql").read_text()
    assert "player_id" in source
    assert "observed_play_credits" in source
    assert "estimated_play_credits" in source
    assert "source_preserved_play_credits" in source
    assert "constrained_or_unresolved_play_credits" in source

    with duckdb.connect() as connection:
        rows = connection.execute(
            """
            WITH plays AS (
                SELECT * FROM (VALUES
                    ('G1', NULL, 6, 'putout', 'Putout', 'estimated'),
                    ('G1', 'short01', 6, 'putout', 'Putout', 'observed'),
                    ('G1', 'short01', 6, 'assist', 'Assist', 'observed')
                ) AS values_(game_id, player_id, position, credit_type, fielding_play, completion_status)
            )
            SELECT player_id, position, credit_type, COUNT(*) AS credits,
                COUNT(*) FILTER (WHERE completion_status = 'observed') AS observed,
                COUNT(*) FILTER (WHERE completion_status <> 'observed') AS estimated
            FROM plays
            GROUP BY ALL
            ORDER BY player_id NULLS FIRST, credit_type
            """
        ).fetchall()

    assert rows[0] == (None, 6, "putout", 1, 0, 1)
    assert rows[1:] == [
        ("short01", 6, "assist", 1, 1, 0),
        ("short01", 6, "putout", 1, 1, 0),
    ]
