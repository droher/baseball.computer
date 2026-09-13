from __future__ import annotations

import duckdb
import pytest
from pydantic import ValidationError
from typing import cast

from python_models.imputation.pitches import (
    OUTPUT_SCHEMA,
    PITCH_COUNTERS,
    Config,
    build_pitch_completion_sql,
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
            season SMALLINT,
            plate_appearance_result VARCHAR,
            count_balls UTINYINT,
            count_strikes UTINYINT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE main_models.stg_event_pitch_sequence_status (
            event_key UINTEGER,
            appearance_start_event_id UTINYINT,
            pitch_sequence_resolution_status VARCHAR,
            raw_pitch_sequence VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE main_models.stg_event_pitch_sequences (
            event_key UINTEGER,
            game_id VARCHAR,
            sequence_id UTINYINT,
            sequence_item VARCHAR
        )
        """
    )
    counter_columns = ", ".join(f"{counter} UTINYINT" for counter in PITCH_COUNTERS)
    connection.execute(
        "CREATE TABLE main_models.event_pitch_sequence_stats "
        f"(event_key UINTEGER, {counter_columns})"
    )
    connection.executemany(
        "INSERT INTO main_models.game_start_info VALUES (?, ?, ?)",
        (
            ("PBP190301010", 1903, "PlayByPlay"),
            ("BOX190301010", 1903, "BoxScore"),
            ("PBP202001010", 2020, "PlayByPlay"),
        ),
    )
    connection.executemany(
        "INSERT INTO main_models.stg_events VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            (1, "PBP190301010", 1, 1903, None, 0, 0),
            (2, "PBP190301010", 2, 1903, None, 0, 0),
            (3, "PBP190301010", 3, 1903, "StrikeOut", 1, 1),
            (4, "PBP190301010", 4, 1903, "InPlayOut", 1, 1),
            (5, "PBP190301010", 5, 1903, "Walk", 2, 2),
            (8, "PBP190301010", 6, 1903, "StrikeOut", 0, 1),
            (9, "PBP190301010", 7, 1903, "Walk", 2, 2),
            (6, "BOX190301010", 1, 1903, "InPlayOut", 0, 0),
            (7, "PBP202001010", 1, 2020, "IntentionalWalk", 0, 0),
            (10, "PBP202001010", 2, 2020, "InPlayOut", None, None),
            (11, "PBP202001010", 3, 2020, "HitByPitch", None, None),
            (12, "PBP202001010", 4, 2020, "InPlayOut", 0, 2),
        ),
    )
    connection.executemany(
        "INSERT INTO main_models.stg_event_pitch_sequence_status VALUES (?, ?, ?, ?)",
        (
            (1, 1, "Resolved", ""),
            (2, 2, "Unavailable", ""),
            (3, 2, "Unavailable", ""),
            (4, 4, "Resolved", "BCX"),
            (5, 5, "Unresolved", "BB.W"),
            (8, 6, "Resolved", "C"),
            (9, 7, "Resolved", "BBFCFB"),
            (6, 1, "Resolved", "X"),
            (7, 1, "Unavailable", ""),
            (10, 2, "Unavailable", ""),
            (11, 3, "Unavailable", ""),
            (12, 4, "Resolved", "CCCX"),
        ),
    )
    connection.executemany(
        "INSERT INTO main_models.stg_event_pitch_sequences VALUES (?, ?, ?, ?)",
        (
            (4, "PBP190301010", 1, "Ball"),
            (4, "PBP190301010", 2, "StrikeUnknownType"),
            (4, "PBP190301010", 3, "InPlay"),
            (8, "PBP190301010", 1, "CalledStrike"),
            (9, "PBP190301010", 1, "Ball"),
            (9, "PBP190301010", 2, "Ball"),
            (9, "PBP190301010", 3, "CalledStrike"),
            (9, "PBP190301010", 4, "CalledStrike"),
            (9, "PBP190301010", 5, "Foul"),
            (9, "PBP190301010", 6, "Ball"),
            (9, "PBP190301010", 7, "Ball"),
            (12, "PBP202001010", 1, "CalledStrike"),
            (12, "PBP202001010", 2, "CalledStrike"),
            (12, "PBP202001010", 3, "CalledStrike"),
            (12, "PBP202001010", 4, "InPlay"),
            (6, "BOX190301010", 1, "InPlay"),
        ),
    )
    zeros = [0] * len(PITCH_COUNTERS)
    in_play: dict[str, int] = dict.fromkeys(PITCH_COUNTERS, 0)
    in_play.update(
        pitches=3,
        swings=1,
        swings_with_contact=1,
        strikes=2,
        strikes_unknown=1,
        strikes_called=1,
        strikes_in_play=1,
        balls=1,
        balls_called=1,
    )
    box: dict[str, int] = dict.fromkeys(PITCH_COUNTERS, 0)
    box.update(pitches=1, swings=1, swings_with_contact=1, strikes=1, strikes_in_play=1)
    contradictory_strikeout: dict[str, int] = dict.fromkeys(PITCH_COUNTERS, 0)
    contradictory_strikeout.update(pitches=1, strikes=1, strikes_called=1)
    illegal_in_play: dict[str, int] = dict.fromkeys(PITCH_COUNTERS, 0)
    illegal_in_play.update(
        pitches=4,
        swings=1,
        swings_with_contact=1,
        strikes=4,
        strikes_called=3,
        strikes_in_play=1,
    )
    long_walk: dict[str, int] = dict.fromkeys(PITCH_COUNTERS, 0)
    long_walk.update(
        pitches=7,
        swings=1,
        swings_with_contact=1,
        strikes=3,
        strikes_called=2,
        strikes_foul=1,
        balls=4,
        balls_called=4,
    )
    placeholders = ", ".join("?" for _ in range(len(PITCH_COUNTERS) + 1))
    connection.executemany(
        f"INSERT INTO main_models.event_pitch_sequence_stats VALUES ({placeholders})",
        (
            (1, *zeros),
            (2, *([None] * len(PITCH_COUNTERS))),
            (3, *([None] * len(PITCH_COUNTERS))),
            (4, *(in_play[counter] for counter in PITCH_COUNTERS)),
            (5, *([None] * len(PITCH_COUNTERS))),
            (8, *(contradictory_strikeout[counter] for counter in PITCH_COUNTERS)),
            (9, *(long_walk[counter] for counter in PITCH_COUNTERS)),
            (6, *(box[counter] for counter in PITCH_COUNTERS)),
            (7, *([None] * len(PITCH_COUNTERS))),
            (10, *([None] * len(PITCH_COUNTERS))),
            (11, *([None] * len(PITCH_COUNTERS))),
            (12, *(illegal_in_play[counter] for counter in PITCH_COUNTERS)),
        ),
    )
    return connection


def _rows(
    connection: duckdb.DuckDBPyConnection, config: Config | None = None
) -> duckdb.DuckDBPyRelation:
    return connection.sql(build_pitch_completion_sql(config or Config()))


def _row(relation: duckdb.DuckDBPyRelation) -> dict[str, object]:
    values = relation.fetchone()
    if values is None:
        raise AssertionError("query returned no row")
    columns = [str(column[0]) for column in relation.description]
    return {column: value for column, value in zip(columns, values, strict=True)}


def _all_rows(relation: duckdb.DuckDBPyRelation) -> list[dict[str, object]]:
    columns = [str(column[0]) for column in relation.description]
    return [
        {column: value for column, value in zip(columns, values, strict=True)}
        for values in relation.fetchall()
    ]


def _simulate_pitch_transitions(sequence: str) -> tuple[int, int, str | None]:
    balls = 0
    strikes = 0
    terminal: str | None = None
    for token in sequence.split("|") if sequence else ():
        if terminal is not None:
            raise AssertionError(f"pitch {token} followed terminal {terminal}")
        if token == "Ball":
            balls += 1
            if balls == 4:
                terminal = "Walk"
        elif token == "CalledStrike":
            strikes += 1
            if strikes == 3:
                terminal = "StrikeOut"
        elif token == "Foul":
            strikes = min(strikes + 1, 2)
        elif token == "HitBatter":
            terminal = "HitByPitch"
        elif token == "InPlay":
            terminal = "InPlay"
        elif token != "NoPitch":
            raise AssertionError(f"unexpected completed token {token}")
    return balls, strikes, terminal


def test_output_schema_and_pbp_scope() -> None:
    relation = _rows(_connection())

    assert tuple(column[0] for column in relation.description) == tuple(OUTPUT_SCHEMA)
    total = relation.count("*").fetchone()
    box_total = relation.filter("game_id = 'BOX190301010'").count("*").fetchone()
    assert total is not None and total[0] == 11
    assert box_total is not None and box_total[0] == 0


def test_resolved_incremental_sequence_and_real_zero_are_preserved() -> None:
    rows = {row["event_key"]: row for row in _all_rows(_rows(_connection()))}

    assert rows[1]["completed_pitch_sequence"] == ""
    assert rows[1]["completed_pitches"] == 0
    assert rows[1]["pitch_sequence_method"] == "observed_incremental"
    assert rows[4]["source_pitch_sequence"] == "Ball|StrikeUnknownType|InPlay"
    assert rows[4]["completed_pitch_sequence"] == "Ball|Foul|InPlay"
    assert rows[4]["completed_pitches"] == 3
    assert rows[4]["completed_strikes_in_play"] == 1
    assert rows[4]["completed_strikes_unknown"] == 0
    assert rows[4]["completed_strikes_called"] == 0
    assert rows[4]["completed_strikes_foul"] == 1
    assert (
        rows[4]["pitch_token_completion_method"]
        == "declared_count_and_terminal_constrained_token_fallback"
    )
    assert rows[4]["constraint_disposition"] == "source_preserved_without_rewrite"


def test_missing_appearance_splits_completion_at_known_count_boundary() -> None:
    relation = _rows(_connection())
    first_row = _row(relation.filter("event_key = 2"))
    terminal_row = _row(relation.filter("event_key = 3"))

    assert first_row["completed_balls"] == 1
    assert first_row["completed_strikes"] == 1
    assert terminal_row["completed_balls"] == 0
    assert terminal_row["completed_strikes"] == 2
    assert (
        cast(int, first_row["completed_pitches"])
        + cast(int, terminal_row["completed_pitches"])
        == 4
    )
    assert terminal_row["constraint_status"] == "terminal_count_contradiction"
    assert (
        terminal_row["constraint_disposition"]
        == "raw_source_retained_completed_appearance_conflict"
    )


def test_decreasing_unsigned_counts_are_clamped_and_flagged() -> None:
    connection = _connection()
    connection.execute(
        "INSERT INTO main_models.game_start_info VALUES (?, ?, ?)",
        ("PBP195707111", 1957, "PlayByPlay"),
    )
    connection.executemany(
        "INSERT INTO main_models.stg_events VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            (13, "PBP195707111", 1, 1957, None, 2, 1),
            (14, "PBP195707111", 2, 1957, "InPlayOut", 0, 0),
        ),
    )
    connection.executemany(
        "INSERT INTO main_models.stg_event_pitch_sequence_status VALUES (?, ?, ?, ?)",
        (
            (13, 1, "Unavailable", ""),
            (14, 1, "Unavailable", ""),
        ),
    )

    rows = {row["event_key"]: row for row in _all_rows(_rows(connection))}

    assert rows[13]["constraint_status"] == "count_progression_contradiction"
    assert (
        rows[13]["constraint_disposition"]
        == "estimate_separate_from_incompatible_count_progression"
    )
    assert rows[13]["completed_balls"] == 0
    assert rows[13]["completed_strikes"] == 0


def test_unresolved_evidence_is_retained_beside_separate_estimate() -> None:
    relation = _rows(_connection()).filter("event_key = 5")
    row = _row(relation)

    assert row["source_resolution_status"] == "Unresolved"
    assert row["raw_pitch_sequence"] == "BB.W"
    assert row["source_pitch_sequence"] is None
    assert row["completed_balls"] == 4
    assert row["completed_strikes_foul"] == 1
    assert row["completed_unknown_pitches"] == 0
    assert "Foul" in cast(str, row["completed_pitch_sequence"])
    assert row["constraint_status"] == "source_conflict_quarantined"
    assert (
        row["constraint_disposition"]
        == "raw_source_retained_completed_appearance_conflict"
    )


def test_resolved_count_contradiction_is_flagged_without_rewriting_source() -> None:
    row = _row(_rows(_connection()).filter("event_key = 8"))

    assert row["source_resolution_status"] == "Resolved"
    assert row["source_pitches"] == 1
    assert row["completed_pitches"] == 1
    assert row["completed_pitch_sequence"] == "CalledStrike"
    assert row["constraint_status"] == "completed_appearance_terminal_conflict"
    assert (
        row["constraint_disposition"]
        == "raw_source_retained_completed_appearance_conflict"
    )


def test_modern_automatic_intentional_walk_preserves_zero_pitch_possibility() -> None:
    relation = _rows(_connection()).filter("event_key = 7")
    row = _row(relation)

    assert row["completed_pitch_sequence"] == ""
    assert row["completed_pitches"] == 0
    assert row["pitch_sequence_method"] == "structural_automatic_intentional_walk"


def test_missing_counts_are_completed_from_reconstructed_legal_sequence() -> None:
    row = _row(_rows(_connection()).filter("event_key = 10"))

    assert row["count_balls"] is None
    assert row["count_strikes"] is None
    assert row["completed_count_balls"] == 0
    assert row["completed_count_strikes"] == 0
    assert row["count_balls_method"] == "derived_terminal_outcome_constraint"
    assert row["count_strikes_method"] == "derived_terminal_outcome_constraint"


def test_donor_priors_do_not_depend_on_target_sampling() -> None:
    connection = _connection()
    full = _row(_rows(connection).filter("event_key = 5"))
    sampled = _row(
        _rows(
            connection,
            Config(start_season=1903, end_season=1903, sample_games=1),
        ).filter("event_key = 5")
    )

    assert sampled["donor_count"] == full["donor_count"]
    assert sampled["completed_pitches"] == full["completed_pitches"]


def test_generated_appearances_follow_legal_pitch_transitions() -> None:
    rows = {row["event_key"]: row for row in _all_rows(_rows(_connection()))}
    strikeout_sequence = "|".join(
        cast(str, rows[event_key]["completed_pitch_sequence"])
        for event_key in (2, 3)
        if rows[event_key]["completed_pitch_sequence"]
    )
    cases = (
        (cast(str, rows[5]["completed_pitch_sequence"]), "Walk"),
        (cast(str, rows[11]["completed_pitch_sequence"]), "HitByPitch"),
        (strikeout_sequence, "StrikeOut"),
        (cast(str, rows[10]["completed_pitch_sequence"]), "InPlay"),
    )

    for sequence, expected_terminal in cases:
        _, _, terminal = _simulate_pitch_transitions(sequence)
        assert terminal == expected_terminal

    walk_balls, walk_strikes, _ = _simulate_pitch_transitions(
        cast(str, rows[5]["completed_pitch_sequence"])
    )
    assert (walk_balls, walk_strikes) == (4, 2)
    assert rows[5]["completed_count_balls"] == 3
    assert rows[5]["count_balls_method"] == "derived_terminal_outcome_constraint"


def test_resolved_illegal_transition_is_explicitly_conflicted() -> None:
    row = _row(_rows(_connection()).filter("event_key = 12"))

    with pytest.raises(AssertionError, match="followed terminal"):
        _simulate_pitch_transitions(cast(str, row["completed_pitch_sequence"]))
    assert row["constraint_status"] == "completed_appearance_transition_conflict"
    assert (
        row["constraint_disposition"]
        == "raw_source_retained_completed_appearance_conflict"
    )


def test_sampling_is_seeded_and_deterministic() -> None:
    connection = _connection()
    config = Config(sample_games=1, sample_seed="same")
    first = _rows(connection, config).project("game_id").distinct().fetchall()
    second = _rows(connection, config).project("game_id").distinct().fetchall()

    assert first == second
    assert len(first) == 1


def test_config_rejects_unsafe_relations_and_invalid_ranges() -> None:
    with pytest.raises(ValidationError):
        _ = Config(events_relation="main_models.events;drop")
    with pytest.raises(ValidationError):
        _ = Config(start_season=2025, end_season=1903)


def test_unknown_foul_completion_updates_all_dependent_counters() -> None:
    connection = _connection()
    connection.execute(
        "UPDATE main_models.stg_event_pitch_sequences SET sequence_item = 'Unknown' "
        "WHERE event_key = 4 AND sequence_id = 2"
    )
    connection.execute(
        "UPDATE main_models.event_pitch_sequence_stats SET "
        "strikes_called = strikes_called - strikes_unknown, "
        "strikes = strikes - strikes_unknown, unknown_pitches = strikes_unknown, "
        "strikes_unknown = 0 WHERE event_key = 4"
    )
    row = _row(_rows(connection).filter("event_key = 4"))
    tokens = cast(str, row["completed_pitch_sequence"]).split("|")

    assert row["completed_pitches"] == len(tokens)
    assert row["completed_strikes"] == tokens.count("Foul") + tokens.count("InPlay")
    assert row["completed_swings"] == row["completed_strikes"]
    assert row["completed_swings_with_contact"] == row["completed_swings"]
    assert row["source_swings"] == 1
    assert row["source_unknown_pitches"] == 1
    assert (
        row["pitch_sequence_method"]
        == "observed_incremental_with_declared_token_fallback"
    )


def test_unknown_terminal_strike_uses_legal_completion() -> None:
    connection = _connection()
    connection.execute(
        "UPDATE main_models.stg_events SET plate_appearance_result = 'StrikeOut', "
        "count_balls = 0, count_strikes = 2 WHERE event_key = 12"
    )
    connection.execute(
        "DELETE FROM main_models.stg_event_pitch_sequences "
        "WHERE event_key = 12 AND sequence_id = 4"
    )
    connection.execute(
        "UPDATE main_models.stg_event_pitch_sequences "
        "SET sequence_item = 'StrikeUnknownType' "
        "WHERE event_key = 12 AND sequence_id = 3"
    )
    row = _row(_rows(connection).filter("event_key = 12"))

    assert row["source_pitch_sequence"] == "CalledStrike|CalledStrike|StrikeUnknownType"
    assert row["completed_pitch_sequence"] == "CalledStrike|CalledStrike|CalledStrike"
    assert (
        _simulate_pitch_transitions(cast(str, row["completed_pitch_sequence"]))[2]
        == "StrikeOut"
    )
    assert (
        row["constraint_status"] == "source_preserved_no_detected_transition_conflict"
    )


def test_transition_validation_carries_state_across_event_boundaries() -> None:
    connection = _connection()
    connection.execute(
        "UPDATE main_models.stg_event_pitch_sequence_status "
        "SET appearance_start_event_id = 6 WHERE event_key = 9"
    )
    row = _row(_rows(connection).filter("event_key = 9"))

    assert (
        row["completed_pitch_sequence"]
        == "Ball|Ball|CalledStrike|CalledStrike|Foul|Ball|Ball"
    )
    assert row["constraint_status"] == "completed_appearance_transition_conflict"
    assert (
        row["constraint_disposition"]
        == "raw_source_retained_completed_appearance_conflict"
    )


def test_unknown_fallback_conflict_with_known_count_is_explicit() -> None:
    connection = _connection()
    connection.execute(
        "UPDATE main_models.stg_events SET count_strikes = 0 WHERE event_key = 4"
    )
    row = _row(_rows(connection).filter("event_key = 4"))

    assert row["count_strikes"] == 0
    assert row["source_pitch_sequence"] == "Ball|StrikeUnknownType|InPlay"
    assert row["constraint_status"] == "completed_count_boundary_conflict"
    assert (
        row["constraint_disposition"]
        == "raw_source_retained_completed_appearance_conflict"
    )


@pytest.mark.parametrize("result", ["Walk", "StrikeOut", "HitByPitch", "InPlayOut"])
def test_generated_outcomes_and_counter_partitions_across_all_counts(
    result: str,
) -> None:
    connection = _connection()
    for balls in range(4):
        for strikes in range(3):
            connection.execute(
                "UPDATE main_models.stg_events SET plate_appearance_result = ?, "
                "count_balls = ?, count_strikes = ? WHERE event_key = 10",
                [result, balls, strikes],
            )
            row = _row(_rows(connection).filter("event_key = 10"))
            sequence = cast(str, row["completed_pitch_sequence"])
            _, _, terminal = _simulate_pitch_transitions(sequence)
            tokens = sequence.split("|")
            expected = "InPlay" if result == "InPlayOut" else result

            assert terminal == expected
            assert row["completed_pitches"] == len(tokens)
            assert row["completed_balls"] == tokens.count("Ball") + tokens.count(
                "HitBatter"
            )
            assert row["completed_strikes"] == sum(
                tokens.count(token) for token in ("CalledStrike", "Foul", "InPlay")
            )
            assert row["completed_swings"] == tokens.count("Foul") + tokens.count(
                "InPlay"
            )
            assert row["completed_swings_with_contact"] == row["completed_swings"]
            assert row["completed_pitches"] == (
                cast(int, row["completed_balls"]) + cast(int, row["completed_strikes"])
            )


def test_unknown_pitch_fills_known_ball_boundary_without_changing_known_tokens() -> (
    None
):
    connection = _connection()
    connection.execute(
        "UPDATE main_models.stg_event_pitch_sequences SET sequence_item = 'Unknown' "
        "WHERE event_key = 4 AND sequence_id = 2"
    )
    connection.execute(
        "UPDATE main_models.stg_events SET count_balls = 2, count_strikes = 0 "
        "WHERE event_key = 4"
    )
    connection.execute(
        "UPDATE main_models.event_pitch_sequence_stats SET "
        "strikes_called = strikes_called - strikes_unknown, "
        "strikes = strikes - strikes_unknown, unknown_pitches = strikes_unknown, "
        "strikes_unknown = 0 WHERE event_key = 4"
    )
    row = _row(_rows(connection).filter("event_key = 4"))

    assert row["source_pitch_sequence"] == "Ball|Unknown|InPlay"
    assert row["completed_pitch_sequence"] == "Ball|Ball|InPlay"
    assert row["completed_balls"] == 2
    assert row["completed_balls_called"] == 2
    assert row["completed_strikes"] == 1
    assert row["completed_swings"] == 1
    assert (
        row["constraint_status"] == "source_preserved_no_detected_transition_conflict"
    )


def test_non_pitch_annotation_after_terminal_is_not_an_illegal_pitch() -> None:
    connection = _connection()
    connection.execute(
        "INSERT INTO main_models.stg_event_pitch_sequences "
        "VALUES (4, 'PBP190301010', 4, 'NoPitch')"
    )
    row = _row(_rows(connection).filter("event_key = 4"))

    assert row["completed_pitch_sequence"] == "Ball|Foul|InPlay|NoPitch"
    assert (
        row["constraint_status"] == "source_preserved_no_detected_transition_conflict"
    )
