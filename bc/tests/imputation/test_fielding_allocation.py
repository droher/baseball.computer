from __future__ import annotations

import polars as pl

from python_models.imputation.fielding_allocation import (
    PATCH_OUTPUT_SCHEMA,
    build_compatible_assignment_patch,
)


def _frame(
    capacities: list[float],
    second_players: list[str] | None = None,
) -> pl.DataFrame:
    players = ["first001", "short001"]
    positions = [3, 6]
    probabilities = [0.7, 0.3]
    rows: list[dict[str, object]] = []
    for event_key in (10, 11):
        eligible_players = (
            players if event_key == 10 or second_players is None else second_players
        )
        eligible_positions = positions if len(eligible_players) == 2 else [6]
        eligible_probabilities = probabilities if len(eligible_players) == 2 else [1.0]
        eligible_capacities = (
            capacities if len(eligible_players) == 2 else [capacities[1]]
        )
        rows.append(
            {
                "event_key": event_key,
                "sequence_id": 1,
                "game_id": "G1",
                "credit_type": "putout",
                "raw_fielding_position": 0,
                "completed_fielding_position": 3,
                "player_id": "first001",
                "completion_method": "partially_pooled_empirical_distribution",
                "aggregate_constraint_complete": True,
                "candidate_positions": eligible_positions,
                "candidate_player_ids": eligible_players,
                "candidate_probabilities": eligible_probabilities,
                "candidate_aggregate_capacities": eligible_capacities,
                "sampled_probability": eligible_probabilities[0],
                "expected_credit": eligible_probabilities[0],
            }
        )
    return pl.DataFrame(rows)


def test_compatible_capacity_assignment_conserves_player_totals() -> None:
    patch = build_compatible_assignment_patch(_frame([1.0, 1.0]))
    assert patch.schema == dict(PATCH_OUTPUT_SCHEMA)
    assert patch["allocation_applied"].to_list() == [True, True]
    assert patch["aggregate_constraint_delta"].to_list() == [0.0, 0.0]
    assert sorted(patch["player_id"].to_list()) == ["first001", "short001"]
    assert set(patch["completion_method"]) == {"aggregate_capacity_matching"}


def test_matching_respects_event_specific_eligible_players() -> None:
    patch = build_compatible_assignment_patch(
        _frame([1.0, 1.0], second_players=["short001"])
    ).sort("event_key")
    assert patch["player_id"].to_list() == ["first001", "short001"]
    assert patch["completed_fielding_position"].to_list() == [3, 6]


def test_incompatible_capacity_sum_preserves_initial_draw_and_flags_group() -> None:
    patch = build_compatible_assignment_patch(_frame([2.0, 1.0]))
    assert patch["allocation_applied"].to_list() == [False, False]
    assert patch["completed_fielding_position"].to_list() == [3, 3]
    assert set(patch["constraint_disposition"]) == {
        "aggregate_capacity_assignment_incompatible"
    }


def test_partial_constraints_are_left_to_sql_disposition() -> None:
    frame = _frame([1.0, 1.0]).with_columns(
        pl.lit(False).alias("aggregate_constraint_complete")
    )
    patch = build_compatible_assignment_patch(frame)
    assert patch.is_empty()


def test_assignment_is_reproducible() -> None:
    frame = _frame([1.0, 1.0])
    assert build_compatible_assignment_patch(frame).equals(
        build_compatible_assignment_patch(frame)
    )
