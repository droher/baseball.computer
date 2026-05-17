from __future__ import annotations

import pytest

from python_models.statistical.deep.pretrain.targets import (
    EVENT_UNIVERSE_DATASET,
    EVENT_UNIVERSE_LOW_CARD,
    EVENT_UNIVERSE_NUMERIC,
    EVENT_UNIVERSE_V8_CONTEXT_LAYOUT,
    EVENT_UNIVERSE_V8_CONTEXT_SPEC,
    EVENT_UNIVERSE_V8_HEADS,
    EVENT_UNIVERSE_V8_LAYOUT,
    EVENT_UNIVERSE_V8_SPEC,
    V8_OBSERVED_LOW_CARD,
    V8_OBSERVED_NUMERIC,
    V8_PLAYER_GROUP_COLS,
    all_pretrain_names,
    get_pretrain_layout,
    get_pretrain_spec,
)


EXPECTED_V8_HEAD_NAMES = (
    "trajectory_remapped",
    "batted_location_general",
    "batted_location_depth",
    "batted_location_edge",
    "batted_to_fielder_class",
)


def test_v8_player_group_is_batter_pitcher_only() -> None:
    assert V8_PLAYER_GROUP_COLS == ("batter_id", "pitcher_id")


def test_v8_high_card_no_fielders_or_runners() -> None:
    high_card = set(EVENT_UNIVERSE_V8_LAYOUT.high_card_columns)
    assert high_card == {"batter_id", "pitcher_id", "park_id", "scorer"}
    for col in high_card:
        assert not col.startswith("fielder_pos_")
        assert not col.startswith("runner_on_")


def test_v8_embedding_group_is_player_batter_pitcher() -> None:
    groups = EVENT_UNIVERSE_V8_LAYOUT.embedding_groups
    assert len(groups) == 1
    name, members = groups[0]
    assert name == "player"
    assert members == V8_PLAYER_GROUP_COLS


def test_v8_observed_outcomes_in_inputs() -> None:
    low_card = set(EVENT_UNIVERSE_V8_LAYOUT.low_card_columns)
    numeric = set(EVENT_UNIVERSE_V8_LAYOUT.numeric_columns)
    for col in V8_OBSERVED_LOW_CARD:
        assert col in low_card, f"observed low_card {col!r} missing from v8 layout"
    for col in V8_OBSERVED_NUMERIC:
        assert col in numeric, f"observed numeric {col!r} missing from v8 layout"


def test_v8_heads_are_imputation_targets_only() -> None:
    names = tuple(h.name for h in EVENT_UNIVERSE_V8_HEADS)
    assert names == EXPECTED_V8_HEAD_NAMES
    forbidden = {
        "pa_result",
        "outs_on_play_capped",
        "runs_on_play_capped",
        "r1_advancement",
        "r2_advancement",
        "r3_advancement",
    }
    assert forbidden.isdisjoint(set(names))


@pytest.mark.parametrize("forbidden", ["pa_result", "r1_advancement", "outs_on_play_capped"])
def test_v8_no_head_overlaps_observed_input(forbidden: str) -> None:
    head_names = {h.name for h in EVENT_UNIVERSE_V8_HEADS}
    assert forbidden not in head_names
    target_cols = {h.target_column for h in EVENT_UNIVERSE_V8_HEADS}
    assert forbidden not in target_cols


def test_v8_context_layout_strips_high_card_and_observed() -> None:
    assert EVENT_UNIVERSE_V8_CONTEXT_LAYOUT.high_card_columns == ()
    assert EVENT_UNIVERSE_V8_CONTEXT_LAYOUT.embedding_groups == ()
    assert EVENT_UNIVERSE_V8_CONTEXT_LAYOUT.low_card_columns == EVENT_UNIVERSE_LOW_CARD
    assert EVENT_UNIVERSE_V8_CONTEXT_LAYOUT.numeric_columns == EVENT_UNIVERSE_NUMERIC
    low = set(EVENT_UNIVERSE_V8_CONTEXT_LAYOUT.low_card_columns)
    num = set(EVENT_UNIVERSE_V8_CONTEXT_LAYOUT.numeric_columns)
    for col in V8_OBSERVED_LOW_CARD:
        assert col not in low
    for col in V8_OBSERVED_NUMERIC:
        assert col not in num


def test_v8_specs_registered() -> None:
    assert EVENT_UNIVERSE_V8_SPEC.name == "event_universe_v8"
    assert EVENT_UNIVERSE_V8_CONTEXT_SPEC.name == "event_universe_v8_context"
    names = set(all_pretrain_names())
    assert {"event_universe_v8", "event_universe_v8_context"}.issubset(names)
    assert get_pretrain_spec("event_universe_v8") is EVENT_UNIVERSE_V8_SPEC
    assert get_pretrain_layout("event_universe_v8") is EVENT_UNIVERSE_V8_LAYOUT
    assert get_pretrain_spec("event_universe_v8_context") is EVENT_UNIVERSE_V8_CONTEXT_SPEC
    assert get_pretrain_layout("event_universe_v8_context") is EVENT_UNIVERSE_V8_CONTEXT_LAYOUT


def test_v8_specs_share_dataset_and_heads() -> None:
    assert EVENT_UNIVERSE_V8_SPEC.dataset_name == EVENT_UNIVERSE_DATASET
    assert EVENT_UNIVERSE_V8_CONTEXT_SPEC.dataset_name == EVENT_UNIVERSE_DATASET
    assert EVENT_UNIVERSE_V8_SPEC.head_specs == EVENT_UNIVERSE_V8_HEADS
    assert EVENT_UNIVERSE_V8_CONTEXT_SPEC.head_specs == EVENT_UNIVERSE_V8_HEADS


def test_v8_row_filter_predicate_is_batted_ball_only_and_shared() -> None:
    from python_models.statistical.deep.pretrain.targets import (
        V8_BATTED_BALL_PA_RESULTS,
        V8_ROW_FILTER_PREDICATE,
    )

    assert EVENT_UNIVERSE_V8_SPEC.row_filter_predicate == V8_ROW_FILTER_PREDICATE
    assert EVENT_UNIVERSE_V8_CONTEXT_SPEC.row_filter_predicate == V8_ROW_FILTER_PREDICATE
    excluded = {"StrikeOut", "Walk", "IntentionalWalk", "HitByPitch", "Interference"}
    assert excluded.isdisjoint(set(V8_BATTED_BALL_PA_RESULTS))
    for v in V8_BATTED_BALL_PA_RESULTS:
        assert f"'{v}'" in V8_ROW_FILTER_PREDICATE
