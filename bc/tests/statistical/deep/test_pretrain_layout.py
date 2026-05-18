from __future__ import annotations

import pytest

from python_models.statistical.deep.pretrain.targets import (
    BATTED_BALL_PA_RESULTS,
    EVENT_UNIVERSE_CONTEXT_LAYOUT,
    EVENT_UNIVERSE_CONTEXT_SPEC,
    EVENT_UNIVERSE_DATASET,
    EVENT_UNIVERSE_HEADS,
    EVENT_UNIVERSE_LAYOUT,
    EVENT_UNIVERSE_LOW_CARD_CONTEXT,
    EVENT_UNIVERSE_NUMERIC_CONTEXT,
    EVENT_UNIVERSE_SPEC,
    OBSERVED_LOW_CARD,
    OBSERVED_NUMERIC,
    PLAYER_GROUP_COLS,
    ROW_FILTER_PREDICATE,
    all_pretrain_names,
    get_pretrain_layout,
    get_pretrain_spec,
)

EXPECTED_HEAD_NAMES = (
    "trajectory_remapped",
    "batted_location_general",
    "batted_location_depth",
    "batted_location_edge",
    "batted_to_fielder_class",
)


def test_player_group_is_batter_pitcher_only() -> None:
    assert PLAYER_GROUP_COLS == ("batter_id", "pitcher_id")


def test_high_card_no_fielders_or_runners() -> None:
    high_card = set(EVENT_UNIVERSE_LAYOUT.high_card_columns)
    assert high_card == {"batter_id", "pitcher_id", "park_id", "scorer"}
    for col in high_card:
        assert not col.startswith("fielder_pos_")
        assert not col.startswith("runner_on_")


def test_embedding_group_is_player_batter_pitcher() -> None:
    groups = EVENT_UNIVERSE_LAYOUT.embedding_groups
    assert len(groups) == 1
    name, members = groups[0]
    assert name == "player"
    assert members == PLAYER_GROUP_COLS


def test_observed_outcomes_in_inputs() -> None:
    low_card = set(EVENT_UNIVERSE_LAYOUT.low_card_columns)
    numeric = set(EVENT_UNIVERSE_LAYOUT.numeric_columns)
    for col in OBSERVED_LOW_CARD:
        assert col in low_card, f"observed low_card {col!r} missing"
    for col in OBSERVED_NUMERIC:
        assert col in numeric, f"observed numeric {col!r} missing"


def test_heads_are_imputation_targets_only() -> None:
    names = tuple(h.name for h in EVENT_UNIVERSE_HEADS)
    assert names == EXPECTED_HEAD_NAMES
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
def test_no_head_overlaps_observed_input(forbidden: str) -> None:
    head_names = {h.name for h in EVENT_UNIVERSE_HEADS}
    assert forbidden not in head_names
    target_cols = {h.target_column for h in EVENT_UNIVERSE_HEADS}
    assert forbidden not in target_cols


def test_context_layout_strips_high_card_and_observed() -> None:
    assert EVENT_UNIVERSE_CONTEXT_LAYOUT.high_card_columns == ()
    assert EVENT_UNIVERSE_CONTEXT_LAYOUT.embedding_groups == ()
    assert EVENT_UNIVERSE_CONTEXT_LAYOUT.embedding_unit_names() == ()
    assert EVENT_UNIVERSE_CONTEXT_LAYOUT.low_card_columns == EVENT_UNIVERSE_LOW_CARD_CONTEXT
    assert EVENT_UNIVERSE_CONTEXT_LAYOUT.numeric_columns == EVENT_UNIVERSE_NUMERIC_CONTEXT
    low = set(EVENT_UNIVERSE_CONTEXT_LAYOUT.low_card_columns)
    num = set(EVENT_UNIVERSE_CONTEXT_LAYOUT.numeric_columns)
    for col in OBSERVED_LOW_CARD:
        assert col not in low
    for col in OBSERVED_NUMERIC:
        assert col not in num


def test_specs_registered() -> None:
    assert EVENT_UNIVERSE_SPEC.name == "event_universe"
    assert EVENT_UNIVERSE_CONTEXT_SPEC.name == "event_universe_context"
    names = set(all_pretrain_names())
    assert {"event_universe", "event_universe_context"} == names
    assert get_pretrain_spec("event_universe") is EVENT_UNIVERSE_SPEC
    assert get_pretrain_layout("event_universe") is EVENT_UNIVERSE_LAYOUT
    assert get_pretrain_spec("event_universe_context") is EVENT_UNIVERSE_CONTEXT_SPEC
    assert get_pretrain_layout("event_universe_context") is EVENT_UNIVERSE_CONTEXT_LAYOUT


def test_specs_share_dataset_and_heads() -> None:
    assert EVENT_UNIVERSE_SPEC.dataset_name == EVENT_UNIVERSE_DATASET
    assert EVENT_UNIVERSE_CONTEXT_SPEC.dataset_name == EVENT_UNIVERSE_DATASET
    assert EVENT_UNIVERSE_SPEC.head_specs == EVENT_UNIVERSE_HEADS
    assert EVENT_UNIVERSE_CONTEXT_SPEC.head_specs == EVENT_UNIVERSE_HEADS


def test_row_filter_predicate_is_batted_ball_only_and_shared() -> None:
    assert EVENT_UNIVERSE_SPEC.row_filter_predicate == ROW_FILTER_PREDICATE
    assert EVENT_UNIVERSE_CONTEXT_SPEC.row_filter_predicate == ROW_FILTER_PREDICATE
    excluded = {"StrikeOut", "Walk", "IntentionalWalk", "HitByPitch", "Interference"}
    assert excluded.isdisjoint(set(BATTED_BALL_PA_RESULTS))
    for v in BATTED_BALL_PA_RESULTS:
        assert f"'{v}'" in ROW_FILTER_PREDICATE


def test_context_layout_passes_pre_event_validator() -> None:
    from python_models.statistical.deep.feature_layout import validate_pre_event

    validate_pre_event(EVENT_UNIVERSE_CONTEXT_LAYOUT)


def test_hard_head_names_are_imputation_targets() -> None:
    from python_models.statistical.deep.pretrain.training import HARD_HEAD_NAMES

    head_names = {h.name for h in EVENT_UNIVERSE_HEADS}
    for h in HARD_HEAD_NAMES:
        assert h in head_names
