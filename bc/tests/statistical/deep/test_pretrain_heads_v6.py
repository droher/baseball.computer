"""Phase-3 v6 pretrain head registry contract."""

from __future__ import annotations

from python_models.statistical.deep.pretrain.targets import EVENT_UNIVERSE_HEADS_V4
from python_models.statistical.deep.pretrain.training import HARD_HEAD_NAMES

V6_DROPPED_HEADS: frozenset[str] = frozenset({"result_family", "hit_or_out"})

V6_EXPECTED_HEAD_NAMES: frozenset[str] = frozenset(
    {
        "pa_result",
        "outs_on_play_capped",
        "runs_on_play_capped",
        "trajectory_remapped",
        "r1_advancement",
        "r2_advancement",
        "r3_advancement",
        "batted_location_general",
        "batted_location_depth",
        "batted_location_edge",
        "batted_to_fielder_class",
    }
)


def test_dropped_heads_absent() -> None:
    names = {h.name for h in EVENT_UNIVERSE_HEADS_V4}
    assert names.isdisjoint(V6_DROPPED_HEADS)


def test_expected_heads_present() -> None:
    names = {h.name for h in EVENT_UNIVERSE_HEADS_V4}
    assert names == V6_EXPECTED_HEAD_NAMES


def test_hard_head_names_non_redundant() -> None:
    assert "hit_or_out" not in HARD_HEAD_NAMES
    assert "batted_to_fielder_class" in HARD_HEAD_NAMES
    assert "pa_result" in HARD_HEAD_NAMES
    assert "trajectory_remapped" in HARD_HEAD_NAMES
