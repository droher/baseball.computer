"""Invariants for the stat-name lists that drive the event and aggregate models."""

from __future__ import annotations

import pytest

from macros import _stat_lists as sl


@pytest.mark.parametrize(
    ("full", "zero_filled"),
    [
        (sl.EVENT_LEVEL_OFFENSE_STATS, sl.EVENT_LEVEL_OFFENSE_ZERO_FILLED_STATS),
        (sl.EVENT_LEVEL_PITCHING_STATS, sl.EVENT_LEVEL_PITCHING_ZERO_FILLED_STATS),
    ],
    ids=["offense", "pitching"],
)
def test_zero_filled_plus_normalized_reproduces_full_list(full, zero_filled):
    assert set(sl.NORMALIZED_PITCH_COUNTERS) <= set(full)
    assert set(zero_filled).isdisjoint(sl.NORMALIZED_PITCH_COUNTERS)
    assert sorted(zero_filled + sl.NORMALIZED_PITCH_COUNTERS) == sorted(full)
    assert zero_filled == [s for s in full if s not in sl.NORMALIZED_PITCH_COUNTERS]


@pytest.mark.parametrize(
    "stats",
    [
        sl.NORMALIZED_PITCH_COUNTERS,
        sl.EVENT_LEVEL_OFFENSE_STATS,
        sl.EVENT_LEVEL_PITCHING_STATS,
        sl.EVENT_LEVEL_OFFENSE_ZERO_FILLED_STATS,
        sl.EVENT_LEVEL_PITCHING_ZERO_FILLED_STATS,
        sl._COMBINED_PITCHING_STATS,
    ],
    ids=[
        "normalized",
        "offense",
        "pitching",
        "offense_zero_filled",
        "pitching_zero_filled",
        "combined_pitching",
    ],
)
def test_lists_have_no_duplicates(stats):
    assert len(stats) == len(set(stats))


def test_baserunning_derived_pitch_stats_are_not_normalized_counters():
    assert {"passed_balls", "wild_pitches", "balks"}.isdisjoint(
        sl.NORMALIZED_PITCH_COUNTERS
    )


def _stat_of(sum_expr: str) -> str:
    return sum_expr.rsplit(" AS ", 1)[1]


@pytest.mark.parametrize(
    ("block", "source"),
    [
        (
            sl._sum_cast_block(
                sl.EVENT_LEVEL_OFFENSE_STATS, "UTINYINT", fill_zero=True
            ),
            sl.EVENT_LEVEL_OFFENSE_STATS,
        ),
        (
            sl._sum_cast_block(sl.EVENT_LEVEL_OFFENSE_STATS, "USMALLINT"),
            sl.EVENT_LEVEL_OFFENSE_STATS,
        ),
        (
            sl._sum_cast_block(sl._COMBINED_PITCHING_STATS, "INT"),
            sl._COMBINED_PITCHING_STATS,
        ),
        (sl.player_pitching_sum_block(None), sl.EVENT_LEVEL_PITCHING_STATS),
    ],
    ids=["offense_fill", "offense_nofill", "combined_pitching", "player_pitching"],
)
def test_sum_blocks_guard_exactly_the_normalized_counters(block, source):
    assert [_stat_of(e) for e in block] == source
    for expr in block:
        stat = _stat_of(expr)
        guarded = (
            "pitch_sequence_resolution_status IN ('Unavailable', 'Unresolved')" in expr
        )
        assert guarded == (stat in sl.NORMALIZED_PITCH_COUNTERS), expr
        assert f"SUM({stat})" in expr


def test_fill_zero_only_applies_to_normalized_counters():
    filled = sl._sum_cast_block(
        sl.EVENT_LEVEL_OFFENSE_STATS, "UTINYINT", fill_zero=True
    )
    unfilled = sl._sum_cast_block(sl.EVENT_LEVEL_OFFENSE_STATS, "UTINYINT")
    for f, u in zip(filled, unfilled, strict=True):
        stat = _stat_of(f)
        if stat in sl.NORMALIZED_PITCH_COUNTERS:
            assert f"COALESCE(SUM({stat}), 0)" in f
            assert "COALESCE" not in u
        else:
            assert f == u


def test_status_aggregate_orders_worst_first():
    expr = sl.PITCH_SEQUENCE_RESOLUTION_STATUS_AGG
    positions = [
        expr.index(f"THEN '{s}'") for s in ("Unresolved", "Unavailable", "Resolved")
    ]
    assert positions == sorted(positions)
    assert expr.endswith("AS pitch_sequence_resolution_status")


@pytest.mark.parametrize("default_cast", ["UTINYINT", "USMALLINT"])
def test_unsigned_blocks_keep_surplus_signed(default_cast):
    casts = {
        _stat_of(e): e.rsplit("::", 1)[1].split(" AS ")[0]
        for e in sl._sum_cast_block(sl.EVENT_LEVEL_OFFENSE_STATS, default_cast)
    }
    assert {c for s, c in casts.items() if s.startswith("surplus")} == {"INT1"}
    assert {c for s, c in casts.items() if not s.startswith("surplus")} == {
        default_cast
    }


@pytest.mark.parametrize("default_cast", ["SMALLINT", "INT"])
def test_signed_blocks_widen_every_stat(default_cast):
    block = sl._sum_cast_block(sl._COMBINED_PITCHING_STATS, default_cast)
    assert {e.rsplit("::", 1)[1].split(" AS ")[0] for e in block} == {default_cast}
