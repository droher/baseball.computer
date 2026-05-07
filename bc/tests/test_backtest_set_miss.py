"""Unit tests for the date-independent set-mismatch metrics in
``scripts/backtest_synthetic_lineups.py``.

These cover the pitcher-axis exclusion, the per-(player, fielding_position)
bucketing, and the orphan-team filtering — all of which are pure DataFrame
transforms decoupled from DuckDB or the optimizer.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import backtest_synthetic_lineups as bsl  # noqa: E402


def _per_player(rows: list[dict[str, object]]) -> pl.DataFrame:
    schema = {
        "game_id": pl.String,
        "side": pl.String,
        "season": pl.Int16,
        "team_id": pl.String,
        "player_id": pl.String,
        "syn_pos": pl.UInt8,
        "real_pos": pl.UInt8,
    }
    return pl.DataFrame(rows, schema=schema)


def _games_full(game_ids: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": game_ids,
            "season": [1903] * len(game_ids),
            "home_team_id": ["CHN"] * len(game_ids),
            "away_team_id": ["PIT"] * len(game_ids),
        },
        schema={
            "game_id": pl.String,
            "season": pl.Int16,
            "home_team_id": pl.String,
            "away_team_id": pl.String,
        },
    )


def test_set_miss_rate_perfect_match() -> None:
    """syn == real on every (player, position) → rate is 0."""
    games = ["G1", "G2", "G3"]
    rows: list[dict[str, object]] = []
    for g in games:
        rows.append(
            {
                "game_id": g,
                "side": "Home",
                "season": 1903,
                "team_id": "CHN",
                "player_id": "alice",
                "syn_pos": 3,
                "real_pos": 3,
            }
        )
    per_player = _per_player(rows)
    player_level, pos_level = bsl._set_miss_metrics(per_player, _games_full(games))
    assert bsl._set_miss_rate(player_level) == pytest.approx(0.0)
    assert bsl._set_miss_rate(pos_level) == pytest.approx(0.0)


def test_set_miss_rate_pitcher_excluded_on_axis_not_player() -> None:
    """A two-way player who pitched once but played 1B twice still scores
    on his 1B bucket. The pitcher row is dropped; the 1B rows survive."""
    games = ["G1", "G2", "G3"]
    rows: list[dict[str, object]] = [
        # P start: dropped by axis filter, regardless of player.
        {
            "game_id": "G1",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "twoway",
            "syn_pos": 1,
            "real_pos": 1,
        },
        # 1B starts: both kept, both perfectly matched.
        {
            "game_id": "G2",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "twoway",
            "syn_pos": 3,
            "real_pos": 3,
        },
        {
            "game_id": "G3",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "twoway",
            "syn_pos": 3,
            "real_pos": 3,
        },
    ]
    per_player = _per_player(rows)
    player_level, pos_level = bsl._set_miss_metrics(per_player, _games_full(games))
    twoway_player = player_level.filter(pl.col("player_id") == "twoway")
    assert twoway_player.height == 1
    assert int(twoway_player["syn_starts"].item()) == 2
    assert int(twoway_player["real_starts"].item()) == 2
    assert bsl._set_miss_rate(player_level) == pytest.approx(0.0)
    twoway_pos = pos_level.filter(pl.col("player_id") == "twoway")
    assert twoway_pos.height == 1
    assert int(twoway_pos["fielding_position"].item()) == 3


def test_set_miss_rate_pitcher_axis_drops_cross_rows() -> None:
    """A row with syn_pos=1 and real_pos != 1 (or vice-versa) is dropped.
    The forfeited real-start is not counted in either numerator or
    denominator. Other rows for the same player on other days still count.
    Locks in the current spec where pitcher exclusion is on the axis,
    not on the player.
    """
    games = ["G1", "G2", "G3"]
    rows: list[dict[str, object]] = [
        # Cross row: syn pitches, real plays 1B. Whole row dropped.
        {
            "game_id": "G1",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "twoway",
            "syn_pos": 1,
            "real_pos": 3,
        },
        # Non-pitcher matched row: counted.
        {
            "game_id": "G2",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "twoway",
            "syn_pos": 3,
            "real_pos": 3,
        },
        # Cross row in the other direction: also dropped.
        {
            "game_id": "G3",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "twoway",
            "syn_pos": 3,
            "real_pos": 1,
        },
    ]
    per_player = _per_player(rows)
    player_level, pos_level = bsl._set_miss_metrics(per_player, _games_full(games))
    twoway_player = player_level.filter(pl.col("player_id") == "twoway")
    assert twoway_player.height == 1
    # Only G2 survives: syn=1, real=1.
    assert int(twoway_player["syn_starts"].item()) == 1
    assert int(twoway_player["real_starts"].item()) == 1
    assert bsl._set_miss_rate(player_level) == pytest.approx(0.0)
    twoway_pos = pos_level.filter(pl.col("player_id") == "twoway")
    # Only the (twoway, 3) bucket exists; the dropped rows leave no trace.
    assert twoway_pos.height == 1
    assert int(twoway_pos["fielding_position"].item()) == 3


def test_set_miss_rate_player_match_position_wrong_separates_metrics() -> None:
    """Right player, wrong position: set_miss_rate stays 0; pos_set_miss_rate
    rises because the (player, F) buckets diverge."""
    games = ["G1", "G2"]
    rows: list[dict[str, object]] = [
        {
            "game_id": "G1",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "alice",
            "syn_pos": 6,  # SS
            "real_pos": 4,  # 2B
        },
        {
            "game_id": "G2",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "alice",
            "syn_pos": 6,
            "real_pos": 4,
        },
    ]
    per_player = _per_player(rows)
    player_level, pos_level = bsl._set_miss_metrics(per_player, _games_full(games))
    assert bsl._set_miss_rate(player_level) == pytest.approx(0.0)
    # Pos-level: per-player has two rows (SS: syn=2, real=0; 2B: syn=0, real=2).
    # Σ_min = 0; Σ_real = 2; rate = 1.0.
    assert bsl._set_miss_rate(pos_level) == pytest.approx(1.0)


def test_set_miss_rate_one_real_starter_missed() -> None:
    """Real starter not in syn at all: contributes to denominator only.
    Σ_min = 1 (one matched start), Σ_real = 2; rate = 0.5."""
    games = ["G1", "G2"]
    rows: list[dict[str, object]] = [
        # alice: real-only start. syn=null.
        {
            "game_id": "G1",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "alice",
            "syn_pos": None,
            "real_pos": 3,
        },
        # bob: matched start.
        {
            "game_id": "G2",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "bob",
            "syn_pos": 3,
            "real_pos": 3,
        },
    ]
    per_player = _per_player(rows)
    player_level, _ = bsl._set_miss_metrics(per_player, _games_full(games))
    # alice: syn=0, real=1 → matched=0. bob: syn=1, real=1 → matched=1.
    assert bsl._set_miss_rate(player_level) == pytest.approx(0.5)


def test_set_miss_rate_over_started_player_does_not_self_penalize() -> None:
    """The plan's spot-check intent: a player over-started by syn doesn't
    raise his own contribution to set_miss_rate. min(syn, real) caps at real."""
    games = ["G1", "G2", "G3"]
    rows: list[dict[str, object]] = [
        {
            "game_id": "G1",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "raubt101",
            "syn_pos": 4,
            "real_pos": 4,
        },
        {
            "game_id": "G2",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "raubt101",
            "syn_pos": 4,
            "real_pos": 4,
        },
        # G3: syn over-starts him; real didn't.
        {
            "game_id": "G3",
            "side": "Home",
            "season": 1903,
            "team_id": "CHN",
            "player_id": "raubt101",
            "syn_pos": 4,
            "real_pos": None,
        },
    ]
    per_player = _per_player(rows)
    player_level, _ = bsl._set_miss_metrics(per_player, _games_full(games))
    # syn=3, real=2. min=2, total=2. rate = 1 - 2/2 = 0.
    assert bsl._set_miss_rate(player_level) == pytest.approx(0.0)


def test_drop_orphan_keys_filters_team_seasons() -> None:
    level = pl.DataFrame(
        {
            "season": pl.Series([1903, 1903, 1904], dtype=pl.Int16),
            "team_id": pl.Series(["CHN", "PIT", "CHN"], dtype=pl.String),
            "player_id": pl.Series(["a", "b", "c"], dtype=pl.String),
            "syn_starts": pl.Series([1, 2, 3], dtype=pl.Int32),
            "real_starts": pl.Series([1, 2, 3], dtype=pl.Int32),
        }
    )
    out = bsl._drop_orphan_keys(level, {(1903, "PIT")})
    assert out.height == 2
    assert set(out["team_id"].to_list()) == {"CHN"}


def test_set_miss_rate_handles_empty_frame() -> None:
    empty = pl.DataFrame(
        {
            "syn_starts": pl.Series([], dtype=pl.Int32),
            "real_starts": pl.Series([], dtype=pl.Int32),
        }
    )
    assert bsl._set_miss_rate(empty) == 0.0


def test_set_miss_rate_zero_real_total_yields_zero() -> None:
    """All rows have real_starts = 0 (only syn populated). The metric is
    defined as 1 − matched/real_total and undefined when real_total is 0;
    we return 0 instead of dividing by zero."""
    level = pl.DataFrame(
        {
            "syn_starts": pl.Series([5, 3], dtype=pl.Int32),
            "real_starts": pl.Series([0, 0], dtype=pl.Int32),
        }
    )
    assert bsl._set_miss_rate(level) == 0.0
