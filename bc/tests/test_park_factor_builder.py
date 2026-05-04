"""Unit tests for the park-factor SQL builder."""

from __future__ import annotations

import pytest

from python_models.park_factors.builder import batter_pitcher_park_factor

_BASE_RATE_STATS = ["singles", "doubles", "triples", "home_runs"]


def test_emits_park_factor_columns_per_rate_stat():
    sql = batter_pitcher_park_factor(
        rate_stats=_BASE_RATE_STATS,
        denominator_stat="plate_appearances",
    )
    for s in _BASE_RATE_STATS:
        assert f"{s}_park_factor" in sql, f"missing {s}_park_factor in output"


def test_groups_by_park_season_league():
    sql = batter_pitcher_park_factor(
        rate_stats=_BASE_RATE_STATS,
        denominator_stat="plate_appearances",
    ).lower()
    assert "park_id" in sql and "season" in sql and "league" in sql


def test_batter_hand_split_adds_batter_hand_partition():
    sql = batter_pitcher_park_factor(
        rate_stats=_BASE_RATE_STATS,
        denominator_stat="plate_appearances",
        batter_hand_split=True,
    )
    assert "batter_hand" in sql


def test_use_odds_false_picks_rate_park_factor():
    sql = batter_pitcher_park_factor(
        rate_stats=_BASE_RATE_STATS,
        denominator_stat="batting_outs",
        use_odds=False,
    )
    assert "rate_park_factor" in sql


def test_use_odds_true_picks_odds_park_factor():
    sql = batter_pitcher_park_factor(
        rate_stats=_BASE_RATE_STATS,
        denominator_stat="plate_appearances",
        use_odds=True,
    )
    assert "odds_park_factor" in sql


def test_filter_exp_threaded_into_lines_cte():
    custom = "trajectory_known = 1 AND batting_outs > 0"
    sql = batter_pitcher_park_factor(
        rate_stats=["trajectory_fly_ball"],
        denominator_stat="plate_appearances",
        filter_exp=custom,
    )
    assert custom in sql


def test_default_filter_is_one_equals_one():
    sql = batter_pitcher_park_factor(
        rate_stats=["singles"],
        denominator_stat="plate_appearances",
    )
    assert "1=1" in sql or "1 = 1" in sql


@pytest.mark.parametrize("denominator", ["plate_appearances", "batting_outs", "balls_in_play"])
def test_denominator_appears_in_sql(denominator):
    sql = batter_pitcher_park_factor(
        rate_stats=["singles"],
        denominator_stat=denominator,
    )
    assert denominator in sql
