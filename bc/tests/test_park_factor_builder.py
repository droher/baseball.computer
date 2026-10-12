"""Unit tests for the park-factor SQL builder."""

from __future__ import annotations

from typing import cast

import duckdb
import pytest

from python_models.park_factors.builder import batter_pitcher_park_factor

_BASE_RATE_STATS = ["singles", "doubles", "triples", "home_runs"]


def test_emits_park_factor_columns_per_rate_stat() -> None:
    sql = batter_pitcher_park_factor(
        rate_stats=_BASE_RATE_STATS,
        denominator_stat="plate_appearances",
    )
    for s in _BASE_RATE_STATS:
        assert f"{s}_park_factor" in sql, f"missing {s}_park_factor in output"


def test_groups_by_park_season_league() -> None:
    sql = batter_pitcher_park_factor(
        rate_stats=_BASE_RATE_STATS,
        denominator_stat="plate_appearances",
    ).lower()
    assert "park_id" in sql and "season" in sql and "league" in sql


def test_batter_hand_split_adds_batter_hand_partition() -> None:
    sql = batter_pitcher_park_factor(
        rate_stats=_BASE_RATE_STATS,
        denominator_stat="plate_appearances",
        batter_hand_split=True,
    )
    assert "batter_hand" in sql


def test_use_odds_false_picks_rate_park_factor() -> None:
    sql = batter_pitcher_park_factor(
        rate_stats=_BASE_RATE_STATS,
        denominator_stat="batting_outs",
        use_odds=False,
    )
    assert "rate_park_factor" in sql


def test_use_odds_true_picks_odds_park_factor() -> None:
    sql = batter_pitcher_park_factor(
        rate_stats=_BASE_RATE_STATS,
        denominator_stat="plate_appearances",
        use_odds=True,
    )
    assert "odds_park_factor" in sql


def test_filter_exp_threaded_into_lines_cte() -> None:
    custom = "trajectory_known = 1 AND batting_outs > 0"
    sql = batter_pitcher_park_factor(
        rate_stats=["trajectory_fly_ball"],
        denominator_stat="plate_appearances",
        filter_exp=custom,
    )
    assert custom in sql


def test_default_filter_is_one_equals_one() -> None:
    sql = batter_pitcher_park_factor(
        rate_stats=["singles"],
        denominator_stat="plate_appearances",
    )
    assert "1=1" in sql or "1 = 1" in sql


@pytest.mark.parametrize(
    "denominator", ["plate_appearances", "batting_outs", "balls_in_play"]
)
def test_denominator_appears_in_sql(denominator: str) -> None:
    sql = batter_pitcher_park_factor(
        rate_stats=["singles"],
        denominator_stat=denominator,
    )
    assert denominator in sql


def park_factor_rows(
    *,
    include_other_league: bool,
    shared_players: bool,
    hand_split: bool,
    use_odds: bool,
) -> list[tuple[object, ...]]:
    parks = "('A', 'AL', 6), ('B', 'AL', 12)"
    if include_other_league:
        parks += ", ('N', 'NN2', 27)"
    player_league = "'shared'" if shared_players else "league"
    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA main_models")
        connection.execute(f"""
            CREATE TABLE main_models.game_start_info AS
            SELECT park_id, 1933 AS season, league AS home_league,
                'RegularSeason' AS game_type
            FROM (VALUES {parks}) AS parks(park_id, league, hits), range(30)
        """)
        connection.execute(f"""
            CREATE TABLE main_models.event_states_full AS
            SELECT row_number() OVER () AS event_key, park_id, 1933 AS season,
                league, 'b_' || {player_league} || hand AS batter_id,
                'p_' || {player_league} || hand AS pitcher_id,
                hand AS batter_hand, 'RegularSeason' AS game_type,
                FALSE AS is_interleague, hits, i
            FROM (VALUES {parks}) AS parks(park_id, league, hits),
                range(30) AS events(i), (VALUES ('L'), ('R')) AS hands(hand)
        """)
        connection.execute("""
            CREATE TABLE main_models.event_offense_stats AS
            SELECT event_key, 1 AS plate_appearances, (i < hits)::INTEGER AS hits
            FROM main_models.event_states_full
        """)
        sql = batter_pitcher_park_factor(
            ["hits"],
            "plate_appearances",
            prior_sample_size=25,
            batter_hand_split=hand_split,
            use_odds=use_odds,
        )
        return cast(
            "list[tuple[object, ...]]",
            connection.execute(f"SELECT * FROM ({sql}) ORDER BY ALL").fetchall(),
        )


@pytest.mark.parametrize("shared_players", [False, True])
@pytest.mark.parametrize("hand_split", [False, True])
@pytest.mark.parametrize("use_odds", [False, True])
def test_peer_comparisons_stay_within_league(
    shared_players: bool, hand_split: bool, use_odds: bool
) -> None:
    original = park_factor_rows(
        include_other_league=False,
        shared_players=shared_players,
        hand_split=hand_split,
        use_odds=use_odds,
    )
    expanded = park_factor_rows(
        include_other_league=True,
        shared_players=shared_players,
        hand_split=hand_split,
        use_odds=use_odds,
    )
    assert expanded == original
    assert len(original) == (4 if hand_split else 2)
    for row in original:
        assert row[2] == "AL"
        factor = row[-1]
        assert isinstance(factor, float)
        assert factor > 0
        assert (factor < 1) if row[0] == "A" else (factor > 1)
