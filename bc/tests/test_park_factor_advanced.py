from typing import cast

import duckdb
import pytest

from python_models.park_factors.advanced import (
    ADVANCED_PARK_FACTOR_RATE_STATS,
    build_advanced_park_factor_sql,
)


def advanced_park_factor_rows(
    *, include_other_league: bool, shared_players: bool
) -> list[tuple[object, ...]]:
    parks = "('A', 'AL', 6), ('B', 'AL', 12)"
    if include_other_league:
        parks += ", ('N', 'NN2', 27)"
    player_league = "'shared'" if shared_players else "league"
    stat_columns = ", ".join(
        f"(i < hits)::INTEGER AS {stat}" for stat in ADVANCED_PARK_FACTOR_RATE_STATS
    )
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
                league, 'b_' || {player_league} AS batter_id,
                'p_' || {player_league} AS pitcher_id,
                'RegularSeason' AS game_type, FALSE AS is_interleague, hits, i
            FROM (VALUES {parks}) AS parks(park_id, league, hits),
                range(30) AS events(i)
        """)
        connection.execute(f"""
            CREATE TABLE main_models.event_offense_stats AS
            SELECT event_key, 1 AS plate_appearances, {stat_columns}
            FROM main_models.event_states_full
        """)
        sql = build_advanced_park_factor_sql()
        return cast(
            "list[tuple[object, ...]]",
            connection.execute(f"SELECT * FROM ({sql}) ORDER BY ALL").fetchall(),
        )


@pytest.mark.parametrize("shared_players", [False, True])
def test_advanced_peer_comparisons_stay_within_league(shared_players: bool) -> None:
    original = advanced_park_factor_rows(
        include_other_league=False, shared_players=shared_players
    )
    expanded = advanced_park_factor_rows(
        include_other_league=True, shared_players=shared_players
    )
    assert expanded == original
    assert len(original) == 2
    for row in original:
        assert row[2] == "AL"
        for factor in row[4:]:
            assert isinstance(factor, float)
            assert factor > 0
            assert (factor < 1) if row[0] == "A" else (factor > 1)
