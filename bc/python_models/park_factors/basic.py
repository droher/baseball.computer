"""SQL builder for ``main_models.calc_park_factors_basic``.

Mirrors ``bc/models/intermediate/park_factors/calc_park_factors_basic.sql`` —
team-game runs / innings, league-rate factor, 2-yr trailing window, pair-wise
self-join with clamped runs/inning, sample-weighted ``basic_park_factor``.
"""

from __future__ import annotations


def build_basic_park_factor_sql() -> str:
    """Return the 6-CTE SQL body for ``calc_park_factors_basic``."""
    return """
WITH batting_agg AS (
    SELECT
        s.park_id,
        s.season,
        s.league,
        s.team_id,
        s.opponent_id,
        SUM(r.runs_scored + r.runs_allowed) AS runs,
        SUM(COALESCE(r.innings_pitched + r.opponent_innings_pitched, 18)) AS innings
    FROM main_models.team_game_start_info AS s
    INNER JOIN main_models.team_game_results AS r USING (game_id, team_id)
    WHERE s.game_type = 'RegularSeason'
        AND NOT s.is_interleague
    GROUP BY 1, 2, 3, 4, 5
),

averages AS (
    SELECT DISTINCT ON (season, league)
        season,
        league,
        SUM(runs) OVER w / SUM(innings) OVER w AS run_rate_league,
        SUM(runs) OVER () / SUM(innings) OVER () AS run_rate_all,
        run_rate_league / run_rate_all AS run_factor
    FROM batting_agg
    WINDOW w AS (PARTITION BY season, league)
),

multi_year_range AS (
    SELECT
        ba.park_id,
        ba.season,
        ba.league,
        ba.team_id,
        ba.opponent_id,
        SUM(ba.runs) OVER w / averages.run_factor AS runs,
        SUM(ba.innings) OVER w AS innings
    FROM batting_agg AS ba
    INNER JOIN averages USING (season, league)
    WINDOW w AS (
        PARTITION BY park_id, league, team_id, opponent_id
        ORDER BY season
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
),

self_joined AS (
    SELECT
        this.park_id AS this_park_id,
        other.park_id AS other_park_id,
        this.season,
        this.league,
        this.team_id,
        this.opponent_id,
        GREATEST(0.1, LEAST(1, this.runs / this.innings)) AS this_runs_per_inning,
        GREATEST(0.1, LEAST(1, other.runs / other.innings)) AS other_runs_per_inning,
        SQRT(LEAST(this.innings, other.innings)) AS sample_size,
        SUM(sample_size) OVER (PARTITION BY this_park_id, this.season, this.league) AS sum_sample_size
    FROM multi_year_range AS this
    INNER JOIN multi_year_range AS other
        ON this.park_id != other.park_id
            AND this.season = other.season
            AND this.league = other.league
            AND this.team_id = other.team_id
            AND this.opponent_id = other.opponent_id
),

rate_calculation AS (
    SELECT
        *,
        MAX(sum_sample_size) OVER (PARTITION BY this_park_id, season, league) AS scaling_factor,
        sample_size * (scaling_factor / sum_sample_size) AS sample_weight
    FROM self_joined
),

final AS (
    SELECT
        this_park_id AS park_id,
        season,
        league,
        SUM(sample_size) AS sqrt_sample_size,
        SUM(this_runs_per_inning * sample_weight) / SUM(sample_weight) AS avg_this_runs_per_inning,
        SUM(other_runs_per_inning * sample_weight) / SUM(sample_weight) AS avg_other_runs_per_inning,
        avg_this_runs_per_inning / avg_other_runs_per_inning AS basic_park_factor
    FROM rate_calculation
    GROUP BY 1, 2, 3
)

SELECT * FROM final
"""
