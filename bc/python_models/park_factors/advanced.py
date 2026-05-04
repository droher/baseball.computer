"""SQL builder for ``main_models.calc_park_factors_advanced``.

The ``multi_year_range`` SUM windows cast to ``INT``; the synthetic prior
rows compute ``averages.avg_*_per_pa * 1000`` as DOUBLE. DuckDB widens both
UNION ALL branches to DOUBLE, and that mismatch drifted ``sqrt_sample_size``
by ~2-3%. Casting the prior numerators to ``INT`` keeps both branches the
same type and the downstream SUM integer-clean.
"""

from __future__ import annotations

ADVANCED_PARK_FACTOR_STATS: list[str] = [
    "plate_appearances",
    "singles",
    "doubles",
    "triples",
    "home_runs",
    "strikeouts",
    "walks",
    "batting_outs",
    "runs",
    "balls_in_play",
    "trajectory_fly_ball",
    "trajectory_ground_ball",
    "trajectory_line_drive",
    "trajectory_pop_up",
    "trajectory_unknown",
    "batted_distance_infield",
    "batted_distance_outfield",
    "batted_distance_unknown",
    "batted_angle_left",
    "batted_angle_right",
    "batted_angle_middle",
]

ADVANCED_PARK_FACTOR_RATE_STATS: list[str] = ADVANCED_PARK_FACTOR_STATS[1:]


def build_advanced_park_factor_sql() -> str:
    """Return the 7-CTE SQL body for ``calc_park_factors_advanced``."""
    indent = "        "
    stats = ADVANCED_PARK_FACTOR_STATS
    rates = ADVANCED_PARK_FACTOR_RATE_STATS

    batting_agg_sums = (",\n" + indent).join(
        f"SUM(batting.{s})::INT AS {s}" for s in stats
    )
    multi_year_sums = (",\n" + indent).join(
        f"SUM({s}) OVER (PARTITION BY park_id, batter_id, pitcher_id, league ORDER BY season RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)::INT AS {s}"
        for s in stats
    )
    avg_rates = (",\n" + indent).join(
        f"SUM({s}) / SUM(plate_appearances) AS avg_{s}_per_pa" for s in rates
    )
    # ``averages.avg_<s>_per_pa * 1000::SMALLINT`` evaluates to DOUBLE.
    # Keep that exact shape so prior numerators retain fractional precision:
    # the multi_year_range branch is INT, but UNION widens to DOUBLE, which
    # is what flows into the downstream SUM/SQRT pipeline.
    prior_rate_cols = (",\n" + indent).join(
        f"averages.avg_{s}_per_pa * 1000::SMALLINT AS {s}" for s in rates
    )
    self_this = (",\n" + indent).join(f"this.{s} AS this_{s}" for s in stats)
    self_other = (",\n" + indent).join(f"other.{s} AS other_{s}" for s in stats)
    rate_calc_this = (",\n" + indent).join(
        f"this_{s} / this_plate_appearances AS this_{s}_per_pa" for s in rates
    )
    rate_calc_other = (",\n" + indent).join(
        f"other_{s} / other_plate_appearances AS other_{s}_per_pa" for s in rates
    )
    weighted_avg_this = (",\n" + indent).join(
        f"SUM(this_{s}_per_pa * sample_weight) / SUM(sample_weight) AS avg_this_{s}_per_pa"
        for s in rates
    )
    weighted_avg_other = (",\n" + indent).join(
        f"SUM(other_{s}_per_pa * sample_weight) / SUM(sample_weight) AS avg_other_{s}_per_pa"
        for s in rates
    )
    weighted_this_odds = (",\n" + indent).join(
        f"avg_this_{s}_per_pa / (1 - avg_this_{s}_per_pa) AS this_{s}_odds"
        for s in rates
    )
    weighted_other_odds = (",\n" + indent).join(
        f"avg_other_{s}_per_pa / (1 - avg_other_{s}_per_pa) AS other_{s}_odds"
        for s in rates
    )
    weighted_park_factor = (",\n" + indent).join(
        f"this_{s}_odds / other_{s}_odds AS {s}_park_factor" for s in rates
    )
    final_rounds = (",\n" + indent).join(
        f"ROUND({s}_park_factor, 2) AS {s}_park_factor" for s in rates
    )

    return f"""
WITH unique_park_seasons AS (
    SELECT
        park_id,
        season,
        home_league AS league
    FROM main_models.game_start_info
    WHERE game_type = 'RegularSeason'
    GROUP BY 1, 2, 3
    HAVING COUNT(*) > 25
),

batting_agg AS (
    SELECT
        states.park_id,
        states.season,
        states.league,
        states.batter_id,
        states.pitcher_id,
        {batting_agg_sums}
    FROM main_models.event_states_full AS states
    INNER JOIN main_models.event_offense_stats AS batting USING (event_key)
    INNER JOIN unique_park_seasons USING (season, league, park_id)
    WHERE states.game_type = 'RegularSeason'
        AND NOT states.is_interleague
    GROUP BY 1, 2, 3, 4, 5
),

multi_year_range AS MATERIALIZED (
    SELECT
        park_id,
        season,
        league,
        batter_id,
        pitcher_id,
        {multi_year_sums}
    FROM batting_agg
),

averages AS MATERIALIZED (
    SELECT
        season,
        league,
        {avg_rates}
    FROM multi_year_range
    GROUP BY 1, 2
),

with_priors AS (
    SELECT *
    FROM multi_year_range
    UNION ALL
    SELECT
        unique_park_seasons.park_id,
        season,
        league,
        'MARK' AS batter_id,
        'PRIOR' AS pitcher_id,
        1000::SMALLINT AS plate_appearances,
        {prior_rate_cols}
    FROM averages
    INNER JOIN unique_park_seasons USING (season, league)
),

self_joined AS (
    SELECT
        this.park_id AS this_park_id,
        other.park_id AS other_park_id,
        this.season,
        this.league,
        this.batter_id,
        this.pitcher_id,
        {self_this},
        {self_other},
        SQRT(LEAST(this_plate_appearances, other_plate_appearances)) AS sample_size,
        SUM(sample_size) OVER (PARTITION BY this.park_id, other.park_id, this.season, this.league) AS sum_sample_size
    FROM with_priors AS this
    INNER JOIN with_priors AS other
        ON this.park_id != other.park_id
            AND this.season = other.season
            AND this.batter_id = other.batter_id
            AND this.pitcher_id = other.pitcher_id
),

rate_calculation AS (
    SELECT
        *,
        {rate_calc_this},
        {rate_calc_other},
        MAX(sum_sample_size) OVER (PARTITION BY this_park_id, season, league) AS scaling_factor,
        sample_size * (scaling_factor / sum_sample_size) AS sample_weight
    FROM self_joined
),

weighted_average AS (
    SELECT
        this_park_id AS park_id,
        season,
        league,
        SUM(sample_size) AS sqrt_sample_size,
        {weighted_avg_this},
        {weighted_avg_other},
        {weighted_this_odds},
        {weighted_other_odds},
        {weighted_park_factor}
    FROM rate_calculation
    GROUP BY 1, 2, 3
),

final AS (
    SELECT
        park_id,
        season,
        league,
        ROUND(sqrt_sample_size, 0) AS sqrt_sample_size,
        {final_rounds}
    FROM weighted_average
)

SELECT * FROM final
"""
