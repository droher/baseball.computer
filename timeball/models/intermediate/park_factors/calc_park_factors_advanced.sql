{{
  config(
    materialized = 'table',
    )
}}
{% set stats = ["plate_appearances", "singles", "doubles", "triples", 
                "home_runs", "strikeouts", "walks", "batting_outs", "runs", "balls_in_play",
                "contact_type_fly_ball", "contact_type_ground_ball", "contact_type_line_drive", "contact_type_pop_fly",
                "contact_type_unknown", "batted_distance_infield", "batted_distance_outfield",
                "batted_distance_unknown", "batted_angle_left", "batted_angle_right", "batted_angle_middle"] %}
{% set rate_stats = stats[1:] %}
{% set prior_sample_size = "1000::SMALLINT" %}

WITH unique_park_seasons AS (
    SELECT
        park_id,
        season,
        home_league AS league
    FROM {{ ref('game_start_info') }}
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
        {%- for stat in stats %}
            SUM(batting.{{ stat }})::INT AS {{ stat }},
        {%- endfor %}
    FROM {{ ref('event_states_full') }} AS states
    INNER JOIN {{ ref('event_offense_stats') }} AS batting USING (event_key)
    -- Restrict to parks with decent sample
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
        {%- for stat in stats %}
            SUM({{ stat }})
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS {{ stat }},
        {%- endfor %}
    FROM batting_agg
),

averages AS MATERIALIZED (
    SELECT
        season,
        league,
        {%- for stat in rate_stats %}
            SUM({{ stat }}) / SUM(plate_appearances) AS avg_{{ stat }}_per_pa,
        {%- endfor %}
    FROM multi_year_range
    GROUP BY 1, 2
),

-- Give each park pair a batter-pitcher matchup at the league average
-- with {{ prior_sample_size }} PA per park
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
        {{ prior_sample_size }} AS plate_appearances,
        {%- for stat in rate_stats %}
            averages.avg_{{ stat }}_per_pa * {{ prior_sample_size }} AS {{ stat }},
        {%- endfor %}
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
        {%- for stat in stats %}
            this.{{ stat }} AS this_{{ stat }},
            other.{{ stat }} AS other_{{ stat }},
        {%- endfor %}
        SQRT(LEAST(this_plate_appearances, other_plate_appearances)) AS sample_size,
        SUM(sample_size) OVER (PARTITION BY this.park_id, other.park_id, this.season, this.league) AS sum_sample_size,
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
        {%- for stat in rate_stats %}
            this_{{ stat }} / this_plate_appearances AS this_{{ stat }}_per_pa,
            other_{{ stat }} / other_plate_appearances AS other_{{ stat }}_per_pa,
        {%- endfor %}
        -- Find the park pair with the highest sample size, and upweight all other pairs to match
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
        {%- for stat in rate_stats %}
            SUM(this_{{ stat }}_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_{{ stat }}_per_pa,
            SUM(other_{{ stat }}_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_{{ stat }}_per_pa,
            avg_this_{{ stat }}_per_pa
            / (1 - avg_this_{{ stat }}_per_pa) AS this_{{ stat }}_odds,
            avg_other_{{ stat }}_per_pa
            / (1 - avg_other_{{ stat }}_per_pa) AS other_{{ stat }}_odds,
            this_{{ stat }}_odds
            / other_{{ stat }}_odds AS {{ stat }}_park_factor,
        {%- endfor %}
    FROM rate_calculation
    GROUP BY 1, 2, 3
),

final AS (
    SELECT
        park_id,
        season,
        league,
        ROUND(sqrt_sample_size, 0) AS sqrt_sample_size,
        {%- for stat in rate_stats %}
            ROUND({{ stat }}_park_factor, 2) AS {{ stat }}_park_factor,
        {%- endfor %}
    FROM weighted_average
)

SELECT * FROM final
