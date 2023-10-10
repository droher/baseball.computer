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
{% set prior_sample_size = "1000" %}

WITH unique_park_seasons AS (
    SELECT DISTINCT
        park_id,
        season
    FROM {{ ref('game_start_info') }}
    WHERE game_type = 'RegularSeason'
),

with_park_info AS (
    SELECT
        states.park_id,
        states.season,
        states.batter_id,
        states.pitcher_id,
        {%- for stat in stats %}
            batting.{{ stat }}::FLOAT AS {{ stat }},
        {%- endfor %}
    FROM {{ ref('event_states_full') }} AS states
    INNER JOIN {{ ref('event_offense_stats') }} AS batting USING (event_key)
    WHERE states.game_type = 'RegularSeason'
),

unioned AS (
    SELECT
        park_id,
        season,
        batter_id AS player_id,
        'BATTER' AS player_type,
        {%- for stat in stats %}
            {{ stat }},
        {%- endfor %}
    FROM with_park_info
    UNION ALL
    SELECT
        park_id,
        season,
        pitcher_id AS player_id,
        'PITCHER' AS player_type,
        {%- for stat in stats %}
            {{ stat }},
        {%- endfor %}
    FROM with_park_info
),

batting_agg AS (
    SELECT
        park_id,
        season,
        player_id,
        player_type,
        {%- for stat in stats %}
            SUM({{ stat }}) AS {{ stat }},
        {%- endfor %}
    FROM unioned
    GROUP BY 1, 2, 3, 4
),

multi_year_range AS (
    SELECT
        park_id,
        season,
        player_id,
        player_type,
        {%- for stat in stats %}
            SUM({{ stat }})
                OVER (
                    PARTITION BY park_id, player_id, player_type
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
                )
            AS {{ stat }},
        {%- endfor %}
    FROM batting_agg
),

averages AS (
    SELECT
        season,
        {%- for stat in rate_stats %}
            SUM({{ stat }}) / SUM(plate_appearances) AS avg_{{ stat }}_per_pa,
        {%- endfor %}
    FROM multi_year_range
    GROUP BY 1
),

with_priors AS (
    SELECT *
    FROM multi_year_range
    UNION ALL
    SELECT
        unique_park_seasons.park_id,
        season,
        'MARK' AS player_id,
        'PRIOR' AS player_type,
        {{ prior_sample_size }} AS plate_appearances,
        {%- for stat in rate_stats %}
            averages.avg_{{ stat }}_per_pa * {{ prior_sample_size }} AS {{ stat }},
        {%- endfor %}
    FROM averages
    INNER JOIN unique_park_seasons USING (season)
),

self_joined AS (
    SELECT
        this.park_id AS this_park_id,
        other.park_id AS other_park_id,
        this.season,
        this.player_id,
        this.player_type,
        {%- for stat in stats %}
            this.{{ stat }} AS this_{{ stat }},
            other.{{ stat }} AS other_{{ stat }},
        {%- endfor %}
    FROM with_priors AS this
    INNER JOIN with_priors AS other
        ON this.park_id != other.park_id
            AND this.season = other.season
            AND this.player_id = other.player_id
            AND this.player_type = other.player_type
),

rate_calculation AS (
    SELECT
        *,
        {%- for stat in rate_stats %}
            this_{{ stat }} / this_plate_appearances AS this_{{ stat }}_per_pa,
            other_{{ stat }} / other_plate_appearances AS other_{{ stat }}_per_pa,
        {%- endfor %}
        SQRT(LEAST(this_plate_appearances, other_plate_appearances)) AS sample_size
    FROM self_joined
),

weighted_average AS (
    SELECT
        this_park_id AS park_id,
        season,
        SUM(sample_size) AS sample_size,
        {%- for stat in rate_stats %}
            SUM(this_{{ stat }}_per_pa * sample_size)
            / SUM(sample_size) AS avg_this_{{ stat }}_per_pa,
            SUM(other_{{ stat }}_per_pa * sample_size)
            / SUM(sample_size) AS avg_other_{{ stat }}_per_pa,
            avg_this_{{ stat }}_per_pa
            / (1 - avg_this_{{ stat }}_per_pa) AS this_{{ stat }}_odds,
            avg_other_{{ stat }}_per_pa
            / (1 - avg_other_{{ stat }}_per_pa) AS other_{{ stat }}_odds,
            this_{{ stat }}_odds
            / other_{{ stat }}_odds AS {{ stat }}_park_factor,
        {%- endfor %}
    FROM rate_calculation
    GROUP BY 1, 2
),

final AS (
    SELECT
        park_id,
        season,
        ROUND(sample_size, 0) AS sqrt_sample_size,
        {%- for stat in rate_stats %}
            ROUND({{ stat }}_park_factor, 2) AS {{ stat }}_park_factor,
        {%- endfor %}
    FROM weighted_average
)

SELECT * FROM final
