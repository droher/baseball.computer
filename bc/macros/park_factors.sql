{% macro batter_pitcher_park_factor(
    rate_stats,
    denominator_stat,
    prior_sample_size=1000,
    prev_years=2,
    filter_exp="1=1",
    batter_hand_split=False,
    use_odds=True
) %}
{% set stats = [denominator_stat] + rate_stats %}
{% set hand_partition = ", batter_hand" if batter_hand_split else "" %}
{% set hand_select = "batter_hand," if batter_hand_split else "" %}
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

    lines AS (
        SELECT *
        FROM {{ ref('event_offense_stats') }}
        WHERE {{ filter_exp }}
    ),

    lines_agg AS (
        SELECT
            states.park_id,
            states.season,
            states.league,
            states.batter_id,
            states.pitcher_id,
            ANY_VALUE(states.batter_hand) AS batter_hand,
            {%- for stat in stats %}
                SUM(lines.{{ stat }})::INT AS {{ stat }},
            {%- endfor %}
        FROM {{ ref('event_states_full') }} AS states
        INNER JOIN lines USING (event_key)
        -- Restrict to parks with decent sample
        INNER JOIN unique_park_seasons USING (season, league, park_id)
        WHERE states.game_type = 'RegularSeason'
            AND NOT states.is_interleague
            {% if batter_hand_split %}
                AND states.batter_hand IN ('L', 'R')
            {% endif %}
        GROUP BY 1, 2, 3, 4, 5
    ),

    multi_year_range AS MATERIALIZED (
        SELECT
            la.park_id,
            ups.season,
            la.league,
            la.batter_id,
            la.pitcher_id,
            {{ hand_select }}
            {%- for stat in stats %}
                SUM(la.{{ stat }})::INT AS {{ stat }},
            {%- endfor %}
        FROM lines_agg AS la
        INNER JOIN unique_park_seasons AS ups
            ON la.park_id = ups.park_id
                AND la.league = ups.league
                AND la.season BETWEEN ups.season - {{ prev_years }} AND ups.season
        GROUP BY 1, 2, 3, 4, 5{{ ", 6" if batter_hand_split else "" }}
    ),

    averages AS MATERIALIZED (
        SELECT
            season,
            league,
            {{ hand_select }}
            {%- for stat in rate_stats %}
                SUM({{ stat }}) / SUM({{ denominator_stat }}) AS {{ stat }}_rate,
            {%- endfor %}
        FROM multi_year_range
        GROUP BY 1, 2{{ ", 3" if batter_hand_split else "" }}
    ),

    -- Give each park pair a batter-pitcher matchup at the league average
    -- with {{ prior_sample_size }} PA per park
    with_priors AS (
        SELECT *
        FROM multi_year_range
        UNION ALL BY NAME
        SELECT
            unique_park_seasons.park_id,
            season,
            league,
            'MARK' AS batter_id,
            'PRIOR' AS pitcher_id,
            {{ hand_select }}
            {{ prior_sample_size }} AS {{ denominator_stat }},
            {%- for stat in rate_stats %}
                averages.{{ stat }}_rate * {{ prior_sample_size }} AS {{ stat }},
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
            {{ "this.batter_hand," if batter_hand_split else "" }}
            {%- for stat in stats %}
                this.{{ stat }} AS this_{{ stat }},
                other.{{ stat }} AS other_{{ stat }},
            {%- endfor %}
            SQRT(LEAST(this_{{ denominator_stat }}, other_{{ denominator_stat }})) AS sample_size,
            SUM(sample_size) OVER (PARTITION BY this.park_id, other.park_id, this.season, this.league) AS sum_sample_size,
        FROM with_priors AS this
        INNER JOIN with_priors AS other
            ON this.park_id != other.park_id
                AND this.season = other.season
                AND this.batter_id = other.batter_id
                AND this.pitcher_id = other.pitcher_id
                {% if batter_hand_split %}
                    AND this.batter_hand = other.batter_hand
                {% endif %}
    ),

    rate_calculation AS (
        SELECT
            *,
            {%- for stat in rate_stats %}
                this_{{ stat }} / this_{{ denominator_stat }} AS this_{{ stat }}_rate,
                other_{{ stat }} / other_{{ denominator_stat }} AS other_{{ stat }}_rate,
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
            {{ hand_select }}
            SUM(sample_size) AS sqrt_sample_size,
            {%- for stat in rate_stats %}
                SUM(this_{{ stat }}_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_{{ stat }}_rate,
                SUM(other_{{ stat }}_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_{{ stat }}_rate,
                avg_this_{{ stat }}_rate
                / (1 - avg_this_{{ stat }}_rate) AS this_{{ stat }}_odds,
                avg_other_{{ stat }}_rate
                / (1 - avg_other_{{ stat }}_rate) AS other_{{ stat }}_odds,
                this_{{ stat }}_odds
                / other_{{ stat }}_odds AS {{ stat }}_odds_park_factor,
                avg_this_{{ stat }}_rate / avg_other_{{ stat }}_rate AS {{ stat }}_rate_park_factor,
            {%- endfor %}
        FROM rate_calculation
        GROUP BY 1, 2, 3{{ ", 4" if batter_hand_split else "" }}
    ),

    final AS (
        SELECT
            park_id,
            season,
            league,
            {{ hand_select }}
            ROUND(sqrt_sample_size, 0) AS sqrt_sample_size,
            {%- for stat in rate_stats %}
                {% if use_odds %}
                    ROUND({{ stat }}_odds_park_factor, 2) AS {{ stat }}_park_factor,
                {% else %}
                    ROUND({{ stat }}_rate_park_factor, 2) AS {{ stat }}_park_factor,
                {% endif %}
            {%- endfor %}
        FROM weighted_average
    )

    SELECT * FROM final
{% endmacro %}