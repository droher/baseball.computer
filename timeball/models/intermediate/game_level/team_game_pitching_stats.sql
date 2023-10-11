{{
  config(
    materialized = 'table',
    )
}}
WITH initial_sum AS (
    SELECT
        game_id,
        team_id,
        {% for stat in event_level_pitching_stats() + game_level_pitching_stats() -%}
            {% set dtype = "INT1" if stat.startswith("surplus") else "USMALLINT" %}
            SUM({{ stat }})::{{ dtype }} AS {{ stat }},
        {% endfor %}
    FROM {{ ref('player_game_pitching_lines') }}
    GROUP BY 1, 2
),

-- A few definitions change when aggregated at the team level
final AS (
    SELECT
        * REPLACE (
            left_on_base_with_two_outs::UTINYINT AS left_on_base,
            -- Combined no-hitters and perfect games (latter hasn't happened yet)
            (hits = 0 AND outs_recorded >= 27)::UTINYINT AS no_hitters,
            (perfect_games = 1 OR outs_recorded >= 27 AND times_reached_base = 0)::UTINYINT AS perfect_games,
            -- Just to avoid weird rounding stuff
            ROUND(outs_recorded / 3, 4)::DECIMAL(6, 4) AS innings_pitched
    )
    FROM initial_sum

)

SELECT * FROM final
