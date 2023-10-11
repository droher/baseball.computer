{{
  config(
    materialized = 'table',
    )
}}
WITH initial_sum AS (
    SELECT
        game_id,
        team_id,
        {% for stat in event_level_offense_stats() -%}
            {% set dtype = "INT1" if stat.startswith("surplus") else "USMALLINT" %}
            SUM({{ stat }})::{{ dtype }} AS {{ stat }},
        {% endfor %}
    FROM {{ ref('event_offense_stats') }}
    GROUP BY 1, 2
),

-- A few definitions change when aggregated at the team level
final AS (
    SELECT 
        * REPLACE (
            left_on_base_with_two_outs AS left_on_base,
    )
    FROM initial_sum
)

SELECT * FROM final
