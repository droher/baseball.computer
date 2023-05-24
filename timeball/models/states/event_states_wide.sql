{{
  config(
    materialized = 'table',
    )
}}
WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

lineups AS (
    SELECT *, hash(event_key) % 10 AS grp FROM {{ ref('event_lineup_states') }}
),

pivoter AS (
    {% for i in range(10) %}
    PIVOT (SELECT * FROM lineups WHERE grp = {{ i }})
    ON lineup_position IN (1, 2, 3, 4, 5, 6, 7, 8, 9)
    USING FIRST(player_id)
    GROUP BY event_key
    {{ 'UNION ALL' if not loop.last }}
    {% endfor %}
)

SELECT * FROM pivoter
