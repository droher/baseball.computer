{{
  config(
    materialized = 'table',
    )
}}
WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

advances AS (
    SELECT * FROM {{ ref('stg_event_baserunning_advance_attempts') }}
),

events_both_sides AS (
    SELECT
        *,
        batting_side AS side,
        TRUE AS is_batting
    FROM events
    UNION ALL
    SELECT
        *,
        CASE WHEN batting_side = 'Home' THEN 'Away' ELSE 'Home' END AS side,
        FALSE AS is_batting
    FROM events
),

run_calc AS (
    SELECT
        e.event_key,
        e.side,
        COUNT(a.event_key) FILTER (WHERE e.is_batting)
            OVER (
                PARTITION BY e.game_id, e.side
                ORDER BY e.event_key
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
        AS score,
    FROM events_both_sides AS e
    LEFT JOIN advances AS a
        ON e.event_key = a.event_key
            AND a.attempted_advance_to = 'Home'
            AND a.is_successful
),

final AS (
    SELECT
        event_key,
        COALESCE(FIRST(score) FILTER (WHERE side = 'Home'), 0) AS score_home,
        COALESCE(FIRST(score) FILTER (WHERE side = 'Away'), 0) AS score_away
    FROM run_calc
    GROUP BY 1
)

SELECT * FROM final
