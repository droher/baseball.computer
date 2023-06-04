{{
  config(
    materialized = 'table',
    )
}}
WITH advance_agg AS (
    SELECT
        event_key,
        COUNT(*)::UTINYINT AS runs
    FROM {{ ref('stg_event_baserunning_advance_attempts') }}
    WHERE is_successful
        AND attempted_advance_to = 'Home'
    GROUP BY 1
),

windowed AS (
    SELECT
        e.event_key,
        SUM(a.runs) FILTER (WHERE e.batting_side = 'Home') OVER start_event AS score_home_start,
        SUM(a.runs) FILTER (WHERE e.batting_side = 'Away') OVER start_event AS score_away_start,
        SUM(a.runs) FILTER (WHERE e.batting_side = 'Home') OVER end_event AS score_home_end,
        SUM(a.runs) FILTER (WHERE e.batting_side = 'Away') OVER end_event AS score_away_end,
    FROM {{ ref('stg_events') }} AS e
    LEFT JOIN advance_agg AS a USING (event_key)
    WINDOW
        start_event AS (
            PARTITION BY e.game_id
            ORDER BY e.event_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        end_event AS (
            PARTITION BY e.game_id
            ORDER BY e.event_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),

final AS (
    SELECT
        event_key,
        COALESCE(score_home_start, 0)::UTINYINT AS score_home_start,
        COALESCE(score_away_end, 0)::UTINYINT AS score_away_start,
        COALESCE(score_home_end, 0)::UTINYINT AS score_home_end,
        COALESCE(score_away_end, 0)::UTINYINT AS score_away_end
    FROM windowed
)

SELECT * FROM final
