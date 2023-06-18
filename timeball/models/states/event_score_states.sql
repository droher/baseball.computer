{{
  config(
    materialized = 'table',
    )
}}
WITH runs AS (
    SELECT
        event_key,
        COUNT(*)::UTINYINT AS runs
    FROM {{ ref('stg_event_runs') }}
    GROUP BY 1
),

windowed AS (
    SELECT
        e.event_key,
        SUM(runs.runs) FILTER (WHERE e.batting_side = 'Home') OVER start_event AS score_home_start,
        SUM(runs.runs) FILTER (WHERE e.batting_side = 'Away') OVER start_event AS score_away_start,
        SUM(runs.runs) FILTER (WHERE e.batting_side = 'Home') OVER end_event AS score_home_end,
        SUM(runs.runs) FILTER (WHERE e.batting_side = 'Away') OVER end_event AS score_away_end,
        COALESCE(runs.runs, 0)::UTINYINT AS runs_on_play,
    FROM {{ ref('stg_events') }} AS e
    LEFT JOIN runs USING (event_key)
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
        COALESCE(score_away_end, 0)::UTINYINT AS score_away_end,
        runs_on_play
    FROM windowed
)

SELECT * FROM final
