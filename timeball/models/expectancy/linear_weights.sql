{{
  config(
    materialized = 'table',
    )
}}
WITH agg AS (
    SELECT DISTINCT ON (states.season, states.league, pa.plate_appearance_result)
        states.season,
        states.league,
        pa.plate_appearance_result,
        AVG(trans.expected_runs_change) OVER all_league AS avg_run_value_all,
        AVG(trans.expected_runs_change) OVER result AS avg_run_value_result,
        AVG(trans.expected_batting_win_change) OVER all_league AS avg_win_value_all,
        AVG(trans.expected_batting_win_change) OVER result AS avg_win_value_result,
    FROM {{ ref('stg_event_plate_appearances') }} AS pa
    INNER JOIN {{ ref('event_states_full') }} AS states USING (event_key)
    INNER JOIN {{ ref('event_transition_values') }} AS trans USING (event_key)
    -- Only include plays without baserunning actions
    WHERE event_key NOT IN (SELECT event_key FROM {{ ref('stg_event_baserunning_plays') }})
        -- Only include seasons with regular season data for now
        AND states.season >= 1914
    WINDOW 
        all_league AS (PARTITION BY states.season, states.league),
        result AS (PARTITION BY states.season, states.league, pa.plate_appearance_result)
),

final AS (
    SELECT
        season,
        league,
        plate_appearance_result,
        ROUND(avg_run_value_result - avg_run_value_all, 3) AS avg_run_value,
        ROUND(avg_win_value_result - avg_win_value_all, 3) AS avg_win_value
    FROM agg
)

SELECT * FROM final
