{{ 
    config(
        materialized = 'table',
    )
}}
WITH final AS (
    SELECT
        season,
        league,
        base_state_start,
        outs_start,
        SUM(runs_on_play) OVER rest_of_inning AS runs_scored,
    FROM {{ ref('event_states_full') }}
    WHERE game_type = 'RegularSeason'
        AND inning_start < 9
    WINDOW rest_of_inning AS (
        PARTITION BY game_id, frame_start, inning_start
        ORDER BY event_id
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    )
    QUALIFY NOT BOOL_OR(truncated_frame_flag) OVER rest_of_inning

)

SELECT
    season,
    league,
    base_state_start AS base_state,
    outs_start AS outs,
    ROUND(AVG(runs_scored), 2) AS avg_runs_scored,
    COUNT(*) AS sample_size
FROM final
GROUP BY 1, 2, 3, 4
