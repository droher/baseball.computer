{{ 
    config(
        materialized = 'table',
    )
}}
WITH final AS (
    SELECT
        run_expectancy_start_key AS run_expectancy_key,
        league_group,
        season_group,
        outs_start,
        base_state_start,
        SUM(runs_on_play) OVER rest_of_inning AS runs_scored,
    FROM {{ ref('event_states_full') }}
    WHERE game_type = 'RegularSeason'
        -- Final/extra innings have atypical expectencies
        AND inning_start < 9
    WINDOW
        rest_of_inning AS (
            PARTITION BY game_id, frame_start, inning_start
            ORDER BY event_id
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )
    QUALIFY NOT BOOL_OR(truncated_frame_flag) OVER rest_of_inning
)

SELECT
    run_expectancy_key,
    ANY_VALUE(league_group) AS league_group,
    ANY_VALUE(season_group) AS season_group,
    ANY_VALUE(outs_start) AS outs,
    ANY_VALUE(base_state_start) AS base_state,
    ROUND(AVG(runs_scored), 2)::DECIMAL AS avg_runs_scored,
    ROUND(VAR_SAMP(runs_scored), 2)::DECIMAL AS variance_runs_scored,
    COUNT(*) AS sample_size
FROM final
GROUP BY 1
