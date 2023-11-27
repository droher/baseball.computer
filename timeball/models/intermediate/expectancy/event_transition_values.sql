{{
  config(
    materialized = 'table',
    )
}}
WITH final AS (
    SELECT
        states.event_key,
        states.run_expectancy_start_key,
        states.run_expectancy_end_key,
        states.win_expectancy_start_key,
        states.win_expectancy_end_key,
        states.season,
        states.league,
        states.game_type,
        states.inning_start,
        states.frame_start,
        states.truncated_home_margin_start,
        states.batting_side,
        states.base_state_start,
        states.outs_start,
        states.inning_end,
        states.frame_end,
        states.truncated_home_margin_end,
        states.base_state_end,
        states.outs_end,
        states.runs_on_play,
        states.game_end_flag,
        ROUND(
            states.runs_on_play
            + COALESCE(runs_end.avg_runs_scored, 0)
            - runs_start.avg_runs_scored,
            3
        ) AS expected_runs_change,
        ROUND(
            CASE
                WHEN states.game_end_flag AND states.truncated_home_margin_end > 0
                    THEN 1 - wins_start.home_win_rate
                WHEN states.game_end_flag AND states.truncated_home_margin_end < 0
                    THEN 0 - wins_start.home_win_rate
                WHEN states.game_end_flag
                    THEN NULL
                ELSE wins_end.home_win_rate - wins_start.home_win_rate
            END,
            3
        ) AS expected_home_win_change,
        CASE WHEN states.batting_side = 'Home' THEN expected_home_win_change
            ELSE -expected_home_win_change
        END AS expected_batting_win_change
    FROM {{ ref('event_states_full') }} AS states
    LEFT JOIN {{ ref('run_expectancy_matrix') }} AS runs_start
        ON runs_start.run_expectancy_key = states.run_expectancy_start_key
    LEFT JOIN {{ ref('run_expectancy_matrix') }} AS runs_end
        ON runs_end.run_expectancy_key = states.run_expectancy_end_key
    LEFT JOIN {{ ref('win_expectancy_matrix' ) }} AS wins_start
        ON wins_start.win_expectancy_key = states.win_expectancy_start_key
    LEFT JOIN {{ ref('win_expectancy_matrix' ) }} AS wins_end
        ON wins_end.win_expectancy_key = states.win_expectancy_end_key
    WHERE states.game_type = 'RegularSeason'
)

SELECT * FROM final
