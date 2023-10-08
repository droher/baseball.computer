{{
  config(
    materialized = 'table',
    )
}}
WITH final AS (
    SELECT
        states.event_key,
        states.season,
        states.league,
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
    -- TODO: Add hashes to full_state table to make these joins not awful
    LEFT JOIN {{ ref('run_expectancy_matrix') }} AS runs_start
        ON runs_start.season = states.season
            AND runs_start.league = states.league
            AND runs_start.outs = states.outs_start
            AND runs_start.base_state = states.base_state_start
    LEFT JOIN {{ ref('run_expectancy_matrix') }} AS runs_end
        ON runs_end.season = states.season
            AND runs_end.league = states.league
            AND runs_end.outs = states.outs_end
            AND runs_end.base_state = COALESCE(states.base_state_end, 0)
    LEFT JOIN {{ ref('win_expectancy_matrix' ) }} AS wins_start
        ON wins_start.inning = LEAST(states.inning_start, 9)
            AND wins_start.frame = states.frame_start
            AND wins_start.truncated_home_margin = states.truncated_home_margin_start
            AND wins_start.outs = states.outs_start
            AND wins_start.base_state = states.base_state_start
    LEFT JOIN {{ ref('win_expectancy_matrix' ) }} AS wins_end
        ON wins_end.inning = LEAST(states.inning_end, 9)
            AND wins_end.frame = states.frame_end
            AND wins_end.truncated_home_margin = states.truncated_home_margin_end
            AND wins_end.outs = states.outs_end % 3
            AND wins_end.base_state = COALESCE(states.base_state_end, 0)
)

SELECT * FROM final
