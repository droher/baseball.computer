WITH states AS (
    SELECT
        event_key,
        season,
        league,
        inning_start,
        frame_start,
        truncated_home_margin_start,
        base_state_start,
        outs_start,
        inning_end,
        frame_end,
        truncated_home_margin_end,
        base_state_end,
        outs_end,
        runs_on_play,
        game_end_flag
    FROM {{ ref('event_states_full') }}
    WINDOW rest_of_inning AS (
        PARTITION BY game_id, frame, inning
        ORDER BY event_id
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    )

)

SELECT
    states.event_key,
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
    ) AS expected_home_win_change
FROM states
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
        AND runs_end.base_state = states.base_state_end
LEFT JOIN {{ ref('win_expectancy_matrix' )}} AS wins_start
    ON wins_start.inning = states.inning_start
        AND wins_start.frame = states.frame_start
        AND wins_start.truncated_home_margin = states.truncated_home_margin_start
        AND wins_start.outs = states.outs_start
        AND wins_start.base_state = states.base_state_start
LEFT JOIN {{ ref('win_expectancy_matrix' )}} AS wins_end
    ON wins_end.inning = states.inning_end
        AND wins_end.frame = states.frame_end
        AND wins_end.truncated_home_margin = states.truncated_home_margin_end
        AND wins_end.outs = states.outs_end
        AND wins_end.base_state = states.base_state_end