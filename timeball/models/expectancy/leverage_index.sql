{{
  config(
    materialized = 'table',
    )
}}
WITH trans AS (
    SELECT
        LEAST(9, inning_start) AS inning_start,
        frame_start,
        truncated_home_margin_start,
        base_state_start,
        outs_start,
        LEAST(9, inning_end) AS inning_end,
        frame_end,
        truncated_home_margin_end,
        base_state_end,
        outs_end,
        game_end_flag,
        COUNT(*) AS sample_size,
        -- We should be able to take any value here 
        -- but avg just to be safe
        AVG(ABS(expected_home_win_change)) AS absolute_expected_home_win_change,
        AVG(ABS(expected_runs_change)) AS absolute_expected_runs_change,
    FROM {{ ref('event_transition_values') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
),

weighted AS (
    SELECT
        inning_start,
        frame_start,
        truncated_home_margin_start,
        base_state_start,
        outs_start,
        SUM(sample_size) AS agg_sample_size,
        -- Weighted averages
        SUM(absolute_expected_home_win_change * sample_size)
        / SUM(sample_size) AS win_leverage_unscaled,
        SUM(absolute_expected_runs_change * sample_size)
        / SUM(sample_size) AS run_leverage_unscaled,
        -- Note that this is a window function applied after the aggregation
        SUM(win_leverage_unscaled * agg_sample_size) OVER ()
        / SUM(agg_sample_size) OVER () AS avg_win_leverage_unscaled,
        SUM(run_leverage_unscaled * agg_sample_size) OVER ()
        / SUM(agg_sample_size) OVER () AS avg_run_leverage_unscaled,
    FROM trans
    GROUP BY 1, 2, 3, 4, 5
),

final AS (
    SELECT
        inning_start AS inning,
        frame_start AS frame,
        truncated_home_margin_start AS truncated_home_margin,
        base_state_start AS base_state,
        outs_start AS outs,
        ROUND(win_leverage_unscaled, 3)::DECIMAL AS win_leverage_unscaled,
        ROUND(run_leverage_unscaled, 3)::DECIMAL AS run_leverage_unscaled,
        ROUND(win_leverage_unscaled / avg_win_leverage_unscaled, 2)::DECIMAL AS win_leverage_index,
        ROUND(run_leverage_unscaled / avg_run_leverage_unscaled, 2)::DECIMAL AS run_leverage_index
    FROM weighted
)

SELECT * FROM final
