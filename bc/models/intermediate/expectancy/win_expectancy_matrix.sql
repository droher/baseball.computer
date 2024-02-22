{{ 
    config(
        materialized = 'table',
    )
}}
WITH states AS (
    SELECT
        -- Treat 9th and later as the same to increase sample size
        -- TODO: Put add upstream `truncated_inning` col
        win_expectancy_start_key AS win_expectancy_key,
        inning_group_start AS inning,
        truncated_home_margin_start AS truncated_home_margin,
        frame_start AS frame,
        outs_start AS outs,
        base_state_start AS base_state,
        -- Buckets for merging low-sample-size states, see next query for integration
        ROUND(CASE
            WHEN home_margin_start = 0 THEN 0
            WHEN home_margin_start BETWEEN 1 AND 3 THEN 1
            WHEN home_margin_start BETWEEN 4 AND 6 THEN 2
            WHEN home_margin_start >= 7 THEN 3
            WHEN home_margin_start BETWEEN -3 AND -1 THEN -1
            WHEN home_margin_start BETWEEN -6 AND -4 THEN -2
            ELSE -3
        END, 0) AS home_margin_bucket,
        base_state_start > 0 AS any_runners_on,
        LAST(score_home_end::INT - score_away_end > 0) OVER rest_of_game AS home_team_win
    FROM {{ ref('event_states_full') }}
    -- Exclude the rare cases where the home team bats first
    -- TODO: Should also exclude/differentiate any bullshit extra innings runner on 2nd stuff
    WHERE bat_first_side = 'Away'
    WINDOW
        rest_of_game AS (
            PARTITION BY game_id
            ORDER BY event_id
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )
    -- Eliminate ties or games that lasted under 9 innings
    QUALIFY LAST(score_away_end::INT - score_home_end !=0) OVER rest_of_game
        AND LAST(inning::INT) OVER rest_of_game >= 9

),

agg AS (
    SELECT DISTINCT ON (win_expectancy_key)
        win_expectancy_key,
        inning,
        frame,
        truncated_home_margin,
        outs,
        base_state,
        SUM(home_team_win::NUMERIC) OVER narrow AS home_team_wins_narrow,
        COUNT(*) OVER narrow AS sample_size_narrow,
        SUM(home_team_win::NUMERIC) OVER broad / COUNT(*) OVER broad AS win_rate_broad,
    FROM states
    WINDOW
        narrow AS (
            PARTITION BY win_expectancy_key
        ),
        broad AS (
            PARTITION BY inning, frame, home_margin_bucket, any_runners_on
        )
),

final AS (
    SELECT
        win_expectancy_key,
        inning,
        frame,
        truncated_home_margin,
        outs,
        base_state,
        -- Use the "broad" category as a prior to smooth out rare states
        ROUND(
            (home_team_wins_narrow + win_rate_broad * 10) / (sample_size_narrow + 10), 3
        )::DECIMAL(4, 3) AS home_win_rate,
    FROM agg
)

SELECT * FROM final
ORDER BY home_win_rate DESC
