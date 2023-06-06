{{ 
    config(
        materialized = 'table',
    )
}}
WITH states AS (
    SELECT
        -- Treat 9th and later as the same to increase sample size 
        LEAST(inning_start, 9) AS inning,
        frame_start AS frame,
        truncated_home_margin,
        base_state_start AS base_state,
        outs_start AS outs,
        -- Buckets for merging low-sample-size states, see next query for integration
        ROUND(CASE
            WHEN home_margin = 0 THEN 0
            WHEN home_margin BETWEEN 1 AND 3 THEN 1
            WHEN home_margin BETWEEN 4 AND 6 THEN 2
            WHEN home_margin >= 7 THEN 3
            WHEN home_margin BETWEEN -3 AND -1 THEN -1
            WHEN home_margin BETWEEN -6 AND -4 THEN -2
            ELSE -3
        END, 0) AS home_margin_bucket,
        base_state_start > 0 AS any_runners_on,
        LAST(score_home_end::INT - score_away_end > 0) OVER rest_of_game AS home_team_win
    FROM {{ ref('event_states_full') }}
    -- Exclude the rare cases where the home team bats first
    WHERE bat_first_side = 'Away'
    WINDOW
        rest_of_game AS (
            PARTITION BY game_id
            ORDER BY event_id
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )
    -- Eliminate ties or games that lasted under 9 innings
    QUALIFY LAST(score_away_end::INT - score_home_end !=0) OVER rest_of_game
        AND LAST(inning) OVER rest_of_game >= 9

),

agg AS (
    SELECT DISTINCT ON (inning, frame, truncated_home_margin, base_state, outs)
        inning,
        frame,
        truncated_home_margin,
        base_state,
        outs,
        SUM(home_team_win::NUMERIC) OVER narrow AS home_team_wins_narrow,
        COUNT(*) OVER narrow AS sample_size_narrow,
        SUM(home_team_win::NUMERIC) OVER broad / COUNT(*) OVER broad AS win_rate_broad,
    FROM states
    WINDOW
        narrow AS (
            PARTITION BY inning, frame, truncated_home_margin, base_state, outs
        ),
        broad AS (
            PARTITION BY inning, frame, home_margin_bucket, any_runners_on
        )
),

final AS (
    SELECT
        inning,
        frame,
        truncated_home_margin,
        base_state,
        outs,
        -- Use the "broad" category as a prior to smooth out rare states
        ROUND(
            (home_team_wins_narrow + win_rate_broad * 10) / (sample_size_narrow + 10), 3
        ) AS home_win_rate,
    FROM agg
)

SELECT * FROM final
