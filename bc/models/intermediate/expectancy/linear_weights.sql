{{
  config(
    materialized = 'table',
    )
}}
WITH valid_leagues AS (
    SELECT
        season,
        league,
        AVG(runs_scored) AS average_runs_scored,
    FROM {{ ref('team_game_results') }}
    GROUP BY 1, 2
),

union_plays AS (
    SELECT
        e.event_key,
        CASE WHEN cat.result_category = 'InPlayOut' AND e.outs_on_play > 1
                THEN 'DoublePlay'
            ELSE cat.result_category
        END AS play,
        'BATTING' AS play_category,
    FROM {{ ref('stg_events') }} AS e
    INNER JOIN {{ ref('seed_plate_appearance_result_types') }} AS cat USING (plate_appearance_result)
    -- Only include plays that didn't have simultaneous baserunning plays,
    -- or plays with an atypical number of outs recorded for its type (e.g. single with an out)
    WHERE e.event_key NOT IN (
        SELECT event_key FROM {{ ref('stg_event_baserunners') }} WHERE baserunning_play_type IS NOT NULL
    )
    UNION ALL BY NAME
    -- Only consider baserunning plays with a single event for now.
    -- We can still handle these cases downstream by assigning the value
    -- to each item of the event, e.g. 2xSB for a double steal, which is probably
    -- wrong but not too far off.
    SELECT
        e.event_key,
        FIRST(CASE WHEN e.is_out THEN cat.result_category_out ELSE cat.result_category_safe END) AS play,
        FIRST('BASERUNNING') AS play_category,
    FROM {{ ref('stg_event_baserunners') }} AS e
    INNER JOIN {{ ref('seed_baserunning_play_types') }} AS cat USING (baserunning_play_type)
    WHERE e.event_key NOT IN (
            SELECT event_key FROM {{ ref('stg_events') }} WHERE plate_appearance_result IS NOT NULL
        )
    GROUP BY 1
    HAVING COUNT(*) = 1
),

joined AS (
    SELECT
        trans.season,
        trans.league,
        union_plays.play,
        union_plays.play_category,
        trans.expected_runs_change,
        trans.expected_batting_win_change
    FROM union_plays
    INNER JOIN {{ ref('event_transition_values') }} AS trans USING (event_key)
),

agg_specific AS (
    SELECT DISTINCT ON (season, league, play)
        season,
        league,
        play,
        play_category,
        AVG(expected_runs_change) OVER all_league AS average_run_value_all,
        AVG(expected_runs_change) OVER result AS average_run_value_result,
        STDDEV_SAMP(expected_runs_change) OVER result AS std_dev_run_value_result,
        AVG(expected_batting_win_change) OVER all_league AS average_win_value_all,
        AVG(expected_batting_win_change) OVER result AS average_win_value_result,
        STDDEV_SAMP(expected_batting_win_change) OVER result AS std_dev_win_value_result,
        FALSE AS is_imputed
    FROM joined
    WINDOW
        all_league AS (PARTITION BY season, league),
        result AS (PARTITION BY season, league, play)
    QUALIFY COUNT(*) OVER result > 100
),

generic_values AS (
    SELECT DISTINCT ON (play)
        play,
        play_category,
        AVG(expected_runs_change) OVER () AS average_run_value_all,
        AVG(expected_runs_change) OVER result AS average_run_value_result,
        STDDEV_SAMP(expected_runs_change) OVER result AS std_dev_run_value_result,
        AVG(expected_batting_win_change) OVER () AS average_win_value_all,
        AVG(expected_batting_win_change) OVER result AS average_win_value_result,
        STDDEV_SAMP(expected_batting_win_change) OVER result AS std_dev_win_value_result,
    FROM joined
    WINDOW result AS (PARTITION BY play)
),

imputed AS (
    SELECT
        valid_leagues.*,
        generic_values.*,
        TRUE AS is_imputed
    FROM generic_values
    CROSS JOIN valid_leagues
    LEFT JOIN agg_specific USING (season, league, play)
    WHERE agg_specific.season IS NULL
),

unioned AS (
    SELECT * FROM agg_specific
    UNION ALL BY NAME
    SELECT * FROM imputed
),

final AS (
    SELECT
        season,
        COALESCE(league, 'N/A') AS league,
        play,
        play_category,
        ROUND(average_run_value_result - average_run_value_all, 3) AS average_run_value,
        ROUND(average_win_value_result - average_win_value_all, 3) AS average_win_value,
        average_run_value_result
            - FIRST(CASE WHEN play = 'InPlayOut' THEN average_run_value_result END IGNORE NULLS) OVER w
        AS relative_run_value,
        ROUND(std_dev_run_value_result, 3) AS std_dev_run_value,
        ROUND(std_dev_win_value_result, 3) AS std_dev_win_value,
        is_imputed
    FROM unioned
    WINDOW w AS (PARTITION BY season, league)
)

SELECT * FROM final
