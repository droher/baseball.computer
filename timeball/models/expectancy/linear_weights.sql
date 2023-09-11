{{
  config(
    materialized = 'table',
    )
}}
-- TODO: Should these also be grouped by outs on play?
-- For example, `OtherAdvance` usually refers to a runner out on an advance attempt,
-- but sometimes the advance is successful (esp. as the result of an error),
-- which is probably about a full run's worth of difference.
-- Instead of adding a col we could always create synthetic event types for these cases,
-- specifically OtherAdvance (-> OtherAdvanceSafe + OtherAdvanceOut)
-- and InPlayOut (-> InPlayOut, DoublePlay, TriplePlay).
WITH union_plays AS (
    SELECT
        event_key,
        plate_appearance_result AS play,
        'BATTING' AS play_category,
    FROM {{ ref('stg_events') }}
    WHERE plate_appearance_result IS NOT NULL
        AND event_key NOT IN (SELECT event_key FROM {{ ref('stg_event_baserunners') }} WHERE baserunning_play_type IS NOT NULL)
    UNION ALL BY NAME
    -- Only consider baserunning plays with a single event for now.
    -- We can still handle these cases downstream by assigning the value
    -- to each item of the event, e.g. 2xSB for a double steal, which is probably
    -- wrong but not too far off.
    SELECT
        event_key,
        FIRST(baserunning_play_type) AS play,
        FIRST('BASERUNNING') AS play_category,
    FROM {{ ref('stg_event_baserunners') }}
    WHERE event_key NOT IN (SELECT event_key FROM {{ ref('stg_events') }} WHERE plate_appearance_result IS NOT NULL)
    GROUP BY 1
    HAVING COUNT(*) = 1
),

agg AS (
    SELECT DISTINCT ON (trans.season, trans.league, union_plays.play)
        trans.season,
        trans.league,
        union_plays.play,
        union_plays.play_category,
        AVG(trans.expected_runs_change) OVER all_league AS avg_run_value_all,
        AVG(trans.expected_runs_change) OVER result AS avg_run_value_result,
        AVG(trans.expected_batting_win_change) OVER all_league AS avg_win_value_all,
        AVG(trans.expected_batting_win_change) OVER result AS avg_win_value_result,
    FROM union_plays
    INNER JOIN {{ ref('event_transition_values') }} AS trans USING (event_key)
    -- Only include seasons with regular season data for now
    WHERE trans.season >= 1914
    WINDOW
        all_league AS (PARTITION BY trans.season, trans.league),
        result AS (PARTITION BY trans.season, trans.league, union_plays.play)
),

final AS (
    SELECT
        season,
        league,
        play,
        play_category,
        ROUND(avg_run_value_result - avg_run_value_all, 3) AS avg_run_value,
        ROUND(avg_win_value_result - avg_win_value_all, 3) AS avg_win_value
    FROM agg
)

SELECT * FROM final
