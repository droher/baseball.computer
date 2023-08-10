WITH event_runs AS (
    SELECT
        run_stats.event_key,
        current_pitcher.fielding_team_id,
        current_pitcher.pitcher_id AS current_pitcher_id,
        COALESCE(run_stats.explicit_charged_pitcher_id, charged_pitcher.pitcher_id) AS charged_pitcher_id,
        run_stats.runs
    FROM {{ ref('event_baserunning_stats') }} run_stats
    INNER JOIN {{ ref('event_states_batter_pitcher') }} current_pitcher USING (event_key)
    INNER JOIN {{ ref('event_states_batter_pitcher') }} charged_pitcher
        ON run_stats.charge_event_key = charged_pitcher.event_key
    WHERE run_stats.runs = 1
),

final AS (
    SELECT
        event_key,
        fielding_team_id,
        current_pitcher_id AS pitcher_id,
        runs,
        0 AS inherited_runners_scored,
        0 AS bequeathed_runners_scored
    FROM event_runs
    WHERE current_pitcher_id = charged_pitcher_id
    UNION ALL
    SELECT
        event_key,
        fielding_team_id,
        charged_pitcher_id AS pitcher_id,
        runs,
        0 AS inherited_runners_scored,
        1 AS bequeathed_runners_scored
    FROM event_runs
    WHERE current_pitcher_id != charged_pitcher_id
    UNION ALL
    SELECT
        event_key,
        fielding_team_id,
        current_pitcher_id AS pitcher_id,
        0 AS runs,
        1 AS inherited_runners_scored,
        0 AS bequeathed_runners_scored
    FROM event_runs
    WHERE current_pitcher_id != charged_pitcher_id
)

SELECT * FROM final
