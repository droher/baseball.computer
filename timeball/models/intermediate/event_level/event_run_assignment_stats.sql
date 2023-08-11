{{
  config(
    materialized = 'table',
    )
}}
WITH event_runs AS (
    SELECT
        run_stats.event_key,
        current_pitcher.game_id,
        current_pitcher.fielding_team_id AS team_id,
        current_pitcher.pitcher_id AS current_pitcher_id,
        COALESCE(run_stats.explicit_charged_pitcher_id, charged_pitcher.pitcher_id) AS charged_pitcher_id,
        run_stats.runs
    FROM {{ ref('event_baserunning_stats') }} AS run_stats
    INNER JOIN {{ ref('event_states_batter_pitcher') }} AS current_pitcher USING (event_key)
    INNER JOIN {{ ref('event_states_batter_pitcher') }} AS charged_pitcher
        ON run_stats.charge_event_key = charged_pitcher.event_key
    WHERE run_stats.runs = 1
),

unioned AS (
    SELECT
        event_key,
        game_id,
        team_id,
        current_pitcher_id AS pitcher_id,
        runs,
        0 AS inherited_runners_scored,
        0 AS bequeathed_runners_scored
    FROM event_runs
    WHERE current_pitcher_id = charged_pitcher_id
    UNION ALL
    SELECT
        event_key,
        game_id,
        team_id,
        charged_pitcher_id AS pitcher_id,
        runs,
        0 AS inherited_runners_scored,
        runs AS bequeathed_runners_scored
    FROM event_runs
    WHERE current_pitcher_id != charged_pitcher_id
    UNION ALL
    SELECT
        event_key,
        game_id,
        team_id,
        current_pitcher_id AS pitcher_id,
        0 AS runs,
        runs AS inherited_runners_scored,
        0 AS bequeathed_runners_scored
    FROM event_runs
    WHERE current_pitcher_id != charged_pitcher_id
),

final AS (
    SELECT
        event_key,
        pitcher_id,
        ANY_VALUE(game_id) AS game_id,
        ANY_VALUE(team_id) AS team_id,
        SUM(runs) AS runs,
        SUM(inherited_runners_scored) AS inherited_runners_scored,
        SUM(bequeathed_runners_scored) AS bequeathed_runners_scored
    FROM unioned
    GROUP BY 1, 2
)

SELECT * FROM final
