
WITH event_runs AS (
    SELECT
        run_stats.event_key,
        run_stats.game_id,
        run_stats.fielding_team_id AS team_id,
        run_stats.current_pitcher_id AS current_pitcher_id,
        COALESCE(run_stats.explicit_charged_pitcher_id, charged_pitcher.pitcher_id) AS charged_pitcher_id,
        run_stats.runs,
        events.team_unearned_runs
    FROM "timeball"."main_models"."event_baserunning_stats" AS run_stats
    INNER JOIN "timeball"."main_models"."event_states_batter_pitcher" AS charged_pitcher USING (event_key)
    INNER JOIN "timeball"."main_models"."stg_events" AS events USING (event_key)
    WHERE run_stats.runs = 1
),

unioned AS (
    SELECT
        event_key,
        game_id,
        team_id,
        current_pitcher_id AS pitcher_id,
        runs,
        team_unearned_runs,
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
        team_unearned_runs,
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
        0 AS team_unearned_runs,
        runs AS inherited_runners_scored,
        0 AS bequeathed_runners_scored
    FROM event_runs
    WHERE current_pitcher_id != charged_pitcher_id
),

final AS (
    SELECT
        event_key,
        pitcher_id,
        MIN(game_id) AS game_id,
        MIN(team_id) AS team_id,
        SUM(runs)::UTINYINT AS runs,
        SUM(team_unearned_runs)::UTINYINT AS team_unearned_runs,
        SUM(inherited_runners_scored)::UTINYINT AS inherited_runners_scored,
        SUM(bequeathed_runners_scored)::UTINYINT AS bequeathed_runners_scored
    FROM unioned
    GROUP BY 1, 2
)

SELECT * FROM final