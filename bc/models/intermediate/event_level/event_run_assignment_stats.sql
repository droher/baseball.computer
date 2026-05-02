MODEL (
  name main_models.event_run_assignment_stats,
  kind FULL,
  grain (event_key, pitcher_id),
  columns (
    event_key UINTEGER,
    pitcher_id VARCHAR,
    game_id VARCHAR,
    team_id TEAM_ID,
    runs UTINYINT,
    team_unearned_runs UTINYINT,
    inherited_runners_scored UTINYINT,
    bequeathed_runners_scored UTINYINT
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    pitcher_id = @doc('pitcher_id'),
    game_id = @doc('game_id'),
    team_id = @doc('team_id'),
    runs = @doc('runs'),
    team_unearned_runs = @doc('team_unearned_runs'),
    inherited_runners_scored = @doc('inherited_runners_scored'),
    bequeathed_runners_scored = @doc('bequeathed_runners_scored')
  ),
  audits (
    not_null(columns := (event_key, pitcher_id)),
    unique_grain(columns := (event_key, pitcher_id)),
    relationships(column := event_key, to_model := main_models.stg_events, to_column := event_key),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id),
    relationships(column := pitcher_id, to_model := main_models.people, to_column := player_id),
    relationships(column := team_id, to_model := main_seeds.seed_franchises, to_column := team_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_run_assignment_stats.parquet'
  ),
);







WITH event_runs AS (
    SELECT
        run_stats.event_key,
        run_stats.game_id,
        run_stats.fielding_team_id AS team_id,
        run_stats.current_pitcher_id AS current_pitcher_id,
        COALESCE(run_stats.explicit_charged_pitcher_id, charged_pitcher.pitcher_id) AS charged_pitcher_id,
        run_stats.runs,
        events.team_unearned_runs
    FROM main_models.event_baserunning_stats AS run_stats
    INNER JOIN main_models.stg_events AS events USING (event_key)
    INNER JOIN main_models.event_states_batter_pitcher AS charged_pitcher
        ON charged_pitcher.event_key = run_stats.charge_event_key
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
