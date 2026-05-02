MODEL (
  name main_models.game_results,
  kind FULL,
  description 'Includes the results of every completed, suspended, or forfeited game in the database.',
  grain (game_id),
  column_descriptions (
    game_id = @doc('game_id'),
    season = @doc('season'),
    game_type = @doc('game_type'),
    game_finish_date = 'This will always be the same as the game''s `date` unless the game was suspended and finished on a different day. Games that end after midnight will still have the same value as the `date`.',
    home_team_id = @doc('home_team_id'),
    away_team_id = @doc('away_team_id')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_game_results.parquet'
  ),
  audits (
    unique_values(columns := (game_id)),
    not_null(columns := (game_id, season)),
    valid_baseball_season(column := season),
    relationships(column := away_team_id, to_model := main_seeds.seed_franchises, to_column := team_id),
    relationships(column := home_team_id, to_model := main_seeds.seed_franchises, to_column := team_id)
  ),
);







WITH event_and_box_results AS (
    SELECT
        game_id,
        games.date,
        games.duration_minutes,
        games.winning_pitcher_id,
        games.losing_pitcher_id,
        games.save_pitcher_id,
        games.game_winning_rbi_player_id,
        line_scores.home_runs_scored,
        line_scores.away_runs_scored,
        line_scores.home_line_score,
        line_scores.away_line_score,
        line_scores.duration_outs,
    FROM main_models.stg_games AS games
    LEFT JOIN main_models.game_line_scores AS line_scores USING (game_id)

),

gamelog_results AS (
    SELECT
        game_id,
        date,
        duration_minutes,
        home_runs_scored,
        away_runs_scored,
        away_line_score,
        home_line_score,
    FROM main_models.stg_gamelog
    WHERE game_id NOT IN (SELECT game_id FROM event_and_box_results)
),

unioned AS (
    SELECT *
    FROM gamelog_results
    UNION ALL BY NAME
    SELECT *
    FROM event_and_box_results
),

final AS (
    SELECT
        game_id,
        start_info.season,
        start_info.game_type,
        COALESCE(suspensions.date_resumed, unioned.date) AS game_finish_date,
        start_info.home_team_id,
        start_info.away_team_id,
        CASE
            WHEN forfeits.winning_side = 'Home'
                THEN start_info.home_team_id
            WHEN forfeits.winning_side = 'Away'
                THEN start_info.away_team_id
            WHEN unioned.home_runs_scored > unioned.away_runs_scored
                THEN start_info.home_team_id
            WHEN unioned.home_runs_scored < unioned.away_runs_scored
                THEN start_info.away_team_id
        END AS winning_team_id,
        CASE
            WHEN winning_team_id = start_info.home_team_id
                THEN start_info.away_team_id
            WHEN winning_team_id = start_info.away_team_id
                THEN start_info.home_team_id
        END AS losing_team_id,
        CASE
            WHEN winning_team_id = start_info.home_team_id
                THEN unioned.home_runs_scored
            WHEN winning_team_id = start_info.away_team_id
                THEN unioned.away_runs_scored
        END::UTINYINT AS winning_team_score,
        CASE
            WHEN winning_team_id = start_info.home_team_id
                THEN unioned.away_runs_scored
            WHEN winning_team_id = start_info.away_team_id
                THEN unioned.home_runs_scored
        END::UTINYINT AS losing_team_score,
        CASE
            WHEN winning_team_id = start_info.home_team_id
                THEN 'Home'
            WHEN winning_team_id = start_info.away_team_id
                THEN 'Away'
        END::SIDE AS winning_side,
        CASE
            WHEN winning_team_id = start_info.home_team_id
                THEN 'Away'
            WHEN winning_team_id = start_info.away_team_id
                THEN 'Home'
        END::SIDE AS losing_side,
        forfeits.game_id IS NOT NULL AS forfeit_flag,
        suspensions.game_id IS NOT NULL AS suspension_flag,
        winning_team_id IS NULL AS tie_flag,
        unioned.winning_pitcher_id,
        unioned.losing_pitcher_id,
        unioned.save_pitcher_id,
        unioned.game_winning_rbi_player_id,
        unioned.home_runs_scored::UTINYINT AS home_runs_scored,
        unioned.away_runs_scored::UTINYINT AS away_runs_scored,
        unioned.away_line_score,
        unioned.home_line_score,
        unioned.duration_minutes::USMALLINT AS duration_minutes,
        unioned.duration_outs,
        -- We'll assume for now that games without recorded outs are 9 innings by default
        COALESCE(unioned.duration_outs BETWEEN 51 AND 54, TRUE) AS is_nine_inning_game,
        COALESCE(unioned.duration_outs > 54, FALSE) AS is_extra_inning_game,
        COALESCE(unioned.duration_outs < 51, FALSE) AS is_shortened_game,
    FROM unioned
    INNER JOIN main_models.game_start_info AS start_info USING (game_id)
    LEFT JOIN main_models.game_suspensions AS suspensions USING (game_id)
    LEFT JOIN main_models.game_forfeits AS forfeits USING (game_id)
)

SELECT * FROM final
