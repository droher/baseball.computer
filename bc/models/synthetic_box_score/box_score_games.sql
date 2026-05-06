MODEL (
  name synthetic_box_score.box_score_games,
  kind FULL,
  description 'Game-level shell for the ~25K games that exist only in misc.gamelog. Mirrors box_score.box_score_games but drops post-game fields we cannot reconstruct without per-pitch data (game_key, account_type, winning_pitcher, losing_pitcher, save_pitcher, game_winning_rbi). Weather columns stay NULL — gamelog carries no weather.',
  grain (game_id),
  column_descriptions (
    game_id = @doc('game_id'),
    date = @doc('date'),
    season = @doc('season'),
    time_of_day = @doc('time_of_day'),
    game_type = @doc('game_type'),
    bat_first_side = @doc('bat_first_side'),
    park_id = @doc('park_id'),
    attendance = @doc('attendance'),
    away_team_id = @doc('away_team_id'),
    home_team_id = @doc('home_team_id')
  ),
  audits (
    not_null(columns := (game_id, date, season, home_team_id, away_team_id)),
    unique_values(columns := (game_id)),
    valid_baseball_season(column := season),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/synthetic_box_score_box_score_games.parquet'
  ),
);









WITH gamelog_only AS (
    SELECT
        g.game_id,
        g.date,
        g.season,
        g.doubleheader_status,
        g.time_of_day,
        g.game_type,
        g.bat_first_side,
        g.park_id,
        g.attendance,
        g.use_dh,
        g.home_team_id,
        g.away_team_id
    FROM main_models.game_start_info AS g
    WHERE g.source_type = 'GameLog'
)

SELECT
    game_id::GAME_ID AS game_id,
    date,
    season,
    doubleheader_status,
    time_of_day,
    game_type,
    bat_first_side,
    NULL::SKY AS sky,
    NULL::FIELD_CONDITION AS field_condition,
    NULL::PRECIPITATION AS precipitation,
    NULL::WIND_DIRECTION AS wind_direction,
    park_id,
    NULL::TINYINT AS temperature_fahrenheit,
    attendance::INTEGER AS attendance,
    NULL::UTINYINT AS wind_speed_mph,
    use_dh,
    away_team_id,
    home_team_id
FROM gamelog_only
