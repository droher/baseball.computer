MODEL (
  name main_models.stg_gamelog,
  kind FULL,
  grain (game_id),
  columns (
    season SMALLINT,
    date DATE,
    doubleheader_status DOUBLEHEADER_STATUS,
    game_id VARCHAR,
    away_team_id TEAM_ID,
    home_team_id TEAM_ID,
    time_of_day TIME_OF_DAY,
    park_id PARK_ID,
    attendance INTEGER,
    umpire_home_id VARCHAR,
    umpire_first_id VARCHAR,
    umpire_second_id VARCHAR,
    umpire_third_id VARCHAR,
    away_starting_pitcher_id VARCHAR,
    home_starting_pitcher_id VARCHAR,
    additional_info VARCHAR,
    bat_first_side SIDE,
    use_dh BOOLEAN,
    game_type GAME_TYPE,
    duration_minutes SMALLINT,
    away_line_score VARCHAR,
    home_line_score VARCHAR,
    away_runs_scored UTINYINT,
    home_runs_scored UTINYINT,
    forfeit_info VARCHAR,
    source_type VARCHAR
  ),
  column_descriptions (
    season = @doc('season'),
    date = @doc('date'),
    game_id = @doc('game_id'),
    away_team_id = @doc('away_team_id'),
    home_team_id = @doc('home_team_id'),
    time_of_day = @doc('time_of_day'),
    park_id = @doc('park_id'),
    attendance = @doc('attendance'),
    umpire_home_id = @doc('umpire_home_id'),
    umpire_first_id = @doc('umpire_first_id'),
    umpire_second_id = @doc('umpire_second_id'),
    umpire_third_id = @doc('umpire_third_id'),
    away_starting_pitcher_id = @doc('away_starting_pitcher_id'),
    home_starting_pitcher_id = @doc('home_starting_pitcher_id'),
    bat_first_side = @doc('bat_first_side'),
    game_type = @doc('game_type'),
    source_type = @doc('source_type')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_gamelog.parquet'
  ),
);







WITH source AS (
    SELECT * FROM misc.gamelog
),

renamed AS (
    SELECT
        EXTRACT(YEAR FROM date)::INT2 AS season,
        date::DATE AS date, -- noqa: RF04
        CASE double_header
            WHEN 0 THEN 'SingleGame'
            WHEN 1 THEN 'DoubleHeaderGame1'
            WHEN 2 THEN 'DoubleHeaderGame2'
            WHEN 3 THEN 'DoubleHeaderGame3'
        END::DOUBLEHEADER_STATUS AS doubleheader_status,
        home_team || STRFTIME(date, '%Y%m%d') || double_header AS game_id,
        visiting_team::TEAM_ID AS away_team_id,
        home_team::TEAM_ID AS home_team_id,
        CASE day_night WHEN 'N' THEN 'Night' ELSE 'Day' END::TIME_OF_DAY AS time_of_day,
        park_id::PARK_ID AS park_id,
        attendance,
        umpire_h_id AS umpire_home_id,
        umpire_1b_id AS umpire_first_id,
        umpire_2b_id AS umpire_second_id,
        umpire_3b_id AS umpire_third_id,
        visitor_starting_pitcher_id::PLAYER_ID AS away_starting_pitcher_id,
        home_starting_pitcher_id::PLAYER_ID AS home_starting_pitcher_id,
        additional_info,
        CASE WHEN additional_info LIKE '%HTBF%' THEN 'Home' ELSE 'Away' END::SIDE AS bat_first_side,
        -- These two might need to be updated if there's ever another game without acq info
        (EXTRACT(year from date) >= 1973 AND home_team_league = 'AL') AS use_dh,
        'RegularSeason'::GAME_TYPE AS game_type,
        -- Everything below is post-game knowledge
        duration::INT2 AS duration_minutes,
        -- TODO: Fix spelling in original
        vistor_line_score AS away_line_score,
        home_line_score,
        -- TODO: Fix spelling in original
        visitor_runs_scored::UTINYINT AS away_runs_scored,
        home_runs_score::UTINYINT AS home_runs_scored,
        forfeit_info,
        'GameLog' AS source_type
    FROM source
)

SELECT * FROM renamed
