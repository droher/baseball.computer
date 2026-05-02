MODEL (
  name main_models.stg_games,
  kind FULL,
  grain (game_id),
  columns (
    game_id VARCHAR,
    game_key UINTEGER,
    date DATE,
    start_time TIMESTAMP,
    doubleheader_status DOUBLEHEADER_STATUS,
    time_of_day TIME_OF_DAY,
    game_type GAME_TYPE,
    bat_first_side SIDE,
    sky SKY,
    field_condition FIELD_CONDITION,
    precipitation PRECIPITATION,
    wind_direction WIND_DIRECTION,
    park_id PARK_ID,
    temperature_fahrenheit TINYINT,
    attendance UINTEGER,
    wind_speed_mph UTINYINT,
    use_dh BOOLEAN,
    winning_pitcher_id VARCHAR,
    losing_pitcher_id VARCHAR,
    save_pitcher_id VARCHAR,
    game_winning_rbi_player_id VARCHAR,
    duration_minutes BIGINT,
    protest_info INTEGER,
    completion_info INTEGER,
    scorer VARCHAR,
    scoring_method VARCHAR,
    inputter VARCHAR,
    translator VARCHAR,
    date_inputted TIMESTAMP,
    date_edited INTEGER,
    account_type ACCOUNT_TYPE,
    filename VARCHAR,
    source_type VARCHAR,
    away_team_id TEAM_ID,
    home_team_id TEAM_ID,
    umpire_home_id VARCHAR,
    umpire_first_id VARCHAR,
    umpire_second_id VARCHAR,
    umpire_third_id VARCHAR,
    umpire_left_id VARCHAR,
    umpire_right_id VARCHAR,
    season SMALLINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    date = @doc('date'),
    start_time = @doc('start_time'),
    time_of_day = @doc('time_of_day'),
    game_type = @doc('game_type'),
    bat_first_side = @doc('bat_first_side'),
    sky = @doc('sky'),
    field_condition = @doc('field_condition'),
    precipitation = @doc('precipitation'),
    wind_direction = @doc('wind_direction'),
    park_id = @doc('park_id'),
    temperature_fahrenheit = @doc('temperature_fahrenheit'),
    attendance = @doc('attendance'),
    wind_speed_mph = @doc('wind_speed_mph'),
    filename = @doc('filename'),
    source_type = @doc('source_type'),
    away_team_id = @doc('away_team_id'),
    home_team_id = @doc('home_team_id'),
    umpire_home_id = @doc('umpire_home_id'),
    umpire_first_id = @doc('umpire_first_id'),
    umpire_second_id = @doc('umpire_second_id'),
    umpire_third_id = @doc('umpire_third_id'),
    umpire_left_id = @doc('umpire_left_id'),
    umpire_right_id = @doc('umpire_right_id'),
    season = @doc('season')
  ),
  audits (
    relationships(column := park_id, to_column := park_id, to_model := main_models.stg_parks),
    not_null(columns := (park_id), condition := (EXTRACT(year FROM date) >= 1875))
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_games.parquet'
  ),
);







WITH from_box_scores AS (
    SELECT *
    FROM box_score.box_score_games
    WHERE game_id NOT IN (SELECT game_id FROM game.games)
),

unioned AS (
    SELECT
        *,
        'PlayByPlay' AS source_type
    FROM game.games
    UNION ALL
    SELECT
        *,
        'BoxScore' AS source_type
    FROM from_box_scores
),

renamed AS (
    SELECT
        game_id,
        game_key,
        date,
        start_time,
        doubleheader_status,
        time_of_day,
        -- TODO: Fix all-star games without game type in raw data
        CASE WHEN REGEXP_FULL_MATCH(filename, '\d{4}AS.EVE')
                THEN 'AllStarGame'::GAME_TYPE
            ELSE game_type
        END AS game_type,
        bat_first_side,
        sky,
        field_condition,
        precipitation,
        wind_direction,
        park_id,
        temperature_fahrenheit,
        CASE
            WHEN attendance = 0 AND EXTRACT(YEAR FROM date) != 2020
                THEN NULL
            ELSE attendance
        END::UINTEGER AS attendance,
        wind_speed_mph,
        use_dh,
        -- TODO: Change in source
        winning_pitcher AS winning_pitcher_id,
        losing_pitcher AS losing_pitcher_id,
        save_pitcher AS save_pitcher_id,
        game_winning_rbi AS game_winning_rbi_player_id,
        time_of_game_minutes AS duration_minutes,
        protest_info,
        completion_info,
        scorer,
        scoring_method,
        inputter,
        translator,
        date_inputted,
        date_edited,
        account_type,
        filename,
        source_type,
        away_team_id,
        home_team_id,
        umpire_home_id,
        umpire_first_id,
        umpire_second_id,
        umpire_third_id,
        umpire_left_id,
        umpire_right_id,
        EXTRACT(YEAR FROM date)::INT2 AS season,
    FROM unioned
)

SELECT * FROM renamed
