MODEL (
  name main_models.team_game_start_info,
  kind FULL,
  description 'A version of `game_start_info` that includes one row for each team in each game.',
  grain (game_id, team_id),
  column_descriptions (
    team_id = @doc('team_id'),
    league = @doc('league'),
    division = @doc('division'),
    team_name = @doc('team_name'),
    game_id = @doc('game_id'),
    date = @doc('date'),
    start_time = @doc('start_time'),
    season = @doc('season'),
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
    source_type = @doc('source_type'),
    umpire_home_id = @doc('umpire_home_id'),
    umpire_first_id = @doc('umpire_first_id'),
    umpire_second_id = @doc('umpire_second_id'),
    umpire_third_id = @doc('umpire_third_id'),
    umpire_left_id = @doc('umpire_left_id'),
    umpire_right_id = @doc('umpire_right_id'),
    filename = @doc('filename'),
    is_regular_season = @doc('is_regular_season'),
    is_postseason = @doc('is_postseason'),
    away_franchise_id = @doc('away_franchise_id'),
    home_franchise_id = @doc('home_franchise_id'),
    is_interleague = @doc('is_interleague'),
    lineup_map_away = @doc('lineup_map_away'),
    lineup_map_home = @doc('lineup_map_home'),
    fielding_map_away = @doc('fielding_map_away'),
    fielding_map_home = @doc('fielding_map_home')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_team_game_start_info.parquet'
  ),
);







WITH base AS (
    SELECT
        home_team_id AS team_id,
        away_team_id AS opponent_id,
        home_league AS league,
        away_league AS opponent_league,
        home_division AS division,
        away_division AS opponent_division,
        home_team_name AS team_name,
        away_team_name AS opponent_name,
        home_starting_pitcher_id AS starting_pitcher_id,
        away_starting_pitcher_id AS opponent_starting_pitcher_id,
        'Home'::SIDE AS team_side,
        *
    FROM main_models.game_start_info
    UNION ALL BY NAME
    SELECT
        away_team_id AS team_id,
        home_team_id AS opponent_id,
        away_league AS league,
        home_league AS opponent_league,
        away_division AS division,
        home_division AS opponent_division,
        away_team_name AS team_name,
        home_team_name AS opponent_name,
        away_starting_pitcher_id AS starting_pitcher_id,
        home_starting_pitcher_id AS opponent_starting_pitcher_id,
        'Away'::SIDE AS team_side,
        *
    FROM main_models.game_start_info
),

add_series_start_flag AS (
    SELECT
        * EXCLUDE (
            away_team_id,
            home_team_id,
            away_league,
            home_league,
            away_division,
            home_division,
            away_team_name,
            home_team_name,
            away_starting_pitcher_id,
            home_starting_pitcher_id
        ),
        CASE
            WHEN LAG(opponent_id::VARCHAR, 1, 'N/A') OVER season_series != opponent_id
                THEN game_id
        END AS series_id
    FROM base
    WINDOW season_series AS (
        PARTITION BY season, team_id, game_type, opponent_id
        ORDER BY date, doubleheader_status
    )
),

assign_series_id AS (
    SELECT -- noqa: AM04
        * REPLACE (
            -- The closest non-null value to the current row (inclusive) is the proper series_id.
            COALESCE(LAG(series_id IGNORE NULLS) OVER season_series, series_id) AS series_id
        )
    FROM add_series_start_flag
    WINDOW season_series AS (
        PARTITION BY season, team_id, game_type, opponent_id
        ORDER BY date, doubleheader_status
    )
),

final AS (
    SELECT
        *,
        COUNT(*) OVER season AS season_game_number,
        COUNT(*) OVER series AS series_game_number,
        DATEDIFF('day', LAG(date) OVER season, date) AS days_since_last_game,
    FROM assign_series_id
    WINDOW
        season AS (
            PARTITION BY season, team_id, game_type
            ORDER BY date, doubleheader_status
        ),
        series AS (
            PARTITION BY team_id, series_id
            ORDER BY date, doubleheader_status
        )
)

SELECT * FROM final
