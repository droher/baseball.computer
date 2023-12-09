WITH from_box_scores AS (
    SELECT *
    FROM {{ source('box_score', 'box_score_games') }}
    WHERE game_id NOT IN (SELECT game_id FROM {{ source('game', 'games') }})
),

unioned AS (
    SELECT
        *,
        'PlayByPlay' AS source_type
    FROM {{ source('game', 'games') }}
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
        CASE WHEN REGEXP_FULL_MATCH(game_type, '\d{4}AS.EVE')
                THEN 'AllStarGame'
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
