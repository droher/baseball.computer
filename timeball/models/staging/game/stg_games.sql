WITH source AS (
    SELECT * FROM {{ source('game', 'game') }}
),

renamed AS (
    SELECT
        game_id,
        game_key,
        date,
        start_time,
        doubleheader_status,
        time_of_day,
        game_type,
        bat_first_side,
        sky,
        field_condition,
        precipitation,
        wind_direction,
        park_id,
        temperature_fahrenheit,
        attendance,
        wind_speed_mph,
        use_dh,
        winning_pitcher,
        losing_pitcher,
        save_pitcher,
        game_winning_rbi,
        time_of_game_minutes,
        protest_info,
        completion_info,
        scorer,
        scoring_method,
        inputter,
        translator,
        date_inputted,
        date_edited,
        EXTRACT(YEAR FROM date) AS season
    FROM source
)

SELECT * FROM renamed
