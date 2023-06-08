WITH from_box_scores AS (
    SELECT *
    FROM {{ source('box_score', 'box_score_game') }}
    WHERE game_id NOT IN (SELECT game_id FROM {{ source('game', 'game') }})
),

unioned AS (
    SELECT
        *,
        'Event' AS source_type
    FROM {{ source('game', 'game') }}
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
        source_type,
        EXTRACT(YEAR FROM date) AS season,
    FROM unioned
)

SELECT * FROM renamed
