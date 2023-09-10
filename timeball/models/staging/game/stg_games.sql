WITH from_box_scores AS (
    SELECT *
    FROM {{ source('box_score', 'box_score_game') }}
    WHERE game_id NOT IN (SELECT game_id FROM {{ source('game', 'game') }})
),

unioned AS (
    SELECT
        *,
        'PlayByPlay' AS source_type
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
        source_type,
        EXTRACT(YEAR FROM date) AS season,
    FROM unioned
)

SELECT * FROM renamed
