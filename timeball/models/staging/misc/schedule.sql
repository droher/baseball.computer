WITH source AS (
    SELECT * FROM {{ source('misc', 'schedule') }}
),

renamed AS (
    SELECT
        date,
        double_header,
        day_of_week,
        visiting_team,
        visiting_team_league,
        visiting_team_game_number,
        home_team,
        home_team_league,
        home_team_game_number,
        day_night,
        postponement_indicator,
        makeup_dates

    FROM source
)

SELECT * FROM renamed
