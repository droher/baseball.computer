WITH source AS (
    SELECT * FROM {{ source('misc', 'people') }}
),

renamed AS (
    SELECT
        retroid AS retrosheet_player_id,
        bbrefid AS baseball_reference_player_id,
        playerid AS chadwick_player_id,
        birthyear AS birth_year,
        birthmonth AS birth_month,
        birthday AS birth_day,
        birthcountry AS birth_country,
        birthstate AS birth_state,
        birthcity AS birth_city,
        deathyear AS death_year,
        deathmonth AS death_month,
        deathday AS death_day,
        deathcountry AS death_country,
        deathstate AS death_state,
        deathcity AS death_city,
        namefirst AS first_name,
        namelast AS last_name,
        namegiven AS given_name,
        weight AS weight_pounds,
        height AS height_inches,
        bats,
        throws,
        debut,
        finalgame AS final_game
    FROM source
)

SELECT * FROM renamed
