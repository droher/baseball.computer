WITH source AS (
    SELECT * FROM {{ source('biodata', 'umpires0') }}
),

renamed AS (
    SELECT
        id AS person_id,
        lastname AS last_name,
        firstname AS first_name,
        first_g AS first_game_date,
        last_g AS last_game_date

    FROM source
)

SELECT * FROM renamed
