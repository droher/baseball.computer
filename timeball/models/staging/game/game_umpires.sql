WITH source AS (
    SELECT * FROM {{ source('game', 'game_umpire') }}
),

renamed AS (
    SELECT
        game_id,
        position,
        umpire_id

    FROM source
)

SELECT * FROM renamed
