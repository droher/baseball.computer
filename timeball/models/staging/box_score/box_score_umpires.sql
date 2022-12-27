WITH source AS (
    SELECT * FROM {{ source('box_score', 'box_score_umpire') }}
),

renamed AS (
    SELECT
        game_id,
        position,
        umpire_id

    FROM source
)

SELECT * FROM renamed
