WITH source AS (
    SELECT * FROM {{ source('box_score', 'box_score_comment') }}
),

renamed AS (
    SELECT
        game_id,
        sequence_id,
        comment
    FROM source
)

SELECT * FROM renamed
