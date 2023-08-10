WITH source AS (
    SELECT * FROM {{ source('box_score', 'box_score_line_score') }}
),

renamed AS (
    SELECT
        game_id,
        side AS batting_side,
    FROM source
)

SELECT * FROM renamed
