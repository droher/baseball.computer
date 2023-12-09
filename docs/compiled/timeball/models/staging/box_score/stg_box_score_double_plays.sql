WITH source AS (
    SELECT * FROM "timeball"."box_score"."box_score_double_plays"
),

renamed AS (
    SELECT
        game_id,
        defense_side,
        fielders

    FROM source
)

SELECT * FROM renamed