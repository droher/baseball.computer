WITH source AS (
    SELECT * FROM "timeball"."box_score"."box_score_line_scores"
),

renamed AS (
    SELECT
        game_id,
        side AS batting_side,
        inning,
        runs
    FROM source
)

SELECT * FROM renamed