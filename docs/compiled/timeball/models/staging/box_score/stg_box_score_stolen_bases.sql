WITH source AS (
    SELECT * FROM "timeball"."box_score"."box_score_stolen_bases"
),

renamed AS (
    SELECT
        game_id,
        running_side,
        runner_id,
        pitcher_id,
        catcher_id,
        inning

    FROM source
)

SELECT * FROM renamed