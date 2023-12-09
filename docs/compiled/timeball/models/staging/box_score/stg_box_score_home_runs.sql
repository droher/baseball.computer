WITH source AS (
    SELECT * FROM "timeball"."box_score"."box_score_home_runs"
),

renamed AS (
    SELECT
        game_id,
        batting_side,
        batter_id,
        pitcher_id,
        inning,
        runners_on,
        outs

    FROM source
)

SELECT * FROM renamed