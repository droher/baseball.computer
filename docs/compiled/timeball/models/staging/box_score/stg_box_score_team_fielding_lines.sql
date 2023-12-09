WITH source AS (
    SELECT * FROM "timeball"."box_score"."box_score_team_fielding_lines"
),

renamed AS (
    SELECT
        game_id,
        side,
        outs_played,
        putouts,
        assists,
        errors,
        double_plays,
        triple_plays,
        passed_balls

    FROM source
)

SELECT * FROM renamed