WITH source AS (
    SELECT * FROM "timeball"."box_score"."box_score_fielding_lines"
),

renamed AS (
    SELECT
        game_id,
        fielder_id,
        side,
        fielding_position,
        nth_position_played_by_player,
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