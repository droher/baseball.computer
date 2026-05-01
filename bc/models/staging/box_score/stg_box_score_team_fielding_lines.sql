WITH source AS (
    SELECT * FROM {{ source('box_score', 'box_score_team_fielding_lines') }}
),

renamed AS (
    SELECT
        game_id,
        side,
        outs_played::BIGINT AS outs_played,
        putouts::UTINYINT AS putouts,
        assists::UTINYINT AS assists,
        errors::UTINYINT AS errors,
        double_plays::UTINYINT AS double_plays,
        triple_plays::UTINYINT AS triple_plays,
        passed_balls::UTINYINT AS passed_balls

    FROM source
)

SELECT * FROM renamed
