WITH from_box_scores AS (
    SELECT *
    FROM {{ source('box_score', 'box_score_team') }}
    WHERE game_id NOT IN (SELECT game_id FROM {{ source('game', 'game_team') }})
),

unioned AS (
    SELECT * FROM {{ source('game', 'game_team') }}
    UNION ALL
    SELECT * FROM from_box_scores
),

renamed AS (
    SELECT
        game_id,
        team_id,
        side

    FROM unioned
)

SELECT * FROM renamed
