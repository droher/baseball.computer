WITH from_box_scores AS (
    SELECT *
    FROM {{ source('box_score', 'box_score_umpire') }}
    WHERE game_id NOT IN (SELECT game_id FROM {{ source('game', 'game_umpire') }})
),

unioned AS (
    SELECT * FROM {{ source('game', 'game_umpire') }}
    UNION ALL
    SELECT * FROM from_box_scores
),

renamed AS (
    SELECT
        game_id,
        position,
        umpire_id

    FROM unioned
)

SELECT * FROM renamed
