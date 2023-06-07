WITH box_score_games AS (
    SELECT *
    FROM {{ ref('stg_box_score_games') }}
    WHERE game_id NOT IN (SELECT game_id FROM {{ ref('stg_games') }})
),

full_games AS (
    SELECT *
    FROM {{ ref('stg_games') }}
    UNION ALL
    SELECT *
    FROM box_score_games
)

SELECT * FROM full_games
