WITH gamelog_games AS (
    SELECT *
    FROM {{ ref('stg_gamelog') }}
    WHERE game_id NOT IN (SELECT game_id FROM {{ ref('stg_games') }})
)

SELECT * FROM gamelog_games
