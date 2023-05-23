WITH games AS (
    SELECT * FROM {{ ref('stg_games') }}
),

final AS (
    SELECT MAX(game_key) FROM games
)

SELECT * FROM final
ORDER BY 1