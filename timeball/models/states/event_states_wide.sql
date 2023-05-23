WITH lineups AS (
    SELECT * 
    FROM {{ ref('event_lineup_states') }}
    WHERE hash(event_key) % 10 = 0
),

pivoter AS (
    PIVOT lineups
    ON lineup_position IN (1, 2, 3, 4, 5, 6, 7, 8, 9)
    USING FIRST(player_id)
    GROUP BY event_key
)

SELECT * FROM pivoter
