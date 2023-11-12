WITH incomplete_games AS (
    SELECT game_id,
        SUM(unknown_putouts) AS unknown_putouts
    FROM {{ ref('event_fielding_stats') }}
    WHERE unknown_putouts > 0
    GROUP BY 1
),

t AS (
    SELECT game_id,
        team_id,
        SUM(ABS(CASE WHEN i.game_id IS NOT NULL
                    THEN LEAST(surplus_box_putouts, 0)
                ELSE surplus_box_putouts
            END
        )) AS putout_discrepancy,
        SUM(ABS(CASE WHEN i.game_id IS NOT NULL
                    THEN LEAST(surplus_box_assists, 0)
                ELSE surplus_box_assists
            END
        )) AS assist_discrepancy,
        SUM(ABS(surplus_box_errors)) AS error_discrepancy,
        putout_discrepancy + assist_discrepancy + error_discrepancy AS total,
        ANY_VALUE(COALESCE(i.unknown_putouts, 0)) AS unknown_putouts
    FROM {{ ref('player_position_game_fielding_lines') }}
    LEFT JOIN incomplete_games i USING (game_id)
    GROUP BY 1, 2
    HAVING total > 0
)

SELECT t.*,
    filename,
    line_number,
    scorer,
    park_id,
    scoring_method,
    inputter,
    translator,
    date_inputted
FROM t
INNER JOIN {{ ref('stg_games') }} AS g USING (game_id)
INNER JOIN {{ ref('stg_event_audit') }} AS e USING (game_id)
WHERE e.event_id = 1
ORDER BY total DESC