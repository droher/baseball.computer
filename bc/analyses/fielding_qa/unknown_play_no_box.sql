WITH t AS (
    SELECT
        game_id,
        ANY_VALUE(substring(game_id, 4, 4)::INT) AS season,
        SUM(unknown_putouts) AS unknown_putouts
    FROM {{ ref('calc_fielding_play_agg') }}
    WHERE game_id NOT IN (SELECT game_id FROM {{ ref('stg_box_score_fielding_lines') }})
        --AND game_id IN (SELECT game_id FROM {{ ref('game_start_info') }} WHERE home_league IN ('NL', 'AL', 'FL'))
    GROUP BY 1
    HAVING SUM(unknown_putouts) > 0
)

SELECT     
    t.*,
    filename,
    line_number,
    scorer,
    park_id,
    scoring_method,
    inputter,
    translator,
    date_inputted,
FROM t
INNER JOIN {{ ref('stg_games') }} USING (game_id)
INNER JOIN {{ ref('stg_event_audit') }} a USING (game_id)
WHERE a.event_id = 1
ORDER BY t.season, game_id