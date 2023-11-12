WITH t AS (
    SELECT
        game_id,
        team_id,
        SUM(putouts) AS total_putouts,
        SUM(unknown_putouts) AS unknown_putouts
    FROM {{ ref('event_fielding_stats') }}
    GROUP BY 1, 2
),

team_season_overall_coverage AS (
    SELECT
        t.*,
        gt.team_side AS side,
        SUM(total_putouts - unknown_putouts) OVER season_side / SUM(total_putouts) OVER season_side AS season_coverage_rate_by_side
    FROM t 
    INNER JOIN {{ ref('stg_games') }} g USING (game_id)
    INNER JOIN {{ ref('team_game_start_info') }} gt USING (game_id, team_id)
    WINDOW season_side AS (PARTITION BY t.team_id, gt.team_side)
),

agg AS (
    SELECT
        game_id,
        SUM(unknown_putouts) AS total_unknown_putouts,
        ANY_VALUE(CASE WHEN side = 'Home' THEN season_coverage_rate_by_side END) AS coverage_rate_home,
        ANY_VALUE(CASE WHEN side = 'Away' THEN season_coverage_rate_by_side END) AS coverage_rate_away,
    FROM team_season_overall_coverage
    GROUP BY 1
    HAVING SUM(unknown_putouts) > 0
)

SELECT
    season,
    agg.*,
    filename,
    line_number,
    scorer,
    park_id,
    scoring_method,
    inputter,
    translator,
    date_inputted,
FROM agg
INNER JOIN {{ ref('stg_games') }} USING (game_id)
INNER JOIN {{ ref('stg_event_audit') }} a USING (game_id)
WHERE a.event_id = 1
    AND filename NOT LIKE '%.EVR'
ORDER BY coverage_rate_home * coverage_rate_away DESC