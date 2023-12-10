WITH event_based_putouts AS (
    SELECT game_id, team_id, SUM(putouts) AS putouts
    FROM {{ ref('event_fielding_stats') }}
    GROUP BY 1, 2
)
SELECT game_id, team_id,
    ROUND(innings_pitched * 3)::int AS ip_outs,
    r.putouts,
    e.putouts AS event_putouts,
    ip_outs - r.putouts AS diff,
    ip_outs - e.putouts AS event_based_diff
FROM {{ ref('team_game_results') }} r
LEFT JOIN event_based_putouts e USING (game_id, team_id)
WHERE diff != 0
ORDER BY ABS(innings_pitched * 3 - r.putouts) DESC