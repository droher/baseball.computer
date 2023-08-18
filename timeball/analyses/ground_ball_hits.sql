WITH t AS (
    SELECT
    player_id,
    COUNT(*) as ab,
    SUM(hits) AS hits,
    SUM(hits * fielded_in_outfield) AS of_hits,
    SUM(hits)/COUNT(*) AS hit_rate,
    SUM(hits * fielded_in_infield)/COUNT(*) AS if_hit_rate,
    of_hits/COUNT(*) AS of_hit_rate,
FROM {{ ref('event_pitching_stats') }} e
JOIN {{ ref('stg_games') }} USING (game_id)
--AND fielded_in_infield + fielded_in_battery > 0
WHERE at_bats = 1
AND (
    contact_type_ground_ball = 1
    OR (contact_type_unknown = 1 AND random() < .35)
)
AND season >= 1989
GROUP BY 1 
HAVING COUNT(*) > 500
ORDER BY 5 DESC
)
-- IF/OF variance contributes roughly evenly for hitters,
-- but pitcher variance explained much more by OF
SELECT regr_r2(hit_rate, if_hit_rate) FROM t