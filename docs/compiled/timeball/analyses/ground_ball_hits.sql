WITH t AS (
    SELECT
    player_id,
    COUNT(*) as ab,
    SUM(hits) AS hits,
    SUM(hits * fielded_by_outfielder) AS of_hits,
    SUM(hits)/COUNT(*) AS hit_rate,
    SUM(hits * fielded_by_infielder)/COUNT(*) AS if_hit_rate,
    of_hits/COUNT(*) AS of_hit_rate,
FROM "timeball"."main_models"."event_pitching_stats" e
JOIN "timeball"."main_models"."stg_games" USING (game_id)
WHERE at_bats = 1
    AND trajectory_ground_ball = 1
    AND fielded_by_known = 1
    AND season BETWEEN 1993 AND 1999
GROUP BY 1 
HAVING COUNT(*) > 100
ORDER BY 5 DESC
)
-- IF/OF variance contributes roughly evenly for hitters,
-- but pitcher variance explained much more by OF
-- Inference: infield hit BABIP should be ignored for pitchers but not hitters
-- SELECT regr_r2(hit_rate, if_hit_rate), regr_r2(hit_rate, of_hit_rate)
-- FROM t #}