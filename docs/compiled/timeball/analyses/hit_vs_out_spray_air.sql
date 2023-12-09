WITH t AS (
SELECT
    p.batter_hand,
    c.batted_to_fielder,
    --c.recorded_location_angle,
    COUNT(*) AS at_bats,
    SUM(hits) AS hits,
    SUM(1 - hits) AS outs,
    SUM(hits) / COUNT(*) AS avg,

INNER JOIN "timeball"."main_models"."calc_batted_ball_type" AS c USING (game_id)
INNER JOIN "timeball"."main_models"."event_offense_stats" AS e USING (event_key)
INNER JOIN "timeball"."main_models"."event_states_full" p USING (event_key)
WHERE g.has_batted_to_fielder AND g.has_trajectory
    AND trajectory_broad_air_ball = 1
    AND bunts = 0
    AND balls_in_play = 1
    AND fielded_by_outfielder = 1
GROUP BY 1, 2
)

SELECT
    batter_hand,
    batted_to_fielder,
    at_bats,
    avg,
    hits / SUM(hits) OVER (PARTITION BY batter_hand) AS hit_share,
    outs / SUM(outs) OVER (PARTITION BY batter_hand) AS out_share,
FROM t
ORDER BY SUM(at_bats) OVER (PARTITION BY batted_to_fielder) DESC, batter_hand