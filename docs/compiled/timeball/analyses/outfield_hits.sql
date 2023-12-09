WITH t AS (
    SELECT
        p.batter_hand,
        p.pitcher_hand,
        c.batted_to_fielder,
        e.total_bases,
        COUNT(*) AS hits,
        SUM(trajectory_ground_ball)/COUNT(*) AS hit_gb_rate

INNER JOIN "timeball"."main_models"."calc_batted_ball_type" AS c USING (game_id)
INNER JOIN "timeball"."main_models"."event_offense_stats" AS e USING (event_key)
INNER JOIN "timeball"."main_models"."event_states_full" p USING (event_key)
WHERE g.has_batted_to_fielder AND g.has_trajectory
    AND e.hits = 1
    AND e.fielded_by_outfielder = 1
    AND e.bunts = 0
GROUP BY 1, 2, 3, 4
)

SELECT * FROM t