WITH t AS (
SELECT
    p.batter_hand,
    c.recorded_location,
    c.recorded_location_angle,
    c.recorded_location_depth,
    COUNT(*) AS at_bats,
    SUM(hits) AS hits,
    SUM(1 - hits) AS outs,
    SUM(hits) / COUNT(*) AS avg,
FROM {{ ref('player_game_data_completeness') }} AS g
INNER JOIN {{ ref('event_offense_stats') }} AS e USING (game_id, player_id)
INNER JOIN {{ ref('calc_batted_ball_type') }} AS c USING (game_id, event_key)
INNER JOIN {{ ref('event_states_full') }} p USING (game_id, event_key)
WHERE g.player_type = 'BATTING'
    AND g.has_scoresheet_location AND g.has_batted_to_fielder AND g.has_trajectory
    AND g.season BETWEEN 1993 AND 1999
    --AND g.season >= 2020
    AND trajectory_known
    AND bunts = 0
    AND balls_in_play = 1

GROUP BY 1, 2, 3, 4
)

SELECT
    batter_hand,
    recorded_location,
    recorded_location_angle,
    recorded_location_depth,
    at_bats,
    avg,
    hits / SUM(hits) OVER (PARTITION BY batter_hand) AS hit_share,
    outs / SUM(outs) OVER (PARTITION BY batter_hand) AS out_share,
FROM t
ORDER BY SUM(at_bats) OVER (PARTITION BY recorded_location) DESC, batter_hand