WITH t AS (
SELECT
    p.batter_hand,
    c.recorded_location,
    --c.recorded_location_angle,
    COUNT(*) AS at_bats,
    SUM(hits) AS hits,
    SUM(1 - hits) AS outs,
    SUM(hits) / COUNT(*) AS avg,
FROM {{ ref('player_game_data_completeness') }} AS g
INNER JOIN {{ ref('event_offense_stats') }} AS e USING (game_id, player_id)
INNER JOIN {{ ref('calc_batted_ball_type') }} AS c USING (game_id, event_key)
INNER JOIN {{ ref('event_states_full') }} p USING (game_id, event_key)
WHERE g.player_type = 'BATTING'
    AND g.has_scoresheet_location AND g.has_batted_to_fielder AND g.has_contact_type
    AND g.season BETWEEN 1993 AND 1999
    --AND g.season >= 2020
    AND recorded_location_angle != 'Foul'
    AND contact_type_known
    AND bunts = 0
    AND balls_in_play = 1

GROUP BY 1, 2
)

SELECT
    batter_hand,
    recorded_location,
    at_bats,
    avg,
    hits / SUM(hits) OVER (PARTITION BY batter_hand) AS hit_share,
    outs / SUM(outs) OVER (PARTITION BY batter_hand) AS out_share,
FROM t
ORDER BY SUM(at_bats) OVER (PARTITION BY recorded_location) DESC, batter_hand