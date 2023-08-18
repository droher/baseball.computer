
WITH t AS (
    SELECT 
    bats,
    substring(game_id, 4, 4)::INT AS year,
    COUNT(*) AS n,
    ROUND( SUM(hits * batted_angle_left) / SUM(hits) * 100) AS batted_angle_left_hit_pct,
    ROUND( SUM((1 - hits) * batted_angle_left) / SUM(1 - hits) * 100) AS batted_angle_left_out_pct,
    ROUND( SUM(hits * batted_angle_right) / SUM(hits) * 100) AS batted_angle_right_hit_pct,
    ROUND( SUM((1 - hits) * batted_angle_right) / SUM(1 - hits) * 100) AS batted_angle_right_out_pct,
    ROUND( SUM(hits * batted_angle_middle) / SUM(hits) * 100) AS batted_angle_middle_hit_pct,
    ROUND( SUM((1 - hits) * batted_angle_middle) / SUM(1 - hits) * 100) AS batted_angle_middle_out_pct,
    (batted_angle_left_hit_pct / (100 - batted_angle_left_hit_pct)) / (batted_angle_left_out_pct / (100 - batted_angle_left_out_pct)) AS left_ratio,
    (batted_angle_right_hit_pct / (100 - batted_angle_right_hit_pct)) / (batted_angle_right_out_pct / (100 - batted_angle_right_out_pct)) AS right_ratio,
    (batted_angle_middle_hit_pct / (100 - batted_angle_middle_hit_pct)) / (batted_angle_middle_out_pct / (100 - batted_angle_middle_out_pct)) AS middle_ratio

FROM {{ ref('event_offense_stats') }}
JOIN {{ ref('stg_people') }} p ON player_id = retrosheet_player_id
WHERE balls_in_play = 1
AND home_runs = 0
AND batted_angle_unknown = 0
AND s = 1
AND year > 1920
GROUP BY 1, 2
)
SELECT 
    bats,
    year,
    n,
    ROUND(left_ratio, 2) AS left_ratio,
    ROUND(right_ratio, 2) AS right_ratio,
    ROUND(middle_ratio, 2) AS middle_ratio
 FROM t ORDER BY 1, 2
