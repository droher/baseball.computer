
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
    (batted_angle_middle_hit_pct / (100 - batted_angle_middle_hit_pct)) / (batted_angle_middle_out_pct / (100 - batted_angle_middle_out_pct)) AS middle_ratio,
    batted_angle_left_hit_pct - batted_angle_left_out_pct AS batted_angle_left_diff,
    batted_angle_right_hit_pct - batted_angle_right_out_pct AS batted_angle_right_diff,
    batted_angle_middle_hit_pct - batted_angle_middle_out_pct AS batted_angle_middle_diff

FROM {{ ref('event_offense_stats') }}
JOIN {{ ref('stg_people') }} p ON player_id = retrosheet_player_id
WHERE balls_in_play = 1
AND home_runs = 0
AND batted_angle_unknown = 0
AND contact_type_ground_ball = 1
AND bats = 'L'
AND year > 1920
GROUP BY 1, 2
)
SELECT 
    bats,
    year,
    n,
    batted_angle_left_out_pct,
    batted_angle_right_out_pct,
    batted_angle_middle_out_pct,
    batted_angle_left_diff,
    batted_angle_right_diff,
    batted_angle_middle_diff,
    ROUND(left_ratio, 2) AS left_ratio,
    ROUND(right_ratio, 2) AS right_ratio,
    ROUND(middle_ratio, 2) AS middle_ratio,
    AVG(left_ratio) OVER (PARTITION BY bats ORDER BY year ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS left_ratio_3yr_avg,
    AVG(right_ratio) OVER (PARTITION BY bats ORDER BY year ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS right_ratio_3yr_avg,
    AVG(middle_ratio) OVER (PARTITION BY bats ORDER BY year ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS middle_ratio_3yr_avg,

 FROM t ORDER BY 1, 2
