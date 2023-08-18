SELECT
    s.count_balls,
    s.count_strikes,
    COUNT(*) as ab,
    SUM(hits) as h,
    h/ab as avg,
FROM {{ ref('event_states_full') }} s
JOIN {{ ref('event_offense_stats') }} o USING (event_key)
WHERE balls_in_play = 1
--AND home_runs = 0
AND sacrifice_hits = 0
AND season >= 2000
AND pitcher_id LIKE 'rivem%' 
GROUP BY 1, 2 ORDER BY 1, 2