WITH players AS (
    SELECT
    s.pitcher_id as player_id,
    s.count_balls,
    s.count_strikes,
    b.contact,
    COUNT(*) as ab,
    SUM(hits) as h,
    h / ab AS avg
FROM {{ ref('event_states_full') }} s
JOIN {{ ref('event_offense_stats') }} o USING (event_key)
JOIN {{ ref('calc_batted_ball_type') }} b USING (event_key)
WHERE balls_in_play = 1
AND sacrifice_hits = 0
AND season >= 2003
AND o.bunts = 0
AND count_balls + count_strikes IS NOT NULL
AND contact = 'LineDrive'
GROUP BY 1, 2, 3, 4
),

odds AS (
    SELECT
        player_id,
        count_balls,
        count_strikes,
        contact,
        ab,
        SUM(h) OVER w / SUM(ab) OVER w as avg_total,
        avg - avg_total AS avg_diff,
    FROM players
    WINDOW w AS (PARTITION BY player_id, contact)
),

weighted AS (
    SELECT
        count_balls,
        count_strikes,
        contact,
        SUM(ab) as ab,
        SUM(avg_diff * ab) / SUM(ab) * 1000 as avg_diff
    FROM odds
    GROUP BY 1, 2, 3
)

SELECT * FROM weighted