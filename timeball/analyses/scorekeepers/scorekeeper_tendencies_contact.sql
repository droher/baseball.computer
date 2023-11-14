SELECT
    cleaned_scorer,
    substring(game_id, 4, 4)::int // 10 * 10 AS decade,
    SUM(game_share) AS plays,
    SUM(contact_type_known * hits) AS hits_contact_known,
    SUM(contact_type_unknown * hits) AS hits_contact_unknown,
    hits_contact_known / COUNT_IF(hits) AS contact_known_hit_rate,
    SUM(contact_type_ground_ball * hits) / SUM(contact_type_known * hits) AS hit_ground_ball_rate,
    SUM(contact_type_ground_ball * (1 - hits)) / SUM(contact_type_known * (1 - hits)) AS out_ground_ball_rate,
    SUM(contact_type_line_drive * hits) / SUM(contact_broad_type_air_ball * hits) AS air_hit_line_drive_rate,
    SUM(contact_type_line_drive * (1 - hits)) / SUM(contact_broad_type_air_ball * (1 - hits)) AS air_out_line_drive_rate,
    SUM(contact_type_pop_fly * hits) / SUM(contact_broad_type_air_ball * hits) AS air_hit_popup_rate,
FROM {{ ref('event_offense_stats') }} s
LEFT JOIN {{ ref('game_scorekeeping') }} k USING (game_id)
WHERE balls_batted = 1
    AND home_runs = 0
    AND ground_rule_doubles = 0
GROUP BY 1, 2
