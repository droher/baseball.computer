WITH t AS (SELECT
    player_id,
    BOOL_OR(throws = 'R')::INT AS is_righty,
    SUM(contact_type_ground_ball) AS gb,
    SUM(strikeouts)/SUM(batters_faced) AS k_rate,
    SUM(walks)/SUM(batters_faced) AS bb_rate,
    SUM(balls_in_play)/SUM(batters_faced) AS bip_rate,
    SUM(contact_type_ground_ball)/SUM(contact_type_known) AS gb_rate,
    SUM(contact_broad_type_air_ball)/SUM(contact_type_known) AS air_ball_rate,
    SUM(contact_type_line_drive)/SUM(contact_broad_type_air_ball) AS ld_rate,
    SUM(contact_type_pop_fly)/SUM(contact_broad_type_air_ball) AS pu_rate,
    SUM(home_runs)/SUM(contact_broad_type_air_ball) AS hr_rate,
    k_rate / bb_rate AS kbb,
    SUM(contact_type_ground_ball * batted_balls_pulled)/gb AS pulled_gb,
    SUM(contact_type_ground_ball * batted_balls_opposite_field)/gb AS oppo_gb,
    SUM(contact_type_ground_ball * batted_angle_middle)/gb AS middle_gb,
    SUM(contact_broad_type_air_ball * batted_balls_pulled)/SUM(contact_broad_type_air_ball) AS pulled_air,
    SUM(contact_broad_type_air_ball * batted_balls_opposite_field)/SUM(contact_broad_type_air_ball) AS oppo_air,
    SUM(contact_broad_type_air_ball * batted_angle_middle)/SUM(contact_broad_type_air_ball) AS middle_air,
    SUM(contact_type_ground_ball * fielded_by_battery)/gb AS weak_grounders,
    SUM(contact_type_ground_ball * hits)/gb AS gb_hit_rate,
    SUM(infield_hits)/gb AS infield_hit_rate,
FROM  {{ ref('event_pitching_stats') }}
INNER JOIN {{ ref('game_start_info') }} g USING (game_id)
INNER JOIN {{ ref('people') }} USING (player_id)
WHERE g.season > 1988
    -- Contact type is missing not at random in these years
    AND season NOT BETWEEN 2000 AND 2001
    AND (contact_type_known OR NOT balls_batted)
    AND (fielded_by_known OR NOT balls_batted)
    AND batters_faced = 1
    AND bunts = 0
GROUP BY 1
HAVING gb > 500
)
SELECT COUNT(*),
    CORR(is_righty, gb_hit_rate) AS righty_corr,
    CORR(weak_grounders, gb_hit_rate) AS weak_corr,
    CORR(kbb, gb_hit_rate) AS kbb_corr,
    CORR(k_rate, gb_hit_rate) AS k_corr,
    CORR(bb_rate, gb_hit_rate) AS bb_corr,
    CORR(pulled_gb, gb_hit_rate) AS pulled_gb_corr,
    CORR(oppo_gb, gb_hit_rate) AS oppo_gb_corr,
    CORR(middle_gb, gb_hit_rate) AS middle_gb_corr,
    CORR(infield_hit_rate, gb_hit_rate) AS infield_corr,
    CORR(pulled_air, gb_hit_rate) AS pulled_air_corr,
    CORR(oppo_air, gb_hit_rate) AS oppo_air_corr,
    CORR(middle_air, gb_hit_rate) AS middle_air_corr,
    CORR(bip_rate, gb_hit_rate) AS bip_rate_corr,
    CORR(gb_rate, gb_hit_rate) AS gb_rate_corr,
    CORR(ld_rate, gb_hit_rate) AS ld_rate_corr,
    CORR(pu_rate, gb_hit_rate) AS pu_rate_corr,
    CORR(hr_rate, gb_hit_rate) AS hr_rate_corr,
    CORR(air_ball_rate, gb_hit_rate) AS air_ball_rate_corr
FROM t