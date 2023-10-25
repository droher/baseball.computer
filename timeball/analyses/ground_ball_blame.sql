WITH t AS (
SELECT
    p.batter_hand,
    c.recorded_location,
    c.recorded_location_angle,
    --c.recorded_location_angle,
    COUNT(*) AS at_bats,
    COUNT_IF(batted_to_fielder BETWEEN 1 AND 6) AS to_infield,
    COUNT_IF(batted_to_fielder BETWEEN 7 AND 9) AS to_outfield,
    COUNT_IF(c.batted_to_fielder = 1)/to_infield AS to_p,
    COUNT_IF(c.batted_to_fielder = 3)/to_infield AS to_1b,
    COUNT_IF(c.batted_to_fielder = 4)/to_infield AS to_2b,
    COUNT_IF(c.batted_to_fielder = 5)/to_infield AS to_3b,
    COUNT_IF(c.batted_to_fielder = 6)/to_infield AS to_ss,
    COUNT_IF(c.batted_to_fielder = 7)/to_outfield AS to_lf,
    COUNT_IF(c.batted_to_fielder = 8)/to_outfield AS to_cf,
    COUNT_IF(c.batted_to_fielder = 9)/to_outfield AS to_rf,
FROM {{ ref('event_offense_stats') }} AS e
INNER JOIN {{ ref('calc_batted_ball_type') }} AS c USING (event_key, game_id)
INNER JOIN {{ ref('event_states_full') }} p USING (event_key, game_id)
INNER JOIN {{ ref('player_game_data_completeness') }} AS g USING (game_id, player_id)
WHERE e.contact_type_ground_ball = 1
    AND g.player_type = 'BATTING'
    AND g.has_scoresheet_location AND g.has_batted_to_fielder AND g.has_contact_type
    AND g.season BETWEEN 1993 AND 1999
    --AND g.season >= 2020
    AND recorded_location_angle != 'Foul'

GROUP BY 1, 2, 3
)
, shares AS (
    SELECT
    *,
    (to_lf * to_outfield) / SUM(to_lf * to_outfield) OVER (PARTITION BY batter_hand) AS lf_share,
    (to_cf * to_outfield) / SUM(to_cf * to_outfield) OVER (PARTITION BY batter_hand) AS cf_share,
    (to_rf * to_outfield) / SUM(to_rf * to_outfield) OVER (PARTITION BY batter_hand) AS rf_share,
    to_3b * lf_share AS weighted_3b_lf,
    to_3b * cf_share AS weighted_3b_cf,
    to_3b * rf_share AS weighted_3b_rf,
    to_ss * lf_share AS weighted_ss_lf,
    to_ss * cf_share AS weighted_ss_cf,
    to_ss * rf_share AS weighted_ss_rf,
    to_2b * lf_share AS weighted_2b_lf,
    to_2b * cf_share AS weighted_2b_cf,
    to_2b * rf_share AS weighted_2b_rf,
    to_1b * cf_share AS weighted_1b_cf,
    to_1b * rf_share AS weighted_1b_rf,
    to_p * lf_share AS weighted_p_lf,
    to_p * cf_share AS weighted_p_cf,
    to_p * rf_share AS weighted_p_rf
FROM t
),

final_splits AS (
    SELECT batter_hand,
        SUM(weighted_3b_lf) AS weighted_3b_lf,
        SUM(weighted_3b_cf) AS weighted_3b_cf,
        SUM(weighted_3b_rf) AS weighted_3b_rf,
        SUM(weighted_ss_lf) AS weighted_ss_lf,
        SUM(weighted_ss_cf) AS weighted_ss_cf,
        SUM(weighted_ss_rf) AS weighted_ss_rf,
        SUM(weighted_2b_lf) AS weighted_2b_lf,
        SUM(weighted_2b_cf) AS weighted_2b_cf,
        SUM(weighted_2b_rf) AS weighted_2b_rf,
        SUM(weighted_1b_cf) AS weighted_1b_cf,
        SUM(weighted_1b_rf) AS weighted_1b_rf,
        SUM(weighted_p_lf) AS weighted_p_lf,
        SUM(weighted_p_cf) AS weighted_p_cf,
        SUM(weighted_p_rf) AS weighted_p_rf
    FROM shares
    GROUP BY 1
)
SELECT * FROM final_splits
--ORDER BY SUM(at_bats) OVER (PARTITION BY recorded_location) DESC, SUM(at_bats) OVER (PARTITION BY recorded_location, recorded_location_angle), batter_hand