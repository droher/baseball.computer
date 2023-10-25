WITH t AS (
    SELECT
        p.batter_hand,
        c.recorded_location,
        c.recorded_location_angle,
        c.recorded_location_depth,
        e.hits::BOOL AS is_hit,
        c.contact_broad_classification = 'AirBall' AS is_air_ball,
        COUNT(*) AS at_bats,
        COUNT_IF(e.fielded_by_known) AS known_fielder,
        COUNT_IF(c.batted_to_fielder = 1)/known_fielder AS to_p,
        COUNT_IF(c.batted_to_fielder = 2)/known_fielder AS to_c,
        COUNT_IF(c.batted_to_fielder = 3)/known_fielder AS to_1b,
        COUNT_IF(c.batted_to_fielder = 4)/known_fielder AS to_2b,
        COUNT_IF(c.batted_to_fielder = 5)/known_fielder AS to_3b,
        COUNT_IF(c.batted_to_fielder = 6)/known_fielder AS to_ss,
        COUNT_IF(c.batted_to_fielder = 7)/known_fielder AS to_lf,
        COUNT_IF(c.batted_to_fielder = 8)/known_fielder AS to_cf,
        COUNT_IF(c.batted_to_fielder = 9)/known_fielder AS to_rf,
        COUNT_IF(c.batted_to_fielder = 0)/COUNT(*) AS to_unknown,
    FROM {{ ref('event_offense_stats') }} AS e
    INNER JOIN {{ ref('calc_batted_ball_type') }} AS c USING (event_key, game_id)
    INNER JOIN {{ ref('event_states_full') }} p USING (event_key, game_id)
    INNER JOIN {{ ref('game_data_completeness') }} AS g USING (game_id)
    INNER JOIN {{ ref('seed_hit_location_categories') }} loc ON loc.batted_location_general = c.recorded_location
    WHERE g.has_location
        AND c.batted_to_fielder IS NOT NULL
        AND p.batter_hand IS NOT NULL
        AND g.season BETWEEN 1988 AND 1999
        AND contact_type_known
        -- Exclude cases where ball is often fielded in a different place than the hit location
        AND NOT (loc.category_depth != 'Outfield' AND fielded_by_outfielder = 1)
        GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT * FROM t
ORDER BY VARIANCE(to_ss) OVER (PARTITION BY recorded_location, recorded_location_angle, recorded_location_depth, batter_hand, is_air_ball) DESC, recorded_location, recorded_location_angle, recorded_location_depth, batter_hand, is_hit