WITH t AS (
    SELECT
        p.batter_hand,
        p.season > 2010 AS is_shift_era,
        p.base_state_start AS base_state,
        p.outs_start < 2 AS under_two_outs,
        c.recorded_location,
        c.recorded_location_angle,
        c.recorded_location_depth,
        c.contact,
        c.batted_to_fielder,
        ANY_VALUE(contact_broad_classification) AS contact_broad_classification,
        COUNT(*) AS at_bats,
        COUNT_IF(e.hits = 1) AS hits,
    FROM {{ ref('event_offense_stats') }} AS e
    INNER JOIN {{ ref('calc_batted_ball_type') }} AS c USING (event_key, game_id)
    INNER JOIN {{ ref('event_states_full') }} p USING (event_key, game_id)
    WHERE c.batted_to_fielder > 0
        AND c.recorded_location != 'Unknown'
        AND p.batter_hand IS NOT NULL
        AND (p.season BETWEEN 1989 AND 1999 OR p.season >= 2000)
        AND e.contact_type_known = 1
        AND e.sacrifice_hits = 0
        AND p.batter_fielding_position != 1
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
), 

t2 AS (
    SELECT DISTINCT ON (batter_hand, is_shift_era, base_state, under_two_outs, recorded_location, recorded_location_angle, recorded_location_depth, batted_to_fielder, contact)
        batter_hand,
        is_shift_era,
        base_state,
        under_two_outs
        recorded_location,
        recorded_location_angle,
        recorded_location_depth,
        batted_to_fielder,
        contact,
        contact_broad_classification,
        SUM(at_bats) AS at_bats,
        SUM(hits)/SUM(at_bats) AS batting_average,
    FROM t
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    WINDOW
        s1 AS (PARTITION BY batter_hand, is_shift_era, base_state, under_two_outs, recorded_location, recorded_location_angle, recorded_location_depth, batted_to_fielder, contact),
        s2 AS (PARTITION BY batter_hand, recorded_location, recorded_location_angle, recorded_location_depth, batted_to_fielder, contact),
        s3 AS (PARTITION BY batter_hand, recorded_location, recorded_location_angle, recorded_location_depth, batted_to_fielder),
        s4 AS (PARTITION BY batter_hand, recorded_location, recorded_location_angle, recorded_location_depth),
        s5 AS (PARTITION BY batter_hand, recorded_location, recorded_location_angle),
        s6 AS (PARTITION BY batter_hand, recorded_location)
)

SELECT * FROM t2
ORDER BY at_bats DESC