WITH final AS (
    SELECT
        event_key,
        COALESCE(e.batted_contact_type != 'Unknown', FALSE) AS has_contact_type,
        COALESCE(e.batted_location_general != 'Unknown', FALSE) AS has_general_location,
        -- To avoid false negatives, we'll say that a fielder is present regardless
        -- if it's a ground-rule double or home run where there is location info
        COALESCE(
            e.batted_to_fielder != 0 OR (NOT rt.is_fielded AND has_general_location),
            FALSE
        ) AS has_batted_to_fielder,
        has_batted_to_fielder OR has_general_location AS has_any_location,
        -- Coverage of the fields below can't be determined at an event granularity, so
        -- the false negative issue above isn't a concern. Coverage should be inferred
        -- by having at least ~1 predicate-satisfying event over the course of
        -- a game or another appropriate sample.
        COALESCE(e.batted_location_depth != 'Default', FALSE) AS has_depth,
        COALESCE(e.batted_location_depth = 'ExtraDeep', FALSE) AS has_extra_deep_depth,
        COALESCE(e.batted_location_angle != 'Default', FALSE) AS has_angle,
        COALESCE(e.batted_location_angle = 'Foul', FALSE) AS has_foul_angle,

        COALESCE(e.batted_location_strength != 'Default', FALSE) AS has_strength,
        COALESCE(lt.is_mid_position, FALSE) AS has_mid_position,
        COALESCE(
            ct.broad_classification = 'GroundBall' AND e.batted_location_general IS NOT NULL,
            FALSE
        ) AS has_general_location_groundball,
        COALESCE(
            ct.broad_classification = 'AirBall' AND e.batted_location_general IS NOT NULL,
            FALSE
        ) AS has_general_location_airball,
        COALESCE(
            ct.broad_classification = 'GroundBall' AND lt.is_mid_position,
            FALSE
        ) AS has_mid_position_groundball,
        COALESCE(
            ct.broad_classification = 'AirBall' AND lt.is_mid_position,
            FALSE
        ) AS has_mid_position_airball,
        COALESCE(
            ct.broad_classification = 'GroundBall' AND e.batted_location_depth != 'Default',
            FALSE
        ) AS has_depth_groundball,
        COALESCE(
            ct.broad_classification = 'AirBall' AND e.batted_location_depth != 'Default',
            FALSE
        ) AS has_depth_airball,
        COALESCE(
            ct.broad_classification = 'GroundBall' AND e.batted_location_angle != 'Default',
            FALSE
        ) AS has_angle_groundball,
        COALESCE(
            ct.broad_classification = 'AirBall' AND e.batted_location_angle != 'Default',
            FALSE
        ) AS has_angle_airball,
        COALESCE(
            ct.broad_classification = 'GroundBall' AND e.batted_location_strength != 'Default',
            FALSE
        ) AS has_strength_groundball,
        COALESCE(
            ct.broad_classification = 'AirBall' AND e.batted_location_strength != 'Default',
            FALSE
        ) AS has_strength_airball,
        -- Based on the location diagram, any outside-the-park home run
        -- from left-center to right-center should be Deep or ExtraDeep in almost any park.
        -- A default (or shallow) batted_location_depth is a strong indicator
        -- that the batted_location_depth info is partially or entirely missing from the sample.
        (
            plate_appearance_result = 'HomeRun'
            AND lt.category_edge = 'Middle'
            AND e.batted_location_depth NOT LIKE '%Deep'
        ) AS has_misclassified_home_run_distance,
    FROM {{ ref('stg_events') }} e
    INNER JOIN {{ ref('seed_plate_appearance_result_types') }} AS rt USING (plate_appearance_result)
    LEFT JOIN {{ ref('seed_hit_location_categories') }} AS lt USING (batted_location_general)
    LEFT JOIN {{ ref('seed_plate_appearance_contact_types') }} AS ct USING (batted_contact_type)
)

SELECT * FROM final
