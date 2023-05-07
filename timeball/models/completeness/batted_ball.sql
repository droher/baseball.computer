WITH result_types AS (
    SELECT * FROM {{ ref('plate_appearance_result_types') }}
),

plate_appearances AS (
    SELECT * FROM {{ ref('stg_event_plate_appearances') }}
),

hit_locations AS (
    SELECT * FROM {{ ref('stg_event_hit_locations') }}
),

location_types AS (
    SELECT * FROM {{ ref('hit_location_categories') }}
),

contact_types AS (
    SELECT * FROM {{ ref('plate_appearance_contact_types') }}
),

final AS (
    SELECT
        event_key,
        COALESCE(pa.contact != 'Unknown', FALSE) AS has_contact,
        hl.general_location IS NOT NULL AS has_general_location,
        -- To avoid false negatives, we'll say that a fielder is present regardless
        -- if it's a ground-rule double or home run where there is location info
        COALESCE(
            pa.hit_to_fielder != 0 OR (NOT rt.is_fielded AND has_general_location),
            FALSE
        ) AS has_hit_to_fielder,
        has_hit_to_fielder OR has_general_location AS has_any_location,
        -- Coverage of the fields below can't be determined at an event level, so
        -- the false negative issue above isn't a concern. Coverage should be inferred
        -- by having at least ~1 predicate-satisfying event over the course of
        -- a game or another appropriate sample.
        COALESCE(hl.depth != 'Default', FALSE) AS has_depth,
        COALESCE(hl.depth = 'ExtraDeep', FALSE) AS has_extra_deep_depth,
        COALESCE(hl.angle != 'Default', FALSE) AS has_angle,
        COALESCE(hl.angle = 'Foul', FALSE) AS has_foul_angle,

        COALESCE(hl.strength != 'Default', FALSE) AS has_strength,
        COALESCE(lt.is_mid_position, FALSE) AS has_mid_position,
        COALESCE(
            ct.broad_classification = 'GroundBall' AND hl.general_location IS NOT NULL,
            FALSE
        ) AS has_general_location_groundball,
        COALESCE(
            ct.broad_classification = 'AirBall' AND hl.general_location IS NOT NULL,
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
            ct.broad_classification = 'GroundBall' AND hl.depth != 'Default',
            FALSE
        ) AS has_depth_groundball,
        COALESCE(
            ct.broad_classification = 'AirBall' AND hl.depth != 'Default',
            FALSE
        ) AS has_depth_airball,
        COALESCE(
            ct.broad_classification = 'GroundBall' AND hl.angle != 'Default',
            FALSE
        ) AS has_angle_groundball,
        COALESCE(
            ct.broad_classification = 'AirBall' AND hl.angle != 'Default',
            FALSE
        ) AS has_angle_airball,
        COALESCE(
            ct.broad_classification = 'GroundBall' AND hl.strength != 'Default',
            FALSE
        ) AS has_strength_groundball,
        COALESCE(
            ct.broad_classification = 'AirBall' AND hl.strength != 'Default',
            FALSE
        ) AS has_strength_airball,
        -- Based on the location diagram, any outside-the-park home run
        -- from left-center to right-center should be Deep or ExtraDeep in almost any park.
        -- A default (or shallow) depth is a strong indicator
        -- that the depth info is partially or entirely missing from the sample.
        (
            plate_appearance_result = 'HomeRun'
            AND lt.category_edge = 'Middle'
            AND hl.depth NOT LIKE '%Deep'
        ) AS has_misclassified_home_run_distance,
    FROM plate_appearances AS pa
    INNER JOIN result_types AS rt USING (plate_appearance_result)
    LEFT JOIN hit_locations AS hl USING (event_key)
    LEFT JOIN location_types AS lt USING (general_location)
    LEFT JOIN contact_types AS ct USING (contact)
    WHERE rt.is_in_play
)

SELECT * FROM final
