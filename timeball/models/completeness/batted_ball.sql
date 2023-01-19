WITH result_types AS (
    SELECT *
    FROM {{ ref('plate_appearance_result_types') }}
    WHERE is_in_play
),

plate_appearances AS (
    SELECT *
    FROM {{ ref('event_plate_appearances') }}
    WHERE plate_appearance_result IN (SELECT name FROM result_types)
),

hit_locations AS (
    SELECT * FROM {{ ref('event_hit_locations') }}
),

final AS (
    SELECT
        event_key,
        COALESCE(pa.hit_to_fielder != 'Unknown' OR NOT rt.is_fielded, FALSE) AS has_hit_to_fielder,
        hl.general_location IS NOT NULL AS has_general_location,
        has_hit_to_fielder OR has_general_location AS has_any_location,
        hl.depth != 'Default' AS has_non_default_depth,
        hl.angle != 'Default' AS has_non_default_angle,
        hl.strength != 'Default' AS has_non_default_strength,
        COALESCE(
            has_non_default_depth OR has_non_default_angle OR has_non_default_strength, FALSE
        ) AS has_any_location_modifier
    FROM plate_appearances AS pa
    INNER JOIN result_types AS rt ON pa.plate_appearance_result = rt.name
    LEFT JOIN hit_locations AS hl USING (event_key)
)

SELECT * FROM final
