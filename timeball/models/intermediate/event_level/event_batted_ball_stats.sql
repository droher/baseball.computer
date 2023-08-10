{{
  config(
    materialized = 'table',
    )
}}
WITH joined AS (
    SELECT
        batted_ball.event_key,
        batted_ball.contact,
        contact.primary_classification,
        contact.broad_classification,
        contact.is_bunt,
        COALESCE(loc.category_depth, fielder.category_depth) AS category_depth,
        COALESCE(loc.category_side, fielder.category_side) AS category_side,
        loc.category_edge,
    FROM {{ ref('stg_event_batted_ball_info') }} AS batted_ball
    LEFT JOIN {{ ref('seed_plate_appearance_contact_types') }} AS contact USING (contact)
    LEFT JOIN {{ ref('seed_hit_location_categories') }} AS loc USING (general_location)
    LEFT JOIN {{ ref('seed_hit_to_fielder_categories') }} AS fielder USING (hit_to_fielder)
),

final AS (
    SELECT
        event_key,
        1 AS balls_in_play,
        CASE WHEN primary_classification = 'FlyBall' THEN 1 ELSE 0 END AS batted_type_fly_ball,
        CASE WHEN primary_classification = 'GroundBall' THEN 1 ELSE 0 END AS batted_type_ground_ball,
        CASE WHEN primary_classification = 'LineDrive' THEN 1 ELSE 0 END AS batted_type_line_drive,
        CASE
            WHEN COALESCE(primary_classification, 'Unknown') = 'Unknown' THEN 1 ELSE 0
        END AS batted_type_unknown,
        CASE WHEN broad_classification = 'AirBall' THEN 1 ELSE 0 END AS batted_broad_type_air_ball,
        CASE
            WHEN broad_classification = 'GroundBall' THEN 1 ELSE 0
        END AS batted_broad_type_ground_ball,
        CASE
            WHEN COALESCE(broad_classification, 'Unknown') = 'Unknown' THEN 1 ELSE 0
        END AS batted_broad_type_unknown,
        CASE WHEN contact = 'PopFly' THEN 1 ELSE 0 END AS batted_subtype_pop_fly,
        CASE WHEN contact = 'Fly' THEN 1 ELSE 0 END AS batted_subtype_non_pop_fly,
        CASE WHEN is_bunt THEN 1 ELSE 0 END AS bunts,
        -- Distances,
        CASE WHEN category_depth = 'Plate' THEN 1 ELSE 0 END AS batted_distance_home_plate,
        CASE WHEN category_depth = 'Infield' THEN 1 ELSE 0 END AS batted_distance_infield,
        CASE WHEN category_depth = 'Outfield' THEN 1 ELSE 0 END AS batted_distance_outfield,
        CASE
            WHEN COALESCE(category_depth, 'Unknown') = 'Unknown' THEN 1 ELSE 0
        END AS batted_distance_unknown,
        CASE WHEN category_side = 'Left' THEN 1 ELSE 0 END AS batted_angle_left,
        CASE WHEN category_side = 'Right' THEN 1 ELSE 0 END AS batted_angle_right,
        CASE WHEN category_side = 'Middle' THEN 1 ELSE 0 END AS batted_angle_middle,
        CASE
            WHEN COALESCE(category_side, 'Unknown') IN ('All', 'Unknown') THEN 1 ELSE 0
        END AS batted_angle_unknown,
        -- All angle_* cols are mutually exclusive,
        -- but hits down foul line can be true at the same time as batted_angle_left/right
        CASE WHEN category_edge = 'Corner' THEN 1 ELSE 0 END AS batted_down_foul_line,
        CASE WHEN category_depth IS NOT NULL AND category_edge IS NULL THEN 1 ELSE 0
        END AS fielder_derived_location,
    FROM joined
)

SELECT * FROM final
