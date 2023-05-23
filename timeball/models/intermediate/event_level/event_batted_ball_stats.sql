WITH plate_appearances AS (
    SELECT *
    FROM {{ ref('stg_event_plate_appearances') }}
),

batted_ball_info AS (
    SELECT *
    FROM {{ ref('stg_event_batted_ball_info') }}
),

result_types AS (
    SELECT *
    FROM {{ ref('plate_appearance_result_types') }}
),

contact_types AS (
    SELECT *
    FROM {{ ref('plate_appearance_contact_types') }}
),

location_types AS (
    SELECT *
    FROM {{ ref('hit_location_categories') }}
),

joined AS (
    SELECT
        pa.*,
        bbi.*,
        ct.*,
        lt.*,
        rt.*
    FROM plate_appearances AS pa
    INNER JOIN result_types AS rt USING (plate_appearance_result)
    INNER JOIN batted_ball_info AS bbi USING (event_key)
    LEFT JOIN contact_types AS ct USING (contact)
    LEFT JOIN location_types AS lt USING (general_location)
),

final AS (
    SELECT
        event_key,
        1 AS balls_in_play,
        CASE WHEN primary_classification = 'FlyBall' THEN 1 ELSE 0 END AS hits_type_fly_ball,
        CASE WHEN primary_classification = 'GroundBall' THEN 1 ELSE 0 END AS hits_type_ground_ball,
        CASE WHEN primary_classification = 'LineDrive' THEN 1 ELSE 0 END AS hits_type_line_drive,
        CASE
            WHEN COALESCE(primary_classification, 'Unknown') = 'Unknown' THEN 1 ELSE 0
        END AS hits_type_unknown,
        CASE WHEN broad_classification = 'AirBall' THEN 1 ELSE 0 END AS hits_broad_type_air_ball,
        CASE
            WHEN broad_classification = 'GroundBall' THEN 1 ELSE 0
        END AS hits_broad_type_ground_ball,
        CASE
            WHEN COALESCE(broad_classification, 'Unknown') = 'Unknown' THEN 1 ELSE 0
        END AS hits_broad_type_unknown,
        CASE WHEN contact = 'PopFly' THEN 1 ELSE 0 END AS hits_subtype_pop_fly,
        CASE WHEN contact = 'Fly' THEN 1 ELSE 0 END AS hits_subtype_non_pop_fly,
        CASE WHEN is_bunt THEN 1 ELSE 0 END AS bunts,
        -- Distances,
        CASE WHEN category_depth = 'Plate' THEN 1 ELSE 0 END AS hits_distance_home_plate,
        CASE WHEN category_depth = 'Infield' THEN 1 ELSE 0 END AS hits_distance_infield,
        CASE WHEN category_depth = 'Outfield' THEN 1 ELSE 0 END AS hits_distance_outfield,
        CASE
            WHEN COALESCE(category_depth, 'Unknown') = 'Unknown' THEN 1 ELSE 0
        END AS hits_distance_unknown,
        CASE WHEN category_side = 'Left' THEN 1 ELSE 0 END AS hits_angle_left,
        CASE WHEN category_side = 'Right' THEN 1 ELSE 0 END AS hits_angle_right,
        CASE WHEN category_side = 'Middle' THEN 1 ELSE 0 END AS hits_angle_middle,
        CASE
            WHEN COALESCE(category_side, 'Unknown') IN ('All', 'Unknown') THEN 1 ELSE 0
        END AS hits_angle_unknown,
        -- All angle_* cols are mutually exclusive,
        -- but hits down foul line can be true at the same time as hits_angle_left/right
        CASE WHEN category_edge = 'Corner' THEN 1 ELSE 0 END AS hits_down_foul_line,
    FROM joined
)

SELECT * FROM final
