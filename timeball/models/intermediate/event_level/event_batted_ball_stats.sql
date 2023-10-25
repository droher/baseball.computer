{{
  config(
    materialized = 'table',
    )
}}
WITH batted_ball AS (
    SELECT
        event_key,
        1::UTINYINT AS balls_batted,
        (batted_to_fielder IS NOT NULL)::UTINYINT AS balls_in_play,
        CASE WHEN contact = 'Fly' THEN 1 ELSE 0 END::UTINYINT AS contact_type_fly_ball,
        CASE WHEN contact = 'GroundBall' THEN 1 ELSE 0 END::UTINYINT AS contact_type_ground_ball,
        CASE WHEN contact = 'LineDrive' THEN 1 ELSE 0 END::UTINYINT AS contact_type_line_drive,
        CASE WHEN contact = 'PopFly' THEN 1 ELSE 0 END::UTINYINT AS contact_type_pop_fly,
        CASE
            WHEN COALESCE(contact, 'Unknown') = 'Unknown' THEN 1 ELSE 0
        END::UTINYINT AS contact_type_unknown,
        (1 - contact_type_unknown)::UTINYINT AS contact_type_known,
        CASE WHEN contact_broad_classification = 'AirBall' THEN 1 ELSE 0 END::UTINYINT AS contact_broad_type_air_ball,
        CASE
            WHEN contact_broad_classification = 'GroundBall' THEN 1 ELSE 0
        END::UTINYINT AS contact_broad_type_ground_ball,
        CASE
            WHEN COALESCE(contact_broad_classification, 'Unknown') = 'Unknown' THEN 1 ELSE 0
        END::UTINYINT AS contact_broad_type_unknown,
        CASE WHEN contact_broad_classification = 'Bunt' THEN 1 ELSE 0 END::UTINYINT AS bunts,
        -- Distances,
        CASE WHEN location_depth = 'Plate' THEN 1 ELSE 0 END::UTINYINT AS batted_distance_plate,
        CASE WHEN location_depth = 'Infield' THEN 1 ELSE 0 END::UTINYINT AS batted_distance_infield,
        CASE WHEN location_depth = 'Outfield' THEN 1 ELSE 0 END::UTINYINT AS batted_distance_outfield,
        CASE
            WHEN COALESCE(location_depth, 'Unknown') = 'Unknown' THEN 1 ELSE 0
        END::UTINYINT AS batted_distance_unknown,
        (1 - batted_distance_unknown)::UTINYINT AS batted_distance_known,
        CASE WHEN batted_to_fielder BETWEEN 1 AND 2 THEN 1 ELSE 0 END::UTINYINT AS fielded_by_battery,
        CASE WHEN batted_to_fielder BETWEEN 3 AND 6 THEN 1 ELSE 0 END::UTINYINT AS fielded_by_infielder,
        CASE WHEN batted_to_fielder BETWEEN 7 AND 9 THEN 1 ELSE 0 END::UTINYINT AS fielded_by_outfielder,
        CASE WHEN batted_to_fielder = 0 THEN 1 ELSE 0 END::UTINYINT AS fielded_by_unknown,
        (1 - fielded_by_unknown)::UTINYINT AS fielded_by_known,
        CASE WHEN location_side = 'Left' THEN 1 ELSE 0 END::UTINYINT AS batted_angle_left,
        CASE WHEN location_side = 'Right' THEN 1 ELSE 0 END::UTINYINT AS batted_angle_right,
        CASE WHEN location_side = 'Middle' THEN 1 ELSE 0 END::UTINYINT AS batted_angle_middle,
        CASE
            WHEN COALESCE(location_side, 'Unknown') IN ('All', 'Unknown') THEN 1 ELSE 0
        END::UTINYINT AS batted_angle_unknown,
        (1 - batted_angle_unknown)::UTINYINT AS batted_angle_known,
        -- More granular locations that are still sensitive to the groundball location problem.
        CASE WHEN batted_to_fielder = 2 THEN 1 ELSE 0 END::UTINYINT AS batted_location_plate,
        batted_distance_infield * batted_angle_right AS batted_location_right_infield,
        batted_distance_infield * batted_angle_left AS batted_location_left_infield,
        batted_distance_infield * batted_angle_middle AS batted_location_middle_infield,
        batted_distance_outfield * batted_angle_left AS batted_location_left_field,
        batted_distance_outfield * batted_angle_middle AS batted_location_center_field,
        batted_distance_outfield * batted_angle_right AS batted_location_right_field,
        CASE WHEN location_depth = 'Unknown' THEN 1 ELSE 0 END::UTINYINT AS batted_location_unknown,
        (1 - batted_location_unknown)::UTINYINT AS batted_location_known,
    FROM {{ ref('calc_batted_ball_type') }}
),

final AS (
    SELECT
        batted_ball.*,
        CASE WHEN hand.batter_hand = 'R'
                THEN batted_ball.batted_angle_left
            WHEN hand.batter_hand = 'L'
                THEN batted_ball.batted_angle_right
            ELSE 0
        END AS batted_balls_pulled,
        CASE WHEN hand.batter_hand = 'R'
                THEN batted_ball.batted_angle_right
            WHEN hand.batter_hand = 'L'
                THEN batted_ball.batted_angle_left
            ELSE 0
        END AS batted_balls_opposite_field,

    FROM batted_ball
    INNER JOIN {{ ref('event_states_batter_pitcher') }} AS hand USING (event_key)
)

SELECT * FROM final
