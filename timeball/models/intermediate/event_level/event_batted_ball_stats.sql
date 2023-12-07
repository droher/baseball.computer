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
        CASE WHEN trajectory = 'Fly' THEN 1 ELSE 0 END::UTINYINT AS trajectory_fly_ball,
        CASE WHEN trajectory = 'GroundBall' THEN 1 ELSE 0 END::UTINYINT AS trajectory_ground_ball,
        CASE WHEN trajectory = 'LineDrive' THEN 1 ELSE 0 END::UTINYINT AS trajectory_line_drive,
        CASE WHEN trajectory = 'PopFly' THEN 1 ELSE 0 END::UTINYINT AS trajectory_pop_fly,
        CASE
            WHEN COALESCE(trajectory, 'Unknown') = 'Unknown' THEN 1 ELSE 0
        END::UTINYINT AS trajectory_unknown,
        (1 - trajectory_unknown)::UTINYINT AS trajectory_known,
        CASE WHEN trajectory_broad_classification = 'AirBall' THEN 1 ELSE 0 END::UTINYINT AS trajectory_broad_air_ball,
        CASE
            WHEN trajectory_broad_classification = 'GroundBall' THEN 1 ELSE 0
        END::UTINYINT AS trajectory_broad_ground_ball,
        CASE
            WHEN COALESCE(trajectory_broad_classification, 'Unknown') = 'Unknown' THEN 1 ELSE 0
        END::UTINYINT AS trajectory_broad_unknown,
        (1 - trajectory_broad_unknown)::UTINYINT AS trajectory_broad_known,
        CASE WHEN trajectory_broad_classification = 'Bunt' THEN 1 ELSE 0 END::UTINYINT AS bunts,
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
