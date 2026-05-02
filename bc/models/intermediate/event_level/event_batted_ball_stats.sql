MODEL (
  name main_models.event_batted_ball_stats,
  kind FULL,
  grain (event_key),
  columns (
    event_key UINTEGER,
    balls_batted UTINYINT,
    balls_in_play UTINYINT,
    trajectory_fly_ball UTINYINT,
    trajectory_ground_ball UTINYINT,
    trajectory_line_drive UTINYINT,
    trajectory_pop_up UTINYINT,
    trajectory_unknown UTINYINT,
    trajectory_known UTINYINT,
    trajectory_broad_air_ball UTINYINT,
    trajectory_broad_ground_ball UTINYINT,
    trajectory_broad_unknown UTINYINT,
    trajectory_broad_known UTINYINT,
    bunts UTINYINT,
    batted_distance_plate UTINYINT,
    batted_distance_infield UTINYINT,
    batted_distance_outfield UTINYINT,
    batted_distance_unknown UTINYINT,
    batted_distance_known UTINYINT,
    fielded_by_battery UTINYINT,
    fielded_by_infielder UTINYINT,
    fielded_by_outfielder UTINYINT,
    fielded_by_unknown UTINYINT,
    fielded_by_known UTINYINT,
    batted_angle_left UTINYINT,
    batted_angle_right UTINYINT,
    batted_angle_middle UTINYINT,
    batted_angle_unknown UTINYINT,
    batted_angle_known UTINYINT,
    batted_location_plate UTINYINT,
    batted_location_right_infield UTINYINT,
    batted_location_left_infield UTINYINT,
    batted_location_middle_infield UTINYINT,
    batted_location_left_field UTINYINT,
    batted_location_center_field UTINYINT,
    batted_location_right_field UTINYINT,
    batted_location_unknown UTINYINT,
    batted_location_known UTINYINT,
    batted_balls_pulled UTINYINT,
    batted_balls_opposite_field UTINYINT
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    balls_batted = @doc('balls_batted'),
    balls_in_play = @doc('balls_in_play'),
    trajectory_fly_ball = @doc('trajectory_fly_ball'),
    trajectory_ground_ball = @doc('trajectory_ground_ball'),
    trajectory_line_drive = @doc('trajectory_line_drive'),
    trajectory_pop_up = @doc('trajectory_pop_up'),
    trajectory_unknown = @doc('trajectory_unknown'),
    trajectory_known = @doc('trajectory_known'),
    trajectory_broad_air_ball = @doc('trajectory_broad_air_ball'),
    trajectory_broad_ground_ball = @doc('trajectory_broad_ground_ball'),
    trajectory_broad_unknown = @doc('trajectory_broad_unknown'),
    trajectory_broad_known = @doc('trajectory_broad_known'),
    bunts = @doc('bunts'),
    batted_distance_plate = @doc('batted_distance_plate'),
    batted_distance_infield = @doc('batted_distance_infield'),
    batted_distance_outfield = @doc('batted_distance_outfield'),
    batted_distance_unknown = @doc('batted_distance_unknown'),
    batted_distance_known = @doc('batted_distance_known'),
    fielded_by_battery = @doc('fielded_by_battery'),
    fielded_by_infielder = @doc('fielded_by_infielder'),
    fielded_by_outfielder = @doc('fielded_by_outfielder'),
    fielded_by_unknown = @doc('fielded_by_unknown'),
    fielded_by_known = @doc('fielded_by_known'),
    batted_angle_left = @doc('batted_angle_left'),
    batted_angle_right = @doc('batted_angle_right'),
    batted_angle_middle = @doc('batted_angle_middle'),
    batted_angle_unknown = @doc('batted_angle_unknown'),
    batted_angle_known = @doc('batted_angle_known'),
    batted_location_plate = @doc('batted_location_plate'),
    batted_location_right_infield = @doc('batted_location_right_infield'),
    batted_location_left_infield = @doc('batted_location_left_infield'),
    batted_location_middle_infield = @doc('batted_location_middle_infield'),
    batted_location_left_field = @doc('batted_location_left_field'),
    batted_location_center_field = @doc('batted_location_center_field'),
    batted_location_right_field = @doc('batted_location_right_field'),
    batted_location_unknown = @doc('batted_location_unknown'),
    batted_location_known = @doc('batted_location_known'),
    batted_balls_pulled = @doc('batted_balls_pulled'),
    batted_balls_opposite_field = @doc('batted_balls_opposite_field')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_batted_ball_stats.parquet'
  ),
);







WITH batted_ball AS (
    SELECT
        event_key,
        1::UTINYINT AS balls_batted,
        (batted_to_fielder IS NOT NULL)::UTINYINT AS balls_in_play,
        CASE WHEN trajectory = 'Fly' THEN 1 ELSE 0 END::UTINYINT AS trajectory_fly_ball,
        CASE WHEN trajectory = 'GroundBall' THEN 1 ELSE 0 END::UTINYINT AS trajectory_ground_ball,
        CASE WHEN trajectory = 'LineDrive' THEN 1 ELSE 0 END::UTINYINT AS trajectory_line_drive,
        CASE WHEN trajectory = 'PopUp' THEN 1 ELSE 0 END::UTINYINT AS trajectory_pop_up,
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
    FROM main_models.calc_batted_ball_type
),

final AS (
    SELECT
        batted_ball.*,
        CASE WHEN hand.batter_hand = 'R'
                THEN batted_ball.batted_angle_left
            WHEN hand.batter_hand = 'L'
                THEN batted_ball.batted_angle_right
            ELSE 0
        END::UTINYINT AS batted_balls_pulled,
        CASE WHEN hand.batter_hand = 'R'
                THEN batted_ball.batted_angle_right
            WHEN hand.batter_hand = 'L'
                THEN batted_ball.batted_angle_left
            ELSE 0
        END::UTINYINT AS batted_balls_opposite_field,

    FROM batted_ball
    INNER JOIN main_models.event_states_batter_pitcher AS hand USING (event_key)
)

SELECT * FROM final
