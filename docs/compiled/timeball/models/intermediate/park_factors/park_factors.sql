

WITH final AS (
    SELECT
        b.season,
        b.park_id,
        b.league,
        b.basic_park_factor,
        
            a.singles_park_factor,
        
            a.doubles_park_factor,
        
            a.triples_park_factor,
        
            a.home_runs_park_factor,
        
            a.strikeouts_park_factor,
        
            a.walks_park_factor,
        
            a.batting_outs_park_factor,
        
            a.runs_park_factor,
        
            a.balls_in_play_park_factor,
        
            a.trajectory_fly_ball_park_factor,
        
            a.trajectory_ground_ball_park_factor,
        
            a.trajectory_line_drive_park_factor,
        
            a.trajectory_pop_up_park_factor,
        
            a.trajectory_unknown_park_factor,
        
            a.batted_distance_infield_park_factor,
        
            a.batted_distance_outfield_park_factor,
        
            a.batted_distance_unknown_park_factor,
        
            a.batted_angle_left_park_factor,
        
            a.batted_angle_right_park_factor,
        
            a.batted_angle_middle_park_factor,
        
        COALESCE(a.runs_park_factor, b.basic_park_factor) AS overall_park_factor
    FROM "timeball"."main_models"."calc_park_factors_basic" AS b
    LEFT JOIN "timeball"."main_models"."calc_park_factors_advanced" AS a USING (season, park_id, league)
)

SELECT * FROM final