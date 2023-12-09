
WITH initial_sum AS (
    SELECT
        game_id,
        team_id,
        
            SUM(plate_appearances)::USMALLINT AS plate_appearances,
        
            SUM(at_bats)::USMALLINT AS at_bats,
        
            SUM(hits)::USMALLINT AS hits,
        
            SUM(singles)::USMALLINT AS singles,
        
            SUM(doubles)::USMALLINT AS doubles,
        
            SUM(triples)::USMALLINT AS triples,
        
            SUM(home_runs)::USMALLINT AS home_runs,
        
            SUM(total_bases)::USMALLINT AS total_bases,
        
            SUM(strikeouts)::USMALLINT AS strikeouts,
        
            SUM(walks)::USMALLINT AS walks,
        
            SUM(intentional_walks)::USMALLINT AS intentional_walks,
        
            SUM(hit_by_pitches)::USMALLINT AS hit_by_pitches,
        
            SUM(sacrifice_hits)::USMALLINT AS sacrifice_hits,
        
            SUM(sacrifice_flies)::USMALLINT AS sacrifice_flies,
        
            SUM(reached_on_errors)::USMALLINT AS reached_on_errors,
        
            SUM(reached_on_interferences)::USMALLINT AS reached_on_interferences,
        
            SUM(inside_the_park_home_runs)::USMALLINT AS inside_the_park_home_runs,
        
            SUM(ground_rule_doubles)::USMALLINT AS ground_rule_doubles,
        
            SUM(infield_hits)::USMALLINT AS infield_hits,
        
            SUM(on_base_opportunities)::USMALLINT AS on_base_opportunities,
        
            SUM(on_base_successes)::USMALLINT AS on_base_successes,
        
            SUM(runs_batted_in)::USMALLINT AS runs_batted_in,
        
            SUM(grounded_into_double_plays)::USMALLINT AS grounded_into_double_plays,
        
            SUM(double_plays)::USMALLINT AS double_plays,
        
            SUM(triple_plays)::USMALLINT AS triple_plays,
        
            SUM(batting_outs)::USMALLINT AS batting_outs,
        
            SUM(balls_in_play)::USMALLINT AS balls_in_play,
        
            SUM(balls_batted)::USMALLINT AS balls_batted,
        
            SUM(trajectory_fly_ball)::USMALLINT AS trajectory_fly_ball,
        
            SUM(trajectory_ground_ball)::USMALLINT AS trajectory_ground_ball,
        
            SUM(trajectory_line_drive)::USMALLINT AS trajectory_line_drive,
        
            SUM(trajectory_pop_up)::USMALLINT AS trajectory_pop_up,
        
            SUM(trajectory_unknown)::USMALLINT AS trajectory_unknown,
        
            SUM(trajectory_known)::USMALLINT AS trajectory_known,
        
            SUM(trajectory_broad_air_ball)::USMALLINT AS trajectory_broad_air_ball,
        
            SUM(trajectory_broad_ground_ball)::USMALLINT AS trajectory_broad_ground_ball,
        
            SUM(trajectory_broad_unknown)::USMALLINT AS trajectory_broad_unknown,
        
            SUM(trajectory_broad_known)::USMALLINT AS trajectory_broad_known,
        
            SUM(bunts)::USMALLINT AS bunts,
        
            SUM(batted_distance_plate)::USMALLINT AS batted_distance_plate,
        
            SUM(batted_distance_infield)::USMALLINT AS batted_distance_infield,
        
            SUM(batted_distance_outfield)::USMALLINT AS batted_distance_outfield,
        
            SUM(batted_distance_unknown)::USMALLINT AS batted_distance_unknown,
        
            SUM(batted_distance_known)::USMALLINT AS batted_distance_known,
        
            SUM(fielded_by_battery)::USMALLINT AS fielded_by_battery,
        
            SUM(fielded_by_infielder)::USMALLINT AS fielded_by_infielder,
        
            SUM(fielded_by_outfielder)::USMALLINT AS fielded_by_outfielder,
        
            SUM(fielded_by_known)::USMALLINT AS fielded_by_known,
        
            SUM(fielded_by_unknown)::USMALLINT AS fielded_by_unknown,
        
            SUM(batted_angle_left)::USMALLINT AS batted_angle_left,
        
            SUM(batted_angle_right)::USMALLINT AS batted_angle_right,
        
            SUM(batted_angle_middle)::USMALLINT AS batted_angle_middle,
        
            SUM(batted_angle_unknown)::USMALLINT AS batted_angle_unknown,
        
            SUM(batted_angle_known)::USMALLINT AS batted_angle_known,
        
            SUM(batted_location_plate)::USMALLINT AS batted_location_plate,
        
            SUM(batted_location_right_infield)::USMALLINT AS batted_location_right_infield,
        
            SUM(batted_location_middle_infield)::USMALLINT AS batted_location_middle_infield,
        
            SUM(batted_location_left_infield)::USMALLINT AS batted_location_left_infield,
        
            SUM(batted_location_left_field)::USMALLINT AS batted_location_left_field,
        
            SUM(batted_location_center_field)::USMALLINT AS batted_location_center_field,
        
            SUM(batted_location_right_field)::USMALLINT AS batted_location_right_field,
        
            SUM(batted_location_unknown)::USMALLINT AS batted_location_unknown,
        
            SUM(batted_location_known)::USMALLINT AS batted_location_known,
        
            SUM(batted_balls_pulled)::USMALLINT AS batted_balls_pulled,
        
            SUM(batted_balls_opposite_field)::USMALLINT AS batted_balls_opposite_field,
        
            SUM(runs)::USMALLINT AS runs,
        
            SUM(times_reached_base)::USMALLINT AS times_reached_base,
        
            SUM(times_lead_runner)::USMALLINT AS times_lead_runner,
        
            SUM(times_force_on_runner)::USMALLINT AS times_force_on_runner,
        
            SUM(times_next_base_empty)::USMALLINT AS times_next_base_empty,
        
            SUM(stolen_base_opportunities)::USMALLINT AS stolen_base_opportunities,
        
            SUM(stolen_base_opportunities_second)::USMALLINT AS stolen_base_opportunities_second,
        
            SUM(stolen_base_opportunities_third)::USMALLINT AS stolen_base_opportunities_third,
        
            SUM(stolen_base_opportunities_home)::USMALLINT AS stolen_base_opportunities_home,
        
            SUM(stolen_bases)::USMALLINT AS stolen_bases,
        
            SUM(stolen_bases_second)::USMALLINT AS stolen_bases_second,
        
            SUM(stolen_bases_third)::USMALLINT AS stolen_bases_third,
        
            SUM(stolen_bases_home)::USMALLINT AS stolen_bases_home,
        
            SUM(caught_stealing)::USMALLINT AS caught_stealing,
        
            SUM(caught_stealing_second)::USMALLINT AS caught_stealing_second,
        
            SUM(caught_stealing_third)::USMALLINT AS caught_stealing_third,
        
            SUM(caught_stealing_home)::USMALLINT AS caught_stealing_home,
        
            SUM(picked_off)::USMALLINT AS picked_off,
        
            SUM(picked_off_first)::USMALLINT AS picked_off_first,
        
            SUM(picked_off_second)::USMALLINT AS picked_off_second,
        
            SUM(picked_off_third)::USMALLINT AS picked_off_third,
        
            SUM(picked_off_caught_stealing)::USMALLINT AS picked_off_caught_stealing,
        
            SUM(outs_on_basepaths)::USMALLINT AS outs_on_basepaths,
        
            SUM(unforced_outs_on_basepaths)::USMALLINT AS unforced_outs_on_basepaths,
        
            SUM(outs_avoided_on_errors)::USMALLINT AS outs_avoided_on_errors,
        
            SUM(advances_on_wild_pitches)::USMALLINT AS advances_on_wild_pitches,
        
            SUM(advances_on_passed_balls)::USMALLINT AS advances_on_passed_balls,
        
            SUM(advances_on_balks)::USMALLINT AS advances_on_balks,
        
            SUM(advances_on_unspecified_plays)::USMALLINT AS advances_on_unspecified_plays,
        
            SUM(advances_on_defensive_indifference)::USMALLINT AS advances_on_defensive_indifference,
        
            SUM(advances_on_errors)::USMALLINT AS advances_on_errors,
        
            SUM(plate_appearances_while_on_base)::USMALLINT AS plate_appearances_while_on_base,
        
            SUM(balls_in_play_while_running)::USMALLINT AS balls_in_play_while_running,
        
            SUM(balls_in_play_while_on_base)::USMALLINT AS balls_in_play_while_on_base,
        
            SUM(batter_total_bases_while_running)::USMALLINT AS batter_total_bases_while_running,
        
            SUM(batter_total_bases_while_on_base)::USMALLINT AS batter_total_bases_while_on_base,
        
            SUM(extra_base_chances)::USMALLINT AS extra_base_chances,
        
            SUM(extra_base_advance_attempts)::USMALLINT AS extra_base_advance_attempts,
        
            SUM(extra_bases_taken)::USMALLINT AS extra_bases_taken,
        
            SUM(bases_advanced)::USMALLINT AS bases_advanced,
        
            SUM(bases_advanced_on_balls_in_play)::USMALLINT AS bases_advanced_on_balls_in_play,
        
            SUM(surplus_bases_advanced_on_balls_in_play)::INT1 AS surplus_bases_advanced_on_balls_in_play,
        
            SUM(outs_on_extra_base_advance_attempts)::USMALLINT AS outs_on_extra_base_advance_attempts,
        
            SUM(pitches)::USMALLINT AS pitches,
        
            SUM(swings)::USMALLINT AS swings,
        
            SUM(swings_with_contact)::USMALLINT AS swings_with_contact,
        
            SUM(strikes)::USMALLINT AS strikes,
        
            SUM(strikes_called)::USMALLINT AS strikes_called,
        
            SUM(strikes_swinging)::USMALLINT AS strikes_swinging,
        
            SUM(strikes_foul)::USMALLINT AS strikes_foul,
        
            SUM(strikes_foul_tip)::USMALLINT AS strikes_foul_tip,
        
            SUM(strikes_in_play)::USMALLINT AS strikes_in_play,
        
            SUM(strikes_unknown)::USMALLINT AS strikes_unknown,
        
            SUM(balls)::USMALLINT AS balls,
        
            SUM(balls_called)::USMALLINT AS balls_called,
        
            SUM(balls_intentional)::USMALLINT AS balls_intentional,
        
            SUM(balls_automatic)::USMALLINT AS balls_automatic,
        
            SUM(unknown_pitches)::USMALLINT AS unknown_pitches,
        
            SUM(pitchouts)::USMALLINT AS pitchouts,
        
            SUM(pitcher_pickoff_attempts)::USMALLINT AS pitcher_pickoff_attempts,
        
            SUM(catcher_pickoff_attempts)::USMALLINT AS catcher_pickoff_attempts,
        
            SUM(pitches_blocked_by_catcher)::USMALLINT AS pitches_blocked_by_catcher,
        
            SUM(pitches_with_runners_going)::USMALLINT AS pitches_with_runners_going,
        
            SUM(passed_balls)::USMALLINT AS passed_balls,
        
            SUM(wild_pitches)::USMALLINT AS wild_pitches,
        
            SUM(balks)::USMALLINT AS balks,
        
            SUM(left_on_base)::USMALLINT AS left_on_base,
        
            SUM(left_on_base_with_two_outs)::USMALLINT AS left_on_base_with_two_outs,
        
    FROM "timeball"."main_models"."player_game_offense_stats"
    GROUP BY 1, 2
),

-- A few definitions change when aggregated at the team level
final AS (
    SELECT 
        * REPLACE (
            left_on_base_with_two_outs AS left_on_base,
    )
    FROM initial_sum
)

SELECT * FROM final