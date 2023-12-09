
WITH box_score AS (
    SELECT
        CASE WHEN bat.side = 'Home' THEN games.home_team_id ELSE games.away_team_id END AS team_id,
        bat.*
    FROM "timeball"."main_models"."stg_box_score_batting_lines" AS bat
    -- This join ensures that we only get the box score lines for games that
    -- do not have an event file.
    INNER JOIN "timeball"."main_models"."game_start_info" AS games USING (game_id)
    WHERE games.source_type = 'BoxScore'
),

final AS (
    SELECT
        game_id,
        team_id,
        player_id,
        
            SUM(plate_appearances)::UTINYINT AS plate_appearances,
        
            SUM(at_bats)::UTINYINT AS at_bats,
        
            SUM(hits)::UTINYINT AS hits,
        
            SUM(singles)::UTINYINT AS singles,
        
            SUM(doubles)::UTINYINT AS doubles,
        
            SUM(triples)::UTINYINT AS triples,
        
            SUM(home_runs)::UTINYINT AS home_runs,
        
            SUM(total_bases)::UTINYINT AS total_bases,
        
            SUM(strikeouts)::UTINYINT AS strikeouts,
        
            SUM(walks)::UTINYINT AS walks,
        
            SUM(intentional_walks)::UTINYINT AS intentional_walks,
        
            SUM(hit_by_pitches)::UTINYINT AS hit_by_pitches,
        
            SUM(sacrifice_hits)::UTINYINT AS sacrifice_hits,
        
            SUM(sacrifice_flies)::UTINYINT AS sacrifice_flies,
        
            SUM(reached_on_errors)::UTINYINT AS reached_on_errors,
        
            SUM(reached_on_interferences)::UTINYINT AS reached_on_interferences,
        
            SUM(inside_the_park_home_runs)::UTINYINT AS inside_the_park_home_runs,
        
            SUM(ground_rule_doubles)::UTINYINT AS ground_rule_doubles,
        
            SUM(infield_hits)::UTINYINT AS infield_hits,
        
            SUM(on_base_opportunities)::UTINYINT AS on_base_opportunities,
        
            SUM(on_base_successes)::UTINYINT AS on_base_successes,
        
            SUM(runs_batted_in)::UTINYINT AS runs_batted_in,
        
            SUM(grounded_into_double_plays)::UTINYINT AS grounded_into_double_plays,
        
            SUM(double_plays)::UTINYINT AS double_plays,
        
            SUM(triple_plays)::UTINYINT AS triple_plays,
        
            SUM(batting_outs)::UTINYINT AS batting_outs,
        
            SUM(balls_in_play)::UTINYINT AS balls_in_play,
        
            SUM(balls_batted)::UTINYINT AS balls_batted,
        
            SUM(trajectory_fly_ball)::UTINYINT AS trajectory_fly_ball,
        
            SUM(trajectory_ground_ball)::UTINYINT AS trajectory_ground_ball,
        
            SUM(trajectory_line_drive)::UTINYINT AS trajectory_line_drive,
        
            SUM(trajectory_pop_up)::UTINYINT AS trajectory_pop_up,
        
            SUM(trajectory_unknown)::UTINYINT AS trajectory_unknown,
        
            SUM(trajectory_known)::UTINYINT AS trajectory_known,
        
            SUM(trajectory_broad_air_ball)::UTINYINT AS trajectory_broad_air_ball,
        
            SUM(trajectory_broad_ground_ball)::UTINYINT AS trajectory_broad_ground_ball,
        
            SUM(trajectory_broad_unknown)::UTINYINT AS trajectory_broad_unknown,
        
            SUM(trajectory_broad_known)::UTINYINT AS trajectory_broad_known,
        
            SUM(bunts)::UTINYINT AS bunts,
        
            SUM(batted_distance_plate)::UTINYINT AS batted_distance_plate,
        
            SUM(batted_distance_infield)::UTINYINT AS batted_distance_infield,
        
            SUM(batted_distance_outfield)::UTINYINT AS batted_distance_outfield,
        
            SUM(batted_distance_unknown)::UTINYINT AS batted_distance_unknown,
        
            SUM(batted_distance_known)::UTINYINT AS batted_distance_known,
        
            SUM(fielded_by_battery)::UTINYINT AS fielded_by_battery,
        
            SUM(fielded_by_infielder)::UTINYINT AS fielded_by_infielder,
        
            SUM(fielded_by_outfielder)::UTINYINT AS fielded_by_outfielder,
        
            SUM(fielded_by_known)::UTINYINT AS fielded_by_known,
        
            SUM(fielded_by_unknown)::UTINYINT AS fielded_by_unknown,
        
            SUM(batted_angle_left)::UTINYINT AS batted_angle_left,
        
            SUM(batted_angle_right)::UTINYINT AS batted_angle_right,
        
            SUM(batted_angle_middle)::UTINYINT AS batted_angle_middle,
        
            SUM(batted_angle_unknown)::UTINYINT AS batted_angle_unknown,
        
            SUM(batted_angle_known)::UTINYINT AS batted_angle_known,
        
            SUM(batted_location_plate)::UTINYINT AS batted_location_plate,
        
            SUM(batted_location_right_infield)::UTINYINT AS batted_location_right_infield,
        
            SUM(batted_location_middle_infield)::UTINYINT AS batted_location_middle_infield,
        
            SUM(batted_location_left_infield)::UTINYINT AS batted_location_left_infield,
        
            SUM(batted_location_left_field)::UTINYINT AS batted_location_left_field,
        
            SUM(batted_location_center_field)::UTINYINT AS batted_location_center_field,
        
            SUM(batted_location_right_field)::UTINYINT AS batted_location_right_field,
        
            SUM(batted_location_unknown)::UTINYINT AS batted_location_unknown,
        
            SUM(batted_location_known)::UTINYINT AS batted_location_known,
        
            SUM(batted_balls_pulled)::UTINYINT AS batted_balls_pulled,
        
            SUM(batted_balls_opposite_field)::UTINYINT AS batted_balls_opposite_field,
        
            SUM(runs)::UTINYINT AS runs,
        
            SUM(times_reached_base)::UTINYINT AS times_reached_base,
        
            SUM(times_lead_runner)::UTINYINT AS times_lead_runner,
        
            SUM(times_force_on_runner)::UTINYINT AS times_force_on_runner,
        
            SUM(times_next_base_empty)::UTINYINT AS times_next_base_empty,
        
            SUM(stolen_base_opportunities)::UTINYINT AS stolen_base_opportunities,
        
            SUM(stolen_base_opportunities_second)::UTINYINT AS stolen_base_opportunities_second,
        
            SUM(stolen_base_opportunities_third)::UTINYINT AS stolen_base_opportunities_third,
        
            SUM(stolen_base_opportunities_home)::UTINYINT AS stolen_base_opportunities_home,
        
            SUM(stolen_bases)::UTINYINT AS stolen_bases,
        
            SUM(stolen_bases_second)::UTINYINT AS stolen_bases_second,
        
            SUM(stolen_bases_third)::UTINYINT AS stolen_bases_third,
        
            SUM(stolen_bases_home)::UTINYINT AS stolen_bases_home,
        
            SUM(caught_stealing)::UTINYINT AS caught_stealing,
        
            SUM(caught_stealing_second)::UTINYINT AS caught_stealing_second,
        
            SUM(caught_stealing_third)::UTINYINT AS caught_stealing_third,
        
            SUM(caught_stealing_home)::UTINYINT AS caught_stealing_home,
        
            SUM(picked_off)::UTINYINT AS picked_off,
        
            SUM(picked_off_first)::UTINYINT AS picked_off_first,
        
            SUM(picked_off_second)::UTINYINT AS picked_off_second,
        
            SUM(picked_off_third)::UTINYINT AS picked_off_third,
        
            SUM(picked_off_caught_stealing)::UTINYINT AS picked_off_caught_stealing,
        
            SUM(outs_on_basepaths)::UTINYINT AS outs_on_basepaths,
        
            SUM(unforced_outs_on_basepaths)::UTINYINT AS unforced_outs_on_basepaths,
        
            SUM(outs_avoided_on_errors)::UTINYINT AS outs_avoided_on_errors,
        
            SUM(advances_on_wild_pitches)::UTINYINT AS advances_on_wild_pitches,
        
            SUM(advances_on_passed_balls)::UTINYINT AS advances_on_passed_balls,
        
            SUM(advances_on_balks)::UTINYINT AS advances_on_balks,
        
            SUM(advances_on_unspecified_plays)::UTINYINT AS advances_on_unspecified_plays,
        
            SUM(advances_on_defensive_indifference)::UTINYINT AS advances_on_defensive_indifference,
        
            SUM(advances_on_errors)::UTINYINT AS advances_on_errors,
        
            SUM(plate_appearances_while_on_base)::UTINYINT AS plate_appearances_while_on_base,
        
            SUM(balls_in_play_while_running)::UTINYINT AS balls_in_play_while_running,
        
            SUM(balls_in_play_while_on_base)::UTINYINT AS balls_in_play_while_on_base,
        
            SUM(batter_total_bases_while_running)::UTINYINT AS batter_total_bases_while_running,
        
            SUM(batter_total_bases_while_on_base)::UTINYINT AS batter_total_bases_while_on_base,
        
            SUM(extra_base_chances)::UTINYINT AS extra_base_chances,
        
            SUM(extra_base_advance_attempts)::UTINYINT AS extra_base_advance_attempts,
        
            SUM(extra_bases_taken)::UTINYINT AS extra_bases_taken,
        
            SUM(bases_advanced)::UTINYINT AS bases_advanced,
        
            SUM(bases_advanced_on_balls_in_play)::UTINYINT AS bases_advanced_on_balls_in_play,
        
            SUM(surplus_bases_advanced_on_balls_in_play)::INT1 AS surplus_bases_advanced_on_balls_in_play,
        
            SUM(outs_on_extra_base_advance_attempts)::UTINYINT AS outs_on_extra_base_advance_attempts,
        
            SUM(pitches)::UTINYINT AS pitches,
        
            SUM(swings)::UTINYINT AS swings,
        
            SUM(swings_with_contact)::UTINYINT AS swings_with_contact,
        
            SUM(strikes)::UTINYINT AS strikes,
        
            SUM(strikes_called)::UTINYINT AS strikes_called,
        
            SUM(strikes_swinging)::UTINYINT AS strikes_swinging,
        
            SUM(strikes_foul)::UTINYINT AS strikes_foul,
        
            SUM(strikes_foul_tip)::UTINYINT AS strikes_foul_tip,
        
            SUM(strikes_in_play)::UTINYINT AS strikes_in_play,
        
            SUM(strikes_unknown)::UTINYINT AS strikes_unknown,
        
            SUM(balls)::UTINYINT AS balls,
        
            SUM(balls_called)::UTINYINT AS balls_called,
        
            SUM(balls_intentional)::UTINYINT AS balls_intentional,
        
            SUM(balls_automatic)::UTINYINT AS balls_automatic,
        
            SUM(unknown_pitches)::UTINYINT AS unknown_pitches,
        
            SUM(pitchouts)::UTINYINT AS pitchouts,
        
            SUM(pitcher_pickoff_attempts)::UTINYINT AS pitcher_pickoff_attempts,
        
            SUM(catcher_pickoff_attempts)::UTINYINT AS catcher_pickoff_attempts,
        
            SUM(pitches_blocked_by_catcher)::UTINYINT AS pitches_blocked_by_catcher,
        
            SUM(pitches_with_runners_going)::UTINYINT AS pitches_with_runners_going,
        
            SUM(passed_balls)::UTINYINT AS passed_balls,
        
            SUM(wild_pitches)::UTINYINT AS wild_pitches,
        
            SUM(balks)::UTINYINT AS balks,
        
            SUM(left_on_base)::UTINYINT AS left_on_base,
        
            SUM(left_on_base_with_two_outs)::UTINYINT AS left_on_base_with_two_outs,
        
    FROM "timeball"."main_models"."event_offense_stats"
    GROUP BY 1, 2, 3
    UNION ALL BY NAME
    SELECT
        game_id,
        team_id,
        batter_id AS player_id,
        at_bats,
        runs,
        hits,
        doubles,
        triples,
        home_runs,
        runs_batted_in,
        strikeouts,
        walks,
        intentional_walks,
        hit_by_pitches,
        sacrifice_hits,
        sacrifice_flies,
        NULL AS reached_on_errors,
        NULL AS reached_on_interferences,
        grounded_into_double_plays,
        NULL AS double_plays,
        NULL AS triple_plays,
        singles,
        total_bases,
        plate_appearances,
        on_base_opportunities,
        on_base_successes,
        batting_outs,
        stolen_bases,
        caught_stealing,
    FROM box_score
)

SELECT * FROM final