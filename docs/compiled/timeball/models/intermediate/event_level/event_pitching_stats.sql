

WITH baserunning_agg AS (
    -- Runs are populated separately to charge to the right pitcher
    SELECT
        event_key,
        MIN(game_id) AS game_id,
        MIN(current_pitcher_id) AS player_id,
        MIN(fielding_team_id) AS team_id,
        SUM(runs)::TINYINT AS runs,
        SUM(outs_on_basepaths)::TINYINT AS outs_on_basepaths,
        SUM(times_reached_base)::TINYINT AS times_reached_base,
        SUM(stolen_base_opportunities)::TINYINT AS stolen_base_opportunities,
        SUM(stolen_base_opportunities_second)::TINYINT AS stolen_base_opportunities_second,
        SUM(stolen_base_opportunities_third)::TINYINT AS stolen_base_opportunities_third,
        SUM(stolen_base_opportunities_home)::TINYINT AS stolen_base_opportunities_home,
        SUM(stolen_bases)::TINYINT AS stolen_bases,
        SUM(stolen_bases_second)::TINYINT AS stolen_bases_second,
        SUM(stolen_bases_third)::TINYINT AS stolen_bases_third,
        SUM(stolen_bases_home)::TINYINT AS stolen_bases_home,
        SUM(caught_stealing)::TINYINT AS caught_stealing,
        SUM(caught_stealing_second)::TINYINT AS caught_stealing_second,
        SUM(caught_stealing_third)::TINYINT AS caught_stealing_third,
        SUM(caught_stealing_home)::TINYINT AS caught_stealing_home,
        SUM(picked_off)::TINYINT AS picked_off,
        SUM(picked_off_first)::TINYINT AS picked_off_first,
        SUM(picked_off_second)::TINYINT AS picked_off_second,
        SUM(picked_off_third)::TINYINT AS picked_off_third,
        SUM(picked_off_caught_stealing)::TINYINT AS picked_off_caught_stealing,
        SUM(advances_on_wild_pitches)::TINYINT AS advances_on_wild_pitches,
        SUM(advances_on_passed_balls)::TINYINT AS advances_on_passed_balls,
        SUM(advances_on_balks)::TINYINT AS advances_on_balks,
        SUM(advances_on_unspecified_plays)::TINYINT AS advances_on_unspecified_plays,
        SUM(advances_on_defensive_indifference)::TINYINT AS advances_on_defensive_indifference,
        SUM(advances_on_errors)::TINYINT AS advances_on_errors,
        SUM(extra_base_advance_attempts)::TINYINT AS extra_base_advance_attempts,
        SUM(bases_advanced)::TINYINT AS bases_advanced,
        SUM(bases_advanced_on_balls_in_play)::TINYINT AS bases_advanced_on_balls_in_play,
        SUM(surplus_bases_advanced_on_balls_in_play)::TINYINT AS surplus_bases_advanced_on_balls_in_play,
        SUM(outs_on_extra_base_advance_attempts)::TINYINT AS outs_on_extra_base_advance_attempts,
        SUM(outs_avoided_on_errors)::TINYINT AS outs_avoided_on_errors,
        SUM(unforced_outs_on_basepaths)::TINYINT AS unforced_outs_on_basepaths,
        SUM(extra_base_chances)::TINYINT AS extra_base_chances,
        SUM(extra_bases_taken)::TINYINT AS extra_bases_taken,
        
    FROM "timeball"."main_models"."event_baserunning_stats"
    GROUP BY 1
),

joined_stats AS (
    SELECT
        event_key,
        COALESCE(baserunning_agg.game_id, hit.game_id) AS game_id,
        COALESCE(baserunning_agg.player_id, hit.pitcher_id) AS player_id,
        COALESCE(baserunning_agg.team_id, hit.fielding_team_id) AS team_id,
        hit.* EXCLUDE (event_key),
        bat.* EXCLUDE (event_key),
        -- Populate runs with the CTE below
        baserunning_agg.* EXCLUDE (event_key, runs),
        pitch.* EXCLUDE (event_key),
        hit.plate_appearances AS batters_faced,
        COALESCE(hit.outs_on_play, baserunning_agg.outs_on_basepaths) AS outs_recorded,
    FROM "timeball"."main_models"."event_batting_stats" AS hit
    FULL OUTER JOIN baserunning_agg USING (event_key)
    LEFT JOIN "timeball"."main_models"."event_batted_ball_stats" AS bat USING (event_key)
    LEFT JOIN "timeball"."main_models"."event_pitch_sequence_stats" AS pitch USING (event_key)
),

add_current_pitcher_runs AS (
    SELECT
        joined_stats.*,
        runs.runs,
        runs.team_unearned_runs,
        runs.inherited_runners_scored,
    FROM joined_stats
    LEFT JOIN "timeball"."main_models"."event_run_assignment_stats" AS runs
        ON joined_stats.event_key = runs.event_key
            AND joined_stats.player_id = runs.pitcher_id
),

-- This gets unioned instead of joined as these rows are supplemental
insert_non_current_pitcher_runs AS (
    SELECT
        game_id,
        event_key,
        team_id,
        pitcher_id AS player_id,
        runs,
        team_unearned_runs,
        bequeathed_runners_scored,
    FROM "timeball"."main_models"."event_run_assignment_stats"
    -- Meaning they are not currently in the game
    WHERE bequeathed_runners_scored > 0
),

unioned AS (
    SELECT * FROM add_current_pitcher_runs
    UNION ALL BY NAME
    SELECT * FROM insert_non_current_pitcher_runs
),

final AS (
    SELECT
        game_id,
        event_key,
        team_id,
        player_id,
        COALESCE(batters_faced, 0)::INT1 AS batters_faced,
        COALESCE(outs_recorded, 0)::INT1 AS outs_recorded,
        COALESCE(inherited_runners_scored, 0)::INT1 AS inherited_runners_scored,
        COALESCE(bequeathed_runners_scored, 0)::INT1 AS bequeathed_runners_scored,
        COALESCE(team_unearned_runs, 0)::INT1 AS team_unearned_runs,
        COALESCE(plate_appearances, 0)::INT1 AS plate_appearances,
        COALESCE(at_bats, 0)::INT1 AS at_bats,
        COALESCE(hits, 0)::INT1 AS hits,
        COALESCE(singles, 0)::INT1 AS singles,
        COALESCE(doubles, 0)::INT1 AS doubles,
        COALESCE(triples, 0)::INT1 AS triples,
        COALESCE(home_runs, 0)::INT1 AS home_runs,
        COALESCE(total_bases, 0)::INT1 AS total_bases,
        COALESCE(strikeouts, 0)::INT1 AS strikeouts,
        COALESCE(walks, 0)::INT1 AS walks,
        COALESCE(intentional_walks, 0)::INT1 AS intentional_walks,
        COALESCE(hit_by_pitches, 0)::INT1 AS hit_by_pitches,
        COALESCE(sacrifice_hits, 0)::INT1 AS sacrifice_hits,
        COALESCE(sacrifice_flies, 0)::INT1 AS sacrifice_flies,
        COALESCE(reached_on_errors, 0)::INT1 AS reached_on_errors,
        COALESCE(reached_on_interferences, 0)::INT1 AS reached_on_interferences,
        COALESCE(inside_the_park_home_runs, 0)::INT1 AS inside_the_park_home_runs,
        COALESCE(ground_rule_doubles, 0)::INT1 AS ground_rule_doubles,
        COALESCE(infield_hits, 0)::INT1 AS infield_hits,
        COALESCE(on_base_opportunities, 0)::INT1 AS on_base_opportunities,
        COALESCE(on_base_successes, 0)::INT1 AS on_base_successes,
        COALESCE(grounded_into_double_plays, 0)::INT1 AS grounded_into_double_plays,
        COALESCE(double_plays, 0)::INT1 AS double_plays,
        COALESCE(triple_plays, 0)::INT1 AS triple_plays,
        COALESCE(batting_outs, 0)::INT1 AS batting_outs,
        COALESCE(balls_in_play, 0)::INT1 AS balls_in_play,
        COALESCE(balls_batted, 0)::INT1 AS balls_batted,
        COALESCE(trajectory_fly_ball, 0)::INT1 AS trajectory_fly_ball,
        COALESCE(trajectory_ground_ball, 0)::INT1 AS trajectory_ground_ball,
        COALESCE(trajectory_line_drive, 0)::INT1 AS trajectory_line_drive,
        COALESCE(trajectory_pop_up, 0)::INT1 AS trajectory_pop_up,
        COALESCE(trajectory_unknown, 0)::INT1 AS trajectory_unknown,
        COALESCE(trajectory_known, 0)::INT1 AS trajectory_known,
        COALESCE(trajectory_broad_air_ball, 0)::INT1 AS trajectory_broad_air_ball,
        COALESCE(trajectory_broad_ground_ball, 0)::INT1 AS trajectory_broad_ground_ball,
        COALESCE(trajectory_broad_unknown, 0)::INT1 AS trajectory_broad_unknown,
        COALESCE(trajectory_broad_known, 0)::INT1 AS trajectory_broad_known,
        COALESCE(bunts, 0)::INT1 AS bunts,
        COALESCE(batted_distance_plate, 0)::INT1 AS batted_distance_plate,
        COALESCE(batted_distance_infield, 0)::INT1 AS batted_distance_infield,
        COALESCE(batted_distance_outfield, 0)::INT1 AS batted_distance_outfield,
        COALESCE(batted_distance_unknown, 0)::INT1 AS batted_distance_unknown,
        COALESCE(batted_distance_known, 0)::INT1 AS batted_distance_known,
        COALESCE(fielded_by_battery, 0)::INT1 AS fielded_by_battery,
        COALESCE(fielded_by_infielder, 0)::INT1 AS fielded_by_infielder,
        COALESCE(fielded_by_outfielder, 0)::INT1 AS fielded_by_outfielder,
        COALESCE(fielded_by_known, 0)::INT1 AS fielded_by_known,
        COALESCE(fielded_by_unknown, 0)::INT1 AS fielded_by_unknown,
        COALESCE(batted_angle_left, 0)::INT1 AS batted_angle_left,
        COALESCE(batted_angle_right, 0)::INT1 AS batted_angle_right,
        COALESCE(batted_angle_middle, 0)::INT1 AS batted_angle_middle,
        COALESCE(batted_angle_unknown, 0)::INT1 AS batted_angle_unknown,
        COALESCE(batted_angle_known, 0)::INT1 AS batted_angle_known,
        COALESCE(batted_location_plate, 0)::INT1 AS batted_location_plate,
        COALESCE(batted_location_right_infield, 0)::INT1 AS batted_location_right_infield,
        COALESCE(batted_location_middle_infield, 0)::INT1 AS batted_location_middle_infield,
        COALESCE(batted_location_left_infield, 0)::INT1 AS batted_location_left_infield,
        COALESCE(batted_location_left_field, 0)::INT1 AS batted_location_left_field,
        COALESCE(batted_location_center_field, 0)::INT1 AS batted_location_center_field,
        COALESCE(batted_location_right_field, 0)::INT1 AS batted_location_right_field,
        COALESCE(batted_location_unknown, 0)::INT1 AS batted_location_unknown,
        COALESCE(batted_location_known, 0)::INT1 AS batted_location_known,
        COALESCE(batted_balls_pulled, 0)::INT1 AS batted_balls_pulled,
        COALESCE(batted_balls_opposite_field, 0)::INT1 AS batted_balls_opposite_field,
        COALESCE(runs, 0)::INT1 AS runs,
        COALESCE(times_reached_base, 0)::INT1 AS times_reached_base,
        COALESCE(stolen_base_opportunities, 0)::INT1 AS stolen_base_opportunities,
        COALESCE(stolen_base_opportunities_second, 0)::INT1 AS stolen_base_opportunities_second,
        COALESCE(stolen_base_opportunities_third, 0)::INT1 AS stolen_base_opportunities_third,
        COALESCE(stolen_base_opportunities_home, 0)::INT1 AS stolen_base_opportunities_home,
        COALESCE(stolen_bases, 0)::INT1 AS stolen_bases,
        COALESCE(stolen_bases_second, 0)::INT1 AS stolen_bases_second,
        COALESCE(stolen_bases_third, 0)::INT1 AS stolen_bases_third,
        COALESCE(stolen_bases_home, 0)::INT1 AS stolen_bases_home,
        COALESCE(caught_stealing, 0)::INT1 AS caught_stealing,
        COALESCE(caught_stealing_second, 0)::INT1 AS caught_stealing_second,
        COALESCE(caught_stealing_third, 0)::INT1 AS caught_stealing_third,
        COALESCE(caught_stealing_home, 0)::INT1 AS caught_stealing_home,
        COALESCE(picked_off, 0)::INT1 AS picked_off,
        COALESCE(picked_off_first, 0)::INT1 AS picked_off_first,
        COALESCE(picked_off_second, 0)::INT1 AS picked_off_second,
        COALESCE(picked_off_third, 0)::INT1 AS picked_off_third,
        COALESCE(picked_off_caught_stealing, 0)::INT1 AS picked_off_caught_stealing,
        COALESCE(outs_on_basepaths, 0)::INT1 AS outs_on_basepaths,
        COALESCE(unforced_outs_on_basepaths, 0)::INT1 AS unforced_outs_on_basepaths,
        COALESCE(outs_avoided_on_errors, 0)::INT1 AS outs_avoided_on_errors,
        COALESCE(advances_on_wild_pitches, 0)::INT1 AS advances_on_wild_pitches,
        COALESCE(advances_on_passed_balls, 0)::INT1 AS advances_on_passed_balls,
        COALESCE(advances_on_balks, 0)::INT1 AS advances_on_balks,
        COALESCE(advances_on_unspecified_plays, 0)::INT1 AS advances_on_unspecified_plays,
        COALESCE(advances_on_defensive_indifference, 0)::INT1 AS advances_on_defensive_indifference,
        COALESCE(advances_on_errors, 0)::INT1 AS advances_on_errors,
        COALESCE(extra_base_chances, 0)::INT1 AS extra_base_chances,
        COALESCE(extra_base_advance_attempts, 0)::INT1 AS extra_base_advance_attempts,
        COALESCE(extra_bases_taken, 0)::INT1 AS extra_bases_taken,
        COALESCE(bases_advanced, 0)::INT1 AS bases_advanced,
        COALESCE(bases_advanced_on_balls_in_play, 0)::INT1 AS bases_advanced_on_balls_in_play,
        COALESCE(surplus_bases_advanced_on_balls_in_play, 0)::INT1 AS surplus_bases_advanced_on_balls_in_play,
        COALESCE(outs_on_extra_base_advance_attempts, 0)::INT1 AS outs_on_extra_base_advance_attempts,
        COALESCE(pitches, 0)::INT1 AS pitches,
        COALESCE(swings, 0)::INT1 AS swings,
        COALESCE(swings_with_contact, 0)::INT1 AS swings_with_contact,
        COALESCE(strikes, 0)::INT1 AS strikes,
        COALESCE(strikes_called, 0)::INT1 AS strikes_called,
        COALESCE(strikes_swinging, 0)::INT1 AS strikes_swinging,
        COALESCE(strikes_foul, 0)::INT1 AS strikes_foul,
        COALESCE(strikes_foul_tip, 0)::INT1 AS strikes_foul_tip,
        COALESCE(strikes_in_play, 0)::INT1 AS strikes_in_play,
        COALESCE(strikes_unknown, 0)::INT1 AS strikes_unknown,
        COALESCE(balls, 0)::INT1 AS balls,
        COALESCE(balls_called, 0)::INT1 AS balls_called,
        COALESCE(balls_intentional, 0)::INT1 AS balls_intentional,
        COALESCE(balls_automatic, 0)::INT1 AS balls_automatic,
        COALESCE(unknown_pitches, 0)::INT1 AS unknown_pitches,
        COALESCE(pitchouts, 0)::INT1 AS pitchouts,
        COALESCE(pitcher_pickoff_attempts, 0)::INT1 AS pitcher_pickoff_attempts,
        COALESCE(catcher_pickoff_attempts, 0)::INT1 AS catcher_pickoff_attempts,
        COALESCE(pitches_blocked_by_catcher, 0)::INT1 AS pitches_blocked_by_catcher,
        COALESCE(pitches_with_runners_going, 0)::INT1 AS pitches_with_runners_going,
        COALESCE(passed_balls, 0)::INT1 AS passed_balls,
        COALESCE(wild_pitches, 0)::INT1 AS wild_pitches,
        COALESCE(balks, 0)::INT1 AS balks,
        COALESCE(left_on_base, 0)::INT1 AS left_on_base,
        COALESCE(left_on_base_with_two_outs, 0)::INT1 AS left_on_base_with_two_outs,
        
    FROM unioned
)

SELECT * FROM final