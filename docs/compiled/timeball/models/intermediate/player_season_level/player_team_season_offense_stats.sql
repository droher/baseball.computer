
WITH databank AS (
    SELECT
        bat.season,
        bat.team_id,
        people.retrosheet_player_id AS player_id,
        'RegularSeason' AS game_type,
        SUM(bat.games)::SMALLINT AS games,
        SUM(bat.at_bats)::SMALLINT AS at_bats,
        SUM(bat.runs)::SMALLINT AS runs,
        SUM(bat.hits)::SMALLINT AS hits,
        SUM(bat.doubles)::SMALLINT AS doubles,
        SUM(bat.triples)::SMALLINT AS triples,
        SUM(bat.home_runs)::SMALLINT AS home_runs,
        SUM(bat.runs_batted_in)::SMALLINT AS runs_batted_in,
        SUM(bat.stolen_bases)::SMALLINT AS stolen_bases,
        SUM(bat.caught_stealing)::SMALLINT AS caught_stealing,
        SUM(bat.walks)::SMALLINT AS walks,
        SUM(bat.strikeouts)::SMALLINT AS strikeouts,
        SUM(bat.intentional_walks)::SMALLINT AS intentional_walks,
        SUM(bat.hit_by_pitches)::SMALLINT AS hit_by_pitches,
        SUM(bat.sacrifice_hits)::SMALLINT AS sacrifice_hits,
        SUM(bat.sacrifice_flies)::SMALLINT AS sacrifice_flies,
        SUM(bat.grounded_into_double_plays)::SMALLINT AS grounded_into_double_plays,
        SUM(bat.singles)::SMALLINT AS singles,
        SUM(bat.total_bases)::SMALLINT AS total_bases,
        SUM(bat.plate_appearances)::SMALLINT AS plate_appearances,
        SUM(bat.on_base_opportunities)::SMALLINT AS on_base_opportunities,
        SUM(bat.on_base_successes)::SMALLINT AS on_base_successes,
    FROM "timeball"."main_models"."stg_databank_batting" AS bat
    INNER JOIN "timeball"."main_models"."stg_people" AS people USING (databank_player_id)
    WHERE bat.season NOT IN (SELECT DISTINCT season FROM "timeball"."main_models"."stg_games")
    GROUP BY 1, 2, 3
),

databank_running AS (
    SELECT
        season,
        player_id,
        team_id,
        SUM(stolen_bases)::SMALLINT AS stolen_bases,
        SUM(caught_stealing)::SMALLINT AS caught_stealing,
    FROM "timeball"."main_models"."stg_databank_batting"
    -- TODO: Add var to indicate final databank override year
    WHERE season < 1920
    GROUP BY 1, 2, 3
),

retrosheet AS (
    SELECT
        games.season,
        stats.team_id,
        stats.player_id,
        games.game_type,
        COUNT(*) AS games,
        SUM(plate_appearances)::SMALLINT AS plate_appearances,
        SUM(at_bats)::SMALLINT AS at_bats,
        SUM(hits)::SMALLINT AS hits,
        SUM(singles)::SMALLINT AS singles,
        SUM(doubles)::SMALLINT AS doubles,
        SUM(triples)::SMALLINT AS triples,
        SUM(home_runs)::SMALLINT AS home_runs,
        SUM(total_bases)::SMALLINT AS total_bases,
        SUM(strikeouts)::SMALLINT AS strikeouts,
        SUM(walks)::SMALLINT AS walks,
        SUM(intentional_walks)::SMALLINT AS intentional_walks,
        SUM(hit_by_pitches)::SMALLINT AS hit_by_pitches,
        SUM(sacrifice_hits)::SMALLINT AS sacrifice_hits,
        SUM(sacrifice_flies)::SMALLINT AS sacrifice_flies,
        SUM(reached_on_errors)::SMALLINT AS reached_on_errors,
        SUM(reached_on_interferences)::SMALLINT AS reached_on_interferences,
        SUM(inside_the_park_home_runs)::SMALLINT AS inside_the_park_home_runs,
        SUM(ground_rule_doubles)::SMALLINT AS ground_rule_doubles,
        SUM(infield_hits)::SMALLINT AS infield_hits,
        SUM(on_base_opportunities)::SMALLINT AS on_base_opportunities,
        SUM(on_base_successes)::SMALLINT AS on_base_successes,
        SUM(runs_batted_in)::SMALLINT AS runs_batted_in,
        SUM(grounded_into_double_plays)::SMALLINT AS grounded_into_double_plays,
        SUM(double_plays)::SMALLINT AS double_plays,
        SUM(triple_plays)::SMALLINT AS triple_plays,
        SUM(batting_outs)::SMALLINT AS batting_outs,
        SUM(balls_in_play)::SMALLINT AS balls_in_play,
        SUM(balls_batted)::SMALLINT AS balls_batted,
        SUM(trajectory_fly_ball)::SMALLINT AS trajectory_fly_ball,
        SUM(trajectory_ground_ball)::SMALLINT AS trajectory_ground_ball,
        SUM(trajectory_line_drive)::SMALLINT AS trajectory_line_drive,
        SUM(trajectory_pop_up)::SMALLINT AS trajectory_pop_up,
        SUM(trajectory_unknown)::SMALLINT AS trajectory_unknown,
        SUM(trajectory_known)::SMALLINT AS trajectory_known,
        SUM(trajectory_broad_air_ball)::SMALLINT AS trajectory_broad_air_ball,
        SUM(trajectory_broad_ground_ball)::SMALLINT AS trajectory_broad_ground_ball,
        SUM(trajectory_broad_unknown)::SMALLINT AS trajectory_broad_unknown,
        SUM(trajectory_broad_known)::SMALLINT AS trajectory_broad_known,
        SUM(bunts)::SMALLINT AS bunts,
        SUM(batted_distance_plate)::SMALLINT AS batted_distance_plate,
        SUM(batted_distance_infield)::SMALLINT AS batted_distance_infield,
        SUM(batted_distance_outfield)::SMALLINT AS batted_distance_outfield,
        SUM(batted_distance_unknown)::SMALLINT AS batted_distance_unknown,
        SUM(batted_distance_known)::SMALLINT AS batted_distance_known,
        SUM(fielded_by_battery)::SMALLINT AS fielded_by_battery,
        SUM(fielded_by_infielder)::SMALLINT AS fielded_by_infielder,
        SUM(fielded_by_outfielder)::SMALLINT AS fielded_by_outfielder,
        SUM(fielded_by_known)::SMALLINT AS fielded_by_known,
        SUM(fielded_by_unknown)::SMALLINT AS fielded_by_unknown,
        SUM(batted_angle_left)::SMALLINT AS batted_angle_left,
        SUM(batted_angle_right)::SMALLINT AS batted_angle_right,
        SUM(batted_angle_middle)::SMALLINT AS batted_angle_middle,
        SUM(batted_angle_unknown)::SMALLINT AS batted_angle_unknown,
        SUM(batted_angle_known)::SMALLINT AS batted_angle_known,
        SUM(batted_location_plate)::SMALLINT AS batted_location_plate,
        SUM(batted_location_right_infield)::SMALLINT AS batted_location_right_infield,
        SUM(batted_location_middle_infield)::SMALLINT AS batted_location_middle_infield,
        SUM(batted_location_left_infield)::SMALLINT AS batted_location_left_infield,
        SUM(batted_location_left_field)::SMALLINT AS batted_location_left_field,
        SUM(batted_location_center_field)::SMALLINT AS batted_location_center_field,
        SUM(batted_location_right_field)::SMALLINT AS batted_location_right_field,
        SUM(batted_location_unknown)::SMALLINT AS batted_location_unknown,
        SUM(batted_location_known)::SMALLINT AS batted_location_known,
        SUM(batted_balls_pulled)::SMALLINT AS batted_balls_pulled,
        SUM(batted_balls_opposite_field)::SMALLINT AS batted_balls_opposite_field,
        SUM(runs)::SMALLINT AS runs,
        SUM(times_reached_base)::SMALLINT AS times_reached_base,
        SUM(times_lead_runner)::SMALLINT AS times_lead_runner,
        SUM(times_force_on_runner)::SMALLINT AS times_force_on_runner,
        SUM(times_next_base_empty)::SMALLINT AS times_next_base_empty,
        SUM(stolen_base_opportunities)::SMALLINT AS stolen_base_opportunities,
        SUM(stolen_base_opportunities_second)::SMALLINT AS stolen_base_opportunities_second,
        SUM(stolen_base_opportunities_third)::SMALLINT AS stolen_base_opportunities_third,
        SUM(stolen_base_opportunities_home)::SMALLINT AS stolen_base_opportunities_home,
        SUM(stolen_bases)::SMALLINT AS stolen_bases,
        SUM(stolen_bases_second)::SMALLINT AS stolen_bases_second,
        SUM(stolen_bases_third)::SMALLINT AS stolen_bases_third,
        SUM(stolen_bases_home)::SMALLINT AS stolen_bases_home,
        SUM(caught_stealing)::SMALLINT AS caught_stealing,
        SUM(caught_stealing_second)::SMALLINT AS caught_stealing_second,
        SUM(caught_stealing_third)::SMALLINT AS caught_stealing_third,
        SUM(caught_stealing_home)::SMALLINT AS caught_stealing_home,
        SUM(picked_off)::SMALLINT AS picked_off,
        SUM(picked_off_first)::SMALLINT AS picked_off_first,
        SUM(picked_off_second)::SMALLINT AS picked_off_second,
        SUM(picked_off_third)::SMALLINT AS picked_off_third,
        SUM(picked_off_caught_stealing)::SMALLINT AS picked_off_caught_stealing,
        SUM(outs_on_basepaths)::SMALLINT AS outs_on_basepaths,
        SUM(unforced_outs_on_basepaths)::SMALLINT AS unforced_outs_on_basepaths,
        SUM(outs_avoided_on_errors)::SMALLINT AS outs_avoided_on_errors,
        SUM(advances_on_wild_pitches)::SMALLINT AS advances_on_wild_pitches,
        SUM(advances_on_passed_balls)::SMALLINT AS advances_on_passed_balls,
        SUM(advances_on_balks)::SMALLINT AS advances_on_balks,
        SUM(advances_on_unspecified_plays)::SMALLINT AS advances_on_unspecified_plays,
        SUM(advances_on_defensive_indifference)::SMALLINT AS advances_on_defensive_indifference,
        SUM(advances_on_errors)::SMALLINT AS advances_on_errors,
        SUM(plate_appearances_while_on_base)::SMALLINT AS plate_appearances_while_on_base,
        SUM(balls_in_play_while_running)::SMALLINT AS balls_in_play_while_running,
        SUM(balls_in_play_while_on_base)::SMALLINT AS balls_in_play_while_on_base,
        SUM(batter_total_bases_while_running)::SMALLINT AS batter_total_bases_while_running,
        SUM(batter_total_bases_while_on_base)::SMALLINT AS batter_total_bases_while_on_base,
        SUM(extra_base_chances)::SMALLINT AS extra_base_chances,
        SUM(extra_base_advance_attempts)::SMALLINT AS extra_base_advance_attempts,
        SUM(extra_bases_taken)::SMALLINT AS extra_bases_taken,
        SUM(bases_advanced)::SMALLINT AS bases_advanced,
        SUM(bases_advanced_on_balls_in_play)::SMALLINT AS bases_advanced_on_balls_in_play,
        SUM(surplus_bases_advanced_on_balls_in_play)::SMALLINT AS surplus_bases_advanced_on_balls_in_play,
        SUM(outs_on_extra_base_advance_attempts)::SMALLINT AS outs_on_extra_base_advance_attempts,
        SUM(pitches)::SMALLINT AS pitches,
        SUM(swings)::SMALLINT AS swings,
        SUM(swings_with_contact)::SMALLINT AS swings_with_contact,
        SUM(strikes)::SMALLINT AS strikes,
        SUM(strikes_called)::SMALLINT AS strikes_called,
        SUM(strikes_swinging)::SMALLINT AS strikes_swinging,
        SUM(strikes_foul)::SMALLINT AS strikes_foul,
        SUM(strikes_foul_tip)::SMALLINT AS strikes_foul_tip,
        SUM(strikes_in_play)::SMALLINT AS strikes_in_play,
        SUM(strikes_unknown)::SMALLINT AS strikes_unknown,
        SUM(balls)::SMALLINT AS balls,
        SUM(balls_called)::SMALLINT AS balls_called,
        SUM(balls_intentional)::SMALLINT AS balls_intentional,
        SUM(balls_automatic)::SMALLINT AS balls_automatic,
        SUM(unknown_pitches)::SMALLINT AS unknown_pitches,
        SUM(pitchouts)::SMALLINT AS pitchouts,
        SUM(pitcher_pickoff_attempts)::SMALLINT AS pitcher_pickoff_attempts,
        SUM(catcher_pickoff_attempts)::SMALLINT AS catcher_pickoff_attempts,
        SUM(pitches_blocked_by_catcher)::SMALLINT AS pitches_blocked_by_catcher,
        SUM(pitches_with_runners_going)::SMALLINT AS pitches_with_runners_going,
        SUM(passed_balls)::SMALLINT AS passed_balls,
        SUM(wild_pitches)::SMALLINT AS wild_pitches,
        SUM(balks)::SMALLINT AS balks,
        SUM(left_on_base)::SMALLINT AS left_on_base,
        SUM(left_on_base_with_two_outs)::SMALLINT AS left_on_base_with_two_outs,
        
    FROM "timeball"."main_models"."stg_games" AS games
    INNER JOIN "timeball"."main_models"."player_game_offense_stats" AS stats USING (game_id)
    GROUP BY 1, 2, 3, 4
),

unioned AS (
    SELECT * FROM retrosheet
    UNION ALL BY NAME
    SELECT * FROM databank
),

final AS (
    SELECT
        u.* REPLACE (
            CASE WHEN u.game_type = 'RegularSeason'
                    THEN COALESCE(d.stolen_bases, u.stolen_bases)
                ELSE u.stolen_bases
            END AS stolen_bases,
            CASE WHEN u.game_type = 'RegularSeason'
                    THEN COALESCE(d.caught_stealing, u.caught_stealing)
                ELSE u.caught_stealing
            END AS caught_stealing
        )
    FROM unioned AS u
    LEFT JOIN databank_running AS d USING (season, player_id, team_id)
)

SELECT * FROM final