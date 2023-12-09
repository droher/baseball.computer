
WITH event_agg AS (
    SELECT
        game_id,
        player_id,
        MIN(team_id) AS team_id,
        
                
            
            SUM(batters_faced)::UTINYINT AS batters_faced,
        
                
            
            SUM(outs_recorded)::UTINYINT AS outs_recorded,
        
                
            
            SUM(inherited_runners_scored)::UTINYINT AS inherited_runners_scored,
        
                
            
            SUM(bequeathed_runners_scored)::UTINYINT AS bequeathed_runners_scored,
        
                
            
            SUM(team_unearned_runs)::UTINYINT AS team_unearned_runs,
        
                
            
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
        
                
            
            SUM(extra_base_chances)::UTINYINT AS extra_base_chances,
        
                
            
            SUM(extra_base_advance_attempts)::UTINYINT AS extra_base_advance_attempts,
        
                
            
            SUM(extra_bases_taken)::UTINYINT AS extra_bases_taken,
        
                
            
            SUM(bases_advanced)::INT2 AS bases_advanced,
        
                
            
            SUM(bases_advanced_on_balls_in_play)::INT2 AS bases_advanced_on_balls_in_play,
        
                
            
            SUM(surplus_bases_advanced_on_balls_in_play)::INT2 AS surplus_bases_advanced_on_balls_in_play,
        
                
            
            SUM(outs_on_extra_base_advance_attempts)::UTINYINT AS outs_on_extra_base_advance_attempts,
        
                
            
            SUM(pitches)::USMALLINT AS pitches,
        
                
            
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
        
                
            
            SUM(pitches_blocked_by_catcher)::USMALLINT AS pitches_blocked_by_catcher,
        
                
            
            SUM(pitches_with_runners_going)::USMALLINT AS pitches_with_runners_going,
        
                
            
            SUM(passed_balls)::UTINYINT AS passed_balls,
        
                
            
            SUM(wild_pitches)::UTINYINT AS wild_pitches,
        
                
            
            SUM(balks)::UTINYINT AS balks,
        
                
            
            SUM(left_on_base)::UTINYINT AS left_on_base,
        
                
            
            SUM(left_on_base_with_two_outs)::UTINYINT AS left_on_base_with_two_outs,
        
    FROM "timeball"."main_models"."event_pitching_stats"
    GROUP BY 1, 2
),

flag_agg AS (
  SELECT
        game_id,
        pitcher_id AS player_id,
        -- Some of these are SUM/COUNT because a pitcher could record separate appearances during the game
        -- so, theoretically, a pitcher could blow multiple saves in the same game
        BOOL_OR(starting_pitcher_flag)::UTINYINT AS games_started,
        SUM(inherited_runners)::UTINYINT AS inherited_runners,
        -- TODO: A bequeathed runner appears to be defined as the number of runners left on base
        -- when a pitcher leaves the game, regardless of whether those runners were inherited
        -- from a previous pitcher. This causes a double-counting issue, which we'll have to
        -- address either by applying bequeathed runner scoring to multiple pitchers
        -- or a bequeathal to a single pitcher.
        SUM(bequeathed_runners)::UTINYINT AS bequeathed_runners,
        BOOL_OR(new_relief_pitcher_flag)::UTINYINT AS games_relieved,
        BOOL_OR(pitcher_finish_flag)::UTINYINT AS games_finished,
        COUNT_IF(save_situation_start_flag)::UTINYINT AS save_situations_entered,
        COUNT_IF(hold_flag)::UTINYINT AS holds,
        COUNT_IF(blown_save_flag)::UTINYINT AS blown_saves,
        -- This could differ from save info in the game-level table if e.g.
        -- the scorekeeper decided to award a win by judgement
        BOOL_OR(save_flag)::UTINYINT AS saves_by_rule,
    FROM "timeball"."main_models"."event_pitching_flags"
    GROUP BY 1, 2  
),

events_with_flags AS (
    SELECT
        event_agg.*,
        flag_agg.* EXCLUDE (game_id, player_id),
    FROM event_agg
    LEFT JOIN flag_agg USING (game_id, player_id)
),

box_agg AS (
    SELECT
        game_id,
        stats.pitcher_id AS player_id,
        ANY_VALUE(CASE WHEN stats.side = 'Home' THEN games.home_team_id ELSE games.away_team_id END) AS team_id,
        SUM(stats.outs_recorded)::UTINYINT AS outs_recorded,
        SUM(stats.batters_faced)::UTINYINT AS batters_faced,
        SUM(stats.hits)::UTINYINT AS hits,
        SUM(stats.doubles)::UTINYINT AS doubles,
        SUM(stats.triples)::UTINYINT AS triples,
        SUM(stats.home_runs)::UTINYINT AS home_runs,
        SUM(stats.runs)::UTINYINT AS runs,
        SUM(stats.earned_runs)::UTINYINT AS earned_runs,
        SUM(stats.walks)::UTINYINT AS walks,
        SUM(stats.intentional_walks)::UTINYINT AS intentional_walks,
        SUM(stats.strikeouts)::UTINYINT AS strikeouts,
        SUM(stats.hit_by_pitches)::UTINYINT AS hit_by_pitches,
        SUM(stats.wild_pitches)::UTINYINT AS wild_pitches,
        SUM(stats.balks)::UTINYINT AS balks,
        SUM(stats.sacrifice_hits)::UTINYINT AS sacrifice_hits,
        SUM(stats.sacrifice_flies)::UTINYINT AS sacrifice_flies,
        SUM(stats.singles)::UTINYINT AS singles,
        SUM(stats.total_bases)::UTINYINT AS total_bases,
        SUM(stats.on_base_opportunities)::UTINYINT AS on_base_opportunities,
        SUM(stats.on_base_successes)::UTINYINT AS on_base_successes,
        SUM(stats.games_started)::UTINYINT AS games_started,
        SUM(stats.games_relieved)::UTINYINT AS games_relieved,
        SUM(stats.games_finished)::UTINYINT AS games_finished,
    FROM "timeball"."main_models"."stg_box_score_pitching_lines" AS stats
    -- This join ensures that we only get the box score lines for games that
    -- do not have an event file.
    INNER JOIN "timeball"."main_models"."stg_games" AS games USING (game_id)
    WHERE games.source_type = 'BoxScore'
    GROUP BY 1, 2
),

unioned AS (
    SELECT * FROM events_with_flags
    UNION ALL BY NAME
    SELECT * FROM box_agg
),

with_game_info AS (
    SELECT
        game_id,
        player_id,
        unioned.team_id,
        ROUND(unioned.outs_recorded / 3, 4)::DECIMAL(6, 4) AS innings_pitched,
        CASE WHEN player_id = games.winning_pitcher_id THEN 1 ELSE 0 END::UTINYINT AS wins,
        CASE WHEN player_id = games.losing_pitcher_id THEN 1 ELSE 0 END::UTINYINT AS losses,
        CASE WHEN player_id = games.save_pitcher_id THEN 1 ELSE 0 END::UTINYINT AS saves,
        -- Box score will have ER directly, but event data will need the join
        COALESCE(earned_runs.earned_runs, unioned.earned_runs)::UTINYINT AS earned_runs,
        unioned.* EXCLUDE (game_id, player_id, team_id, earned_runs),
        (saves + unioned.blown_saves)::UTINYINT AS save_opportunities,
    FROM unioned
    LEFT JOIN "timeball"."main_models"."stg_games" AS games USING (game_id)
    LEFT JOIN "timeball"."main_models"."stg_game_earned_runs" AS earned_runs USING (game_id, player_id)
),

final AS (
    SELECT
        *,
        CASE WHEN COUNT(*) OVER team_game = 1
                THEN 1
            ELSE 0
        END::UTINYINT AS complete_games,
        -- It's possible to record a shutout without a complete game
        -- if no other pitchers record outs (see Ernie Shore)
        CASE WHEN SUM(runs) OVER team_game = 0
                AND SUM(outs_recorded) OVER team_game = outs_recorded
                THEN 1
            ELSE 0
        END::UTINYINT AS shutouts,
        CASE WHEN games_started = 1 AND outs_recorded >= 18 AND earned_runs <= 3 THEN 1 ELSE 0 END::UTINYINT AS quality_starts,
        CASE WHEN games_started = 1 AND quality_starts = 0 AND wins = 1 THEN 1 ELSE 0 END::UTINYINT AS cheap_wins,
        CASE WHEN quality_starts = 1 AND losses = 1 THEN 1 ELSE 0 END::UTINYINT AS tough_losses,
        CASE WHEN games_started = 1 AND wins + losses = 0 THEN 1 ELSE 0 END::UTINYINT AS no_decisions,
        CASE WHEN complete_games = 1 AND hits = 0 AND outs_recorded >= 27 THEN 1 ELSE 0 END::UTINYINT AS no_hitters,
        -- Easy to calculate perfect games for games with event files, but box scores don't have ROEs.
        -- The logic here would be broken if a batter reached on an error and then was out on the bases,
        -- but no such event happened in prior to the event data era (maybe ever?)
        (
        CASE WHEN no_hitters = 1 AND (times_reached_base = 0
                OR (outs_recorded >= batters_faced AND COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) = 0) 
                ) THEN 1 
            ELSE 0 END)::UTINYINT AS perfect_games, 
    FROM with_game_info
    WINDOW team_game AS (PARTITION BY team_id, game_id)
)

SELECT * FROM final