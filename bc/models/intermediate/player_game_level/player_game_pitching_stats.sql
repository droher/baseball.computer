MODEL (
  name main_models.player_game_pitching_stats,
  kind FULL,
  grain (game_id, player_id),
  column_descriptions (
    game_id = @doc('game_id'),
    player_id = @doc('player_id'),
    team_id = @doc('team_id'),
    innings_pitched = @doc('innings_pitched'),
    wins = @doc('wins'),
    losses = @doc('losses'),
    saves = @doc('saves'),
    earned_runs = @doc('earned_runs'),
    batters_faced = @doc('batters_faced'),
    outs_recorded = @doc('outs_recorded'),
    inherited_runners_scored = @doc('inherited_runners_scored'),
    bequeathed_runners_scored = @doc('bequeathed_runners_scored'),
    team_unearned_runs = @doc('team_unearned_runs'),
    at_bats = @doc('at_bats'),
    hits = @doc('hits'),
    singles = @doc('singles'),
    doubles = @doc('doubles'),
    triples = @doc('triples'),
    home_runs = @doc('home_runs'),
    total_bases = @doc('total_bases'),
    strikeouts = @doc('strikeouts'),
    walks = @doc('walks'),
    intentional_walks = @doc('intentional_walks'),
    hit_by_pitches = @doc('hit_by_pitches'),
    sacrifice_hits = @doc('sacrifice_hits'),
    sacrifice_flies = @doc('sacrifice_flies'),
    reached_on_errors = @doc('reached_on_errors'),
    reached_on_interferences = @doc('reached_on_interferences'),
    inside_the_park_home_runs = @doc('inside_the_park_home_runs'),
    ground_rule_doubles = @doc('ground_rule_doubles'),
    infield_hits = @doc('infield_hits'),
    on_base_opportunities = @doc('on_base_opportunities'),
    on_base_successes = @doc('on_base_successes'),
    grounded_into_double_plays = @doc('grounded_into_double_plays'),
    double_plays = @doc('double_plays'),
    triple_plays = @doc('triple_plays'),
    batting_outs = @doc('batting_outs'),
    balls_in_play = @doc('balls_in_play'),
    balls_batted = @doc('balls_batted'),
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
    fielded_by_known = @doc('fielded_by_known'),
    fielded_by_unknown = @doc('fielded_by_unknown'),
    batted_angle_left = @doc('batted_angle_left'),
    batted_angle_right = @doc('batted_angle_right'),
    batted_angle_middle = @doc('batted_angle_middle'),
    batted_angle_unknown = @doc('batted_angle_unknown'),
    batted_angle_known = @doc('batted_angle_known'),
    batted_location_plate = @doc('batted_location_plate'),
    batted_location_right_infield = @doc('batted_location_right_infield'),
    batted_location_middle_infield = @doc('batted_location_middle_infield'),
    batted_location_left_infield = @doc('batted_location_left_infield'),
    batted_location_left_field = @doc('batted_location_left_field'),
    batted_location_center_field = @doc('batted_location_center_field'),
    batted_location_right_field = @doc('batted_location_right_field'),
    batted_location_unknown = @doc('batted_location_unknown'),
    batted_location_known = @doc('batted_location_known'),
    batted_balls_pulled = @doc('batted_balls_pulled'),
    batted_balls_opposite_field = @doc('batted_balls_opposite_field'),
    runs = @doc('runs'),
    times_reached_base = @doc('times_reached_base'),
    stolen_bases = @doc('stolen_bases'),
    caught_stealing = @doc('caught_stealing'),
    picked_off = @doc('picked_off'),
    picked_off_caught_stealing = @doc('picked_off_caught_stealing'),
    outs_on_basepaths = @doc('outs_on_basepaths'),
    unforced_outs_on_basepaths = @doc('unforced_outs_on_basepaths'),
    outs_avoided_on_errors = @doc('outs_avoided_on_errors'),
    advances_on_wild_pitches = @doc('advances_on_wild_pitches'),
    advances_on_passed_balls = @doc('advances_on_passed_balls'),
    advances_on_balks = @doc('advances_on_balks'),
    advances_on_unspecified_plays = @doc('advances_on_unspecified_plays'),
    advances_on_defensive_indifference = @doc('advances_on_defensive_indifference'),
    advances_on_errors = @doc('advances_on_errors'),
    extra_base_advance_attempts = @doc('extra_base_advance_attempts'),
    bases_advanced = @doc('bases_advanced'),
    bases_advanced_on_balls_in_play = @doc('bases_advanced_on_balls_in_play'),
    surplus_bases_advanced_on_balls_in_play = @doc('surplus_bases_advanced_on_balls_in_play'),
    outs_on_extra_base_advance_attempts = @doc('outs_on_extra_base_advance_attempts'),
    stolen_bases_second = @doc('stolen_bases_second'),
    stolen_bases_third = @doc('stolen_bases_third'),
    stolen_bases_home = @doc('stolen_bases_home'),
    caught_stealing_second = @doc('caught_stealing_second'),
    caught_stealing_third = @doc('caught_stealing_third'),
    caught_stealing_home = @doc('caught_stealing_home'),
    stolen_base_opportunities = @doc('stolen_base_opportunities'),
    stolen_base_opportunities_second = @doc('stolen_base_opportunities_second'),
    stolen_base_opportunities_third = @doc('stolen_base_opportunities_third'),
    stolen_base_opportunities_home = @doc('stolen_base_opportunities_home'),
    picked_off_first = @doc('picked_off_first'),
    picked_off_second = @doc('picked_off_second'),
    picked_off_third = @doc('picked_off_third'),
    pitches = @doc('pitches'),
    swings = @doc('swings'),
    swings_with_contact = @doc('swings_with_contact'),
    strikes = @doc('strikes'),
    strikes_called = @doc('strikes_called'),
    strikes_swinging = @doc('strikes_swinging'),
    strikes_foul = @doc('strikes_foul'),
    strikes_foul_tip = @doc('strikes_foul_tip'),
    strikes_in_play = @doc('strikes_in_play'),
    strikes_unknown = @doc('strikes_unknown'),
    balls = @doc('balls'),
    balls_called = @doc('balls_called'),
    balls_intentional = @doc('balls_intentional'),
    balls_automatic = @doc('balls_automatic'),
    unknown_pitches = @doc('unknown_pitches'),
    pitchouts = @doc('pitchouts'),
    pitcher_pickoff_attempts = @doc('pitcher_pickoff_attempts'),
    catcher_pickoff_attempts = @doc('catcher_pickoff_attempts'),
    pitches_blocked_by_catcher = @doc('pitches_blocked_by_catcher'),
    pitches_with_runners_going = @doc('pitches_with_runners_going'),
    passed_balls = @doc('passed_balls'),
    wild_pitches = @doc('wild_pitches'),
    balks = @doc('balks'),
    left_on_base = @doc('left_on_base'),
    left_on_base_with_two_outs = @doc('left_on_base_with_two_outs'),
    games_started = @doc('games_started'),
    inherited_runners = @doc('inherited_runners'),
    bequeathed_runners = @doc('bequeathed_runners'),
    games_relieved = @doc('games_relieved'),
    games_finished = @doc('games_finished'),
    save_situations_entered = @doc('save_situations_entered'),
    holds = @doc('holds'),
    blown_saves = @doc('blown_saves'),
    saves_by_rule = @doc('saves_by_rule'),
    save_opportunities = @doc('save_opportunities'),
    complete_games = @doc('complete_games'),
    shutouts = @doc('shutouts'),
    quality_starts = @doc('quality_starts'),
    cheap_wins = @doc('cheap_wins'),
    tough_losses = @doc('tough_losses'),
    no_decisions = @doc('no_decisions'),
    no_hitters = @doc('no_hitters'),
    perfect_games = @doc('perfect_games'),
    extra_base_chances = @doc('extra_base_chances'),
    extra_bases_taken = @doc('extra_bases_taken'),
    plate_appearances = @doc('plate_appearances')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_player_game_pitching_stats.parquet'
  ),
  audits (
    not_null(columns := (game_id, player_id, team_id)),
    unique_grain(columns := (game_id, player_id)),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id),
    relationships(column := player_id, to_model := main_models.people, to_column := player_id),
    relationships(column := team_id, to_model := main_seeds.seed_franchises, to_column := team_id),
    team_game_has_one_starter(),
    bounded_range(column := walks, min_v := 0, max_v := batters_faced, condition := batters_faced IS NOT NULL),
    bounded_range(column := complete_games, min_v := 0, max_v := games_started, condition := NOT @team_game_data_issue_match(game_id, team_id, 'starting_pitcher_no_appearance')),
    bounded_range(column := perfect_games, min_v := 0, max_v := no_hitters),
    bounded_excluding_data_issues(column := hits, min_v := 0, max_v := batters_faced, issue_type := 'hits_gt_batters_faced', condition := batters_faced IS NOT NULL),
    bounded_excluding_data_issues(column := strikeouts, min_v := 0, max_v := batters_faced, issue_type := 'strikeouts_gt_batters_faced', condition := batters_faced IS NOT NULL),
    bounded_excluding_data_issues(column := home_runs, min_v := 0, max_v := hits, issue_type := 'home_runs_gt_hits')
  ),
);







WITH event_agg AS (
    SELECT
        game_id,
        player_id,
        MIN(team_id) AS team_id,
        @player_pitching_sum_block()
    FROM main_models.event_pitching_stats
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
    FROM main_models.event_pitching_flags
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
    FROM main_models.stg_box_score_pitching_lines AS stats
    -- This join ensures that we only get the box score lines for games that
    -- do not have an event file.
    INNER JOIN main_models.stg_games AS games USING (game_id)
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
    LEFT JOIN main_models.stg_games AS games USING (game_id)
    LEFT JOIN main_models.stg_game_earned_runs AS earned_runs USING (game_id, player_id)
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
