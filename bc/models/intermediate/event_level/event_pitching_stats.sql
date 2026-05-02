MODEL (
  name main_models.event_pitching_stats,
  kind FULL,
  description 'Event-level counting stats for each pitcher involved in each event. This includes both the pitcher on the mound and, if applicable, the pitcher being charged with the run(s) on the event.',
  grain (event_key, player_id),
  column_descriptions (
    game_id = @doc('game_id'),
    event_key = @doc('event_key'),
    team_id = @doc('team_id'),
    player_id = @doc('player_id'),
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
    extra_base_chances = @doc('extra_base_chances'),
    extra_bases_taken = @doc('extra_bases_taken'),
    plate_appearances = @doc('plate_appearances')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_pitching_stats.parquet'
  ),
);







WITH baserunning_agg AS (
    -- Runs are populated separately to charge to the right pitcher
    SELECT
        event_key,
        MIN(game_id) AS game_id,
        MIN(current_pitcher_id) AS player_id,
        MIN(fielding_team_id) AS team_id,
        @EACH(@pitching_baserunning_cols(), col -> SUM(@col)::TINYINT AS @col)
    FROM main_models.event_baserunning_stats
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
    FROM main_models.event_batting_stats AS hit
    FULL OUTER JOIN baserunning_agg USING (event_key)
    LEFT JOIN main_models.event_batted_ball_stats AS bat USING (event_key)
    LEFT JOIN main_models.event_pitch_sequence_stats AS pitch USING (event_key)
),

add_current_pitcher_runs AS (
    SELECT
        joined_stats.*,
        runs.runs,
        runs.team_unearned_runs,
        runs.inherited_runners_scored,
    FROM joined_stats
    LEFT JOIN main_models.event_run_assignment_stats AS runs
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
    FROM main_models.event_run_assignment_stats
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
        @EACH(@event_level_pitching_stats(), stat -> COALESCE(@stat, 0)::INT1 AS @stat)
    FROM unioned
)

SELECT * FROM final
