{% macro event_level_offense_stats() %}
    {{ return([
        "plate_appearances",
        "at_bats",
        "hits",
        "singles",
        "doubles",
        "triples",
        "home_runs",
        "total_bases",
        "strikeouts",
        "walks",
        "intentional_walks",
        "hit_by_pitches",
        "sacrifice_hits",
        "sacrifice_flies",
        "reached_on_errors",
        "reached_on_interferences",
        "on_base_opportunities",
        "on_base_successes",
        "runs_batted_in",
        "grounded_into_double_plays",
        "double_plays",
        "triple_plays",
        "batting_outs",
        "balls_in_play",
        "contact_type_fly_ball",
        "contact_type_ground_ball",
        "contact_type_line_drive",
        "contact_type_pop_fly",
        "contact_type_unknown",
        "contact_broad_type_air_ball",
        "contact_broad_type_ground_ball",
        "contact_broad_type_unknown",
        "bunts",
        "batted_distance_battery",
        "batted_distance_infield",
        "batted_distance_outfield",
        "batted_distance_unknown",
        "fielded_in_battery",
        "fielded_in_infield",
        "fielded_in_outfield",
        "fielded_in_unknown",
        "batted_angle_left",
        "batted_angle_right",
        "batted_angle_middle",
        "batted_angle_unknown",
        "batted_down_foul_line",
        "runs",
        "times_reached_base",
        "stolen_bases",
        "caught_stealing",
        "picked_off",
        "picked_off_caught_stealing",
        "outs_on_basepaths",
        "advances_on_wild_pitches",
        "advances_on_passed_balls",
        "advances_on_balks",
        "advances_on_unspecified_plays",
        "advances_on_defensive_indifference",
        "advances_on_errors",
        "plate_appearances_while_on_base",
        "balls_in_play_while_running",
        "balls_in_play_while_on_base",
        "batter_total_bases_while_running",
        "batter_total_bases_while_on_base",
        "extra_base_advance_attempts",
        "bases_advanced",
        "bases_advanced_on_balls_in_play",
        "surplus_bases_advanced_on_balls_in_play",
        "outs_on_extra_base_advance_attempts",
        "pitches",
        "swings",
        "swings_with_contact",
        "strikes",
        "strikes_called",
        "strikes_swinging",
        "strikes_foul",
        "strikes_foul_tip",
        "strikes_in_play",
        "strikes_unknown",
        "balls",
        "balls_called",
        "balls_intentional",
        "balls_automatic",
        "unknown_pitches",
        "pitchouts",
        "pitcher_pickoff_attempts",
        "catcher_pickoff_attempts",
        "pitches_blocked_by_catcher",
        "pitches_with_runners_going",
        "passed_balls",
        "wild_pitches",
        "balks",
        "left_on_base",
        "left_on_base_with_two_outs"
    ]) }}
{% endmacro %}

{% macro event_level_pitching_stats() %}
    {% set non_pitching_stats = [
        "plate_appearances",
        "runs_batted_in",
        "plate_appearances_while_on_base",
        "balls_in_play_while_running",
        "balls_in_play_while_on_base",
        "batter_total_bases_while_running",
        "batter_total_bases_while_on_base",
    ] %}
    {% set extra_pitching_stats = [
        "batters_faced",
        "outs_recorded",
        "inherited_runners_scored",
        "bequeathed_runners_scored",
        "team_unearned_runs"
    ] %}

    {{ return(extra_pitching_stats + remove_items(event_level_offense_stats(), non_pitching_stats)) }}
{% endmacro %}


{% macro game_level_pitching_stats() %}
    {{ return([
        "games_started",
        "innings_pitched",
        "inherited_runners",
        "bequeathed_runners",
        "games_relieved",
        "games_finished",
        "save_situations_entered",
        "holds",
        "blown_saves",
        "saves_by_rule",
        "save_opportunities",
        "wins",
        "losses",
        "saves",
        "earned_runs",
        "complete_games",
        "shutouts",
        "quality_starts",
        "cheap_wins",
        "tough_losses",
        "no_decisions",
        "no_hitters",
        "perfect_games",
    ]) }}
{% endmacro %}

{% macro remove_items(my_list, values_to_remove) %}
  {% set new_list = [] %}
  {% for item in my_list if item not in values_to_remove %}
    {% do new_list.append(item) %}
  {% endfor %}
  {{ return(new_list) }}
{% endmacro %}