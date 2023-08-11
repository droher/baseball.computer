{% macro offense_stats() %}
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
        "batted_type_fly_ball",
        "batted_type_ground_ball",
        "batted_type_line_drive",
        "batted_type_unknown",
        "batted_broad_type_air_ball",
        "batted_broad_type_ground_ball",
        "batted_broad_type_unknown",
        "batted_subtype_pop_fly",
        "batted_subtype_non_pop_fly",
        "bunts",
        "batted_distance_home_plate",
        "batted_distance_infield",
        "batted_distance_outfield",
        "batted_distance_unknown",
        "batted_angle_left",
        "batted_angle_right",
        "batted_angle_middle",
        "batted_angle_unknown",
        "batted_down_foul_line",
        "fielder_derived_location",
        "runs",
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
    ]) }}
{% endmacro %}

{% macro pitching_stats() %}
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
    ] %}

    {{ return(extra_pitching_stats + remove_items(offense_stats(), non_pitching_stats)) }}
{% endmacro %}


{% macro remove_items(my_list, values_to_remove) %}
  {% set new_list = [] %}
  {% for item in my_list if item not in values_to_remove %}
    {% do new_list.append(item) %}
  {% endfor %}
  {{ return(new_list) }}
{% endmacro %}