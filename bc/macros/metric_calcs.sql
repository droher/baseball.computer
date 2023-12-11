

{% macro basic_rate_stats_offense() %}
    {{ return({
        "batting_average": "SUM(hits) / SUM(at_bats)",
        "on_base_percentage": "SUM(on_base_successes) / SUM(on_base_opportunities)",
        "slugging_percentage": "SUM(total_bases) / SUM(at_bats)",
        "on_base_plus_slugging": "SUM(on_base_successes) / SUM(on_base_opportunities) + SUM(total_bases) / SUM(at_bats)",
        "isolated_power": "SUM(total_bases) / SUM(at_bats) - SUM(hits) / SUM(at_bats)",
        "secondary_average": "SUM(total_bases - hits + walks + stolen_bases - caught_stealing) / SUM(at_bats)",
        "batting_average_on_balls_in_play": "SUM(hits - home_runs) / (SUM(at_bats) - SUM(home_runs) - SUM(strikeouts) + SUM(COALESCE(sacrifice_flies, 0)))",
        "home_run_rate": "SUM(home_runs) / SUM(plate_appearances)",
        "walk_rate": "SUM(walks) / SUM(plate_appearances)",
        "strikeout_rate": "SUM(strikeouts) / SUM(plate_appearances)",
        "stolen_base_percentage": "SUM(stolen_bases) / SUM(stolen_bases + caught_stealing)",
    })}}
{% endmacro %}

{% macro basic_rate_stats_pitching() %}
    {{ return ({
        "earned_run_average": "SUM(earned_runs) * 9 / SUM(outs_recorded * 3)",
        "run_average": "SUM(runs) * 9 / SUM(outs_recorded * 3)",
        "walks_per_9_innings": "SUM(walks) * 9 / SUM(outs_recorded * 3)",
        "strikeouts_per_9_innings": "SUM(strikeouts) * 9 / SUM(outs_recorded * 3)",
        "home_runs_per_9_innings": "SUM(home_runs) * 9 / SUM(outs_recorded * 3)",
        "hits_per_9_innings": "SUM(hits) * 9 / SUM(outs_recorded * 3)",
        "walks_and_hits_per_innings_pitched": "(SUM(walks) + SUM(hits)) / SUM(outs_recorded * 3)",
        "strikeout_to_walk_ratio": "SUM(strikeouts) / SUM(walks)",
        "walk_rate": "SUM(walks) / SUM(batters_faced)",
        "strikeout_rate": "SUM(strikeouts) / SUM(batters_faced)",
        "home_run_rate": "SUM(home_runs) / SUM(batters_faced)",
        "batting_average_against": "SUM(hits) / SUM(at_bats)",
        "on_base_percentage_against": "SUM(on_base_successes) / SUM(on_base_opportunities)",
        "slugging_percentage_against": "SUM(total_bases) / SUM(at_bats)",
        "on_base_plus_slugging_against": "SUM(on_base_successes) / SUM(on_base_opportunities) + SUM(total_bases) / SUM(at_bats)",
        "batting_average_on_balls_in_play": "SUM(hits - home_runs) / (SUM(at_bats) - SUM(home_runs) - SUM(strikeouts) + SUM(COALESCE(sacrifice_flies, 0)))",
    }) }}
{% endmacro %}

{% macro basic_rate_stats_fielding() %}
    {{ return ({
        "fielding_percentage": "SUM(putouts + assists) / SUM(putouts + assists + errors)",
        "range_factor": "(SUM(putouts) + SUM(assists)) * 9 / SUM(outs_played * 3)",
        "innings_played": "ROUND(SUM(outs_played) / 3, 2)",
    }) }}
{% endmacro %}

{% macro batted_ball_stats() %}
    {% set trajectory_stats = {
        "known_trajectory_rate_outs": "SUM(trajectory_known * balls_batted * (at_bats - hits)) / SUM(balls_batted * (at_bats - hits))",
        "known_trajectory_rate_hits": "SUM(trajectory_known * balls_batted * hits) / SUM(balls_batted * hits)",
        "known_trajectory_rate": "SUM(trajectory_known * balls_batted) / SUM(balls_batted)",
        "known_trajectory_broad_rate_outs": "SUM(trajectory_broad_known * balls_batted * (at_bats - hits)) / SUM(balls_batted * (at_bats - hits))",
        "known_trajectory_broad_rate_hits": "SUM(trajectory_broad_known * balls_batted * hits) / SUM(balls_batted * hits)",
        "known_trajectory_broad_rate": "SUM(trajectory_broad_known * balls_batted) / SUM(balls_batted)",
        "known_trajectory_out_hit_ratio": "known_trajectory_rate_outs / known_trajectory_rate_hits",
        "known_trajectory_broad_out_hit_ratio": "known_trajectory_broad_rate_outs / known_trajectory_broad_rate_hits",

        "air_ball_rate_outs": "SUM(trajectory_broad_air_ball * (at_bats - hits)) / SUM(trajectory_broad_known * (at_bats - hits))",
        "ground_ball_rate_outs": "SUM(trajectory_broad_ground_ball * (at_bats - hits)) / SUM(trajectory_broad_known * (at_bats - hits))",
        "ground_air_out_ratio": "ground_ball_rate_outs / air_ball_rate_outs",
        "air_ball_hit_rate": "SUM(trajectory_broad_air_ball * hits) / SUM(trajectory_broad_known * hits)",
        "ground_ball_hit_rate": "SUM(trajectory_broad_ground_ball * hits) / SUM(trajectory_broad_known * hits)",
        "ground_air_hit_ratio": "ground_ball_hit_rate / air_ball_hit_rate",
        "fly_ball_rate": "SUM(trajectory_fly_ball) / SUM(trajectory_known)",
        "line_drive_rate": "SUM(trajectory_line_drive) / SUM(trajectory_known)",
        "pop_up_rate": "SUM(trajectory_pop_up) / SUM(trajectory_known)",
        "ground_ball_rate": "SUM(trajectory_ground_ball) / SUM(trajectory_broad_known)",
        
        "coverage_weighted_air_ball_batting_average": "SUM(trajectory_broad_air_ball * hits) * known_trajectory_broad_out_hit_ratio / (SUM(trajectory_broad_air_ball * hits) * known_trajectory_broad_out_hit_ratio + SUM(trajectory_broad_air_ball * (at_bats - hits)))",
        "coverage_weighted_ground_ball_batting_average": "SUM(trajectory_ground_ball * hits) * known_trajectory_broad_out_hit_ratio / (SUM(trajectory_ground_ball * hits) * known_trajectory_broad_out_hit_ratio + SUM(trajectory_ground_ball * (at_bats - hits)))",
        "coverage_weighted_fly_ball_batting_average": "SUM(trajectory_fly_ball * hits) * known_trajectory_out_hit_ratio / (SUM(trajectory_fly_ball * hits) * known_trajectory_out_hit_ratio + SUM(trajectory_fly_ball * (at_bats - hits)))",
        "coverage_weighted_line_drive_batting_average": "SUM(trajectory_line_drive * hits) * known_trajectory_out_hit_ratio / (SUM(trajectory_line_drive * hits) * known_trajectory_out_hit_ratio + SUM(trajectory_line_drive * (at_bats - hits)))",
        "coverage_weighted_pop_up_batting_average": "SUM(trajectory_pop_up * hits) * known_trajectory_out_hit_ratio / (SUM(trajectory_pop_up * hits) * known_trajectory_out_hit_ratio + SUM(trajectory_pop_up * (at_bats - hits)))",
    } %}


    {% set angles = ["left", "right", "middle"] %}
    {% set directions = ["pulled", "opposite_field"] %}

    {% set angle_stats = {
        "known_angle_rate_outs": "SUM(batted_angle_known * (at_bats - hits)) / SUM(balls_batted * (at_bats - hits))",
        "known_angle_rate_hits": "SUM(batted_angle_known * hits) / SUM(hits)",
        "known_angle_rate": "SUM(batted_angle_known) / SUM(balls_batted)",
        "known_angle_out_hit_ratio": "known_angle_rate_outs / known_angle_rate_hits"
    } %}

    {% for a in angles %}
        {{ angle_stats.update({
            "angle_" ~ a ~ "_rate_outs": "SUM(batted_angle_" ~ a ~ " * (at_bats - hits)) / SUM(batted_angle_known * (at_bats - hits))",
            "angle_" ~ a ~ "_rate_hits": "SUM(batted_angle_" ~ a ~ " * hits) / SUM(batted_angle_known * hits)",
            "angle_" ~ a ~ "_rate": "SUM(batted_angle_" ~ a ~ ") / SUM(batted_angle_known)",
            "coverage_weighted_angle_" ~ a ~ "_batting_average": "SUM(batted_angle_" ~ a ~ " * hits) * known_angle_out_hit_ratio / (SUM(batted_angle_" ~ a ~ " * hits) * known_angle_out_hit_ratio + SUM(batted_angle_" ~ a ~ " * (at_bats - hits)))",
        }) }}
    {% endfor %}

    {% set direction_stats = {} %}
    {% for direction in directions %}
        {{ direction_stats.update({
            direction ~ "_rate_outs": "SUM(batted_balls_" ~ direction ~ " * (at_bats - hits)) / SUM(batted_angle_known * (at_bats - hits))",
            direction ~ "_rate_hits": "SUM(batted_balls_" ~ direction ~ " * hits) / SUM(batted_angle_known * hits)",
            direction ~ "_rate": "SUM(batted_balls_" ~ direction ~ ") / SUM(batted_angle_known)",
            "coverage_weighted_" ~ direction ~ "_batting_average": "SUM(batted_balls_" ~ direction ~ " * hits) * known_angle_out_hit_ratio / (SUM(batted_balls_" ~ direction ~ " * hits) * known_angle_out_hit_ratio + SUM(batted_balls_" ~ direction ~ " * (at_bats - hits)))",
        }) }}
    {% endfor %}

    {% set combined_stats = {} %}
    {% for d in [trajectory_stats, angle_stats, direction_stats] %}
        {{ combined_stats.update(d) }}
    {% endfor %}
    
    {{ return(combined_stats) }}
{% endmacro %}

{% macro baserunning_stats() %}
    {{ return({
        "stolen_base_attempt_rate_second": "SUM(stolen_bases_second + caught_stealing_second) / SUM(stolen_base_opportunities_second)",
        "stolen_base_attempt_rate_third": "SUM(stolen_bases_third + caught_stealing_third) / SUM(stolen_base_opportunities_third)",
        "stolen_base_attempt_rate_home": "SUM(stolen_bases_home + caught_stealing_home) / SUM(stolen_base_opportunities_home)",
        "unforced_out_rate": "SUM(unforced_outs_on_basepaths) / SUM(times_reached_base)",

    }) }}
{% endmacro %}

{% macro pitch_sequence_stats() %}
    {{ return({
        "pitch_strike_rate": "SUM(strikes) / SUM(pitches)",
        "pitch_contact_rate": "SUM(swings_with_contact) / SUM(pitches)",
        "pitch_swing_rate": "SUM(swings_with_contact + strikes_swinging) / SUM(pitches)",
        "pitch_ball_rate": "SUM(balls) / SUM(pitches)",
        "pitch_swing_and_miss_rate": "SUM(strikes_swinging) / SUM(pitches)",
        "pitch_foul_rate": "SUM(strikes_foul) / SUM(pitches)",
        "pitched_called_strike_rate": "SUM(strikes_called) / SUM(pitches)",
        "pitch_data_coverage_rate": "COUNT_IF(pitches > 0) / SUM(plate_appearances)",
    }) }}
{% endmacro %}

{% macro full_metric_list() %}
    {% set all_keys = [] %}
    {% for d in [basic_rate_stats_offense(), basic_rate_stats_pitching(), batted_ball_stats(), baserunning_stats(), pitch_sequence_stats()] %}
        {{ all_keys.extend(d) }}
    {% endfor %}
    {{ log('var: ' ~ all_keys, info=True) }}
    {{ return(all_keys) }}
{% endmacro %}