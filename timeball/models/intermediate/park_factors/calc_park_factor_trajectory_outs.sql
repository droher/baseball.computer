{{
  config(
    materialized = 'table',
    )
}}
{% set rate_stats =
  ["trajectory_broad_type_air_ball", "trajectory_ground_ball", "trajectory_fly_ball", "trajectory_line_drive", "trajectory_pop_fly"]
%}

{{ batter_pitcher_park_factor(rate_stats, "plate_appearances", filter_exp="trajectory_known = 1 AND batting_outs > 0" ) }}
