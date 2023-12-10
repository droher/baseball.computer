{{
  config(
    materialized = 'table',
    )
}}
{% set rate_stats =
  ["trajectory_broad_air_ball", "trajectory_ground_ball", "trajectory_fly_ball", "trajectory_line_drive", "trajectory_pop_up"]
%}

{{ batter_pitcher_park_factor(rate_stats, "plate_appearances", filter_exp="trajectory_known = 1 AND batting_outs > 0" ) }}
