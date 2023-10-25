{{
  config(
    materialized = 'table',
    )
}}
{% set rate_stats =
  ["contact_broad_type_air_ball", "contact_type_ground_ball", "contact_type_fly_ball", "contact_type_line_drive", "contact_type_pop_fly"]
%}

{{ batter_pitcher_park_factor(rate_stats, "plate_appearances", filter_exp="contact_type_known = 1" ) }}
