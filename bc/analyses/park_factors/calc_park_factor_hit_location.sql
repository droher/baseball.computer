{{
  config(
    materialized = 'table',
    )
}}
{% set rate_stats = [
        "batted_distance_plate",
        "batted_distance_infield",
        "batted_distance_outfield",
        "fielded_by_battery",
        "fielded_by_infielder",
        "fielded_by_outfielder",
        "batted_angle_left",
        "batted_angle_right",
        "batted_angle_middle",
        "batted_location_left_field",
        "batted_location_center_field",
        "batted_location_right_field",
    ]
%}

{{ batter_pitcher_park_factor(rate_stats, "plate_appearances", filter_exp="batted_location_known = 1 AND hits = 1", batter_hand_split=True) }}
