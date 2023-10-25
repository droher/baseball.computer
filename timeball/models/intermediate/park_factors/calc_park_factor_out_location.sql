{{
  config(
    materialized = 'table',
    )
}}
{% set rate_stats = [
        "batted_distance_battery",
        "batted_distance_infield",
        "batted_distance_outfield",
        "fielded_in_battery",
        "fielded_in_infield",
        "fielded_in_outfield",
        "batted_angle_left",
        "batted_angle_right",
        "batted_angle_middle",
        "batted_location_mound",
        "batted_location_plate",
        "batted_location_first",
        "batted_location_second",
        "batted_location_third",
        "batted_location_short",
        "batted_location_left_field",
        "batted_location_center_field",
        "batted_location_right_field",
    ]
%}

{{ batter_pitcher_park_factor(rate_stats, "plate_appearances", filter_exp="batted_location_known = 1 AND batting_outs > 0", batter_hand_split=True) }}
