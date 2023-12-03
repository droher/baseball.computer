{{
  config(
    materialized = 'table',
    )
}}
{% set rate_stats = [
  "singles", "doubles", "triples", "home_runs", "strikeouts", "reached_on_errors", "walks", "batting_outs", "runs", "balls_in_play"
] %}

{{ batter_pitcher_park_factor(rate_stats, "plate_appearances") }}
