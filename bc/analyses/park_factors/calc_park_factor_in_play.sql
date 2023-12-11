{{
  config(
    materialized = 'table',
    )
}}
{% set rate_stats = [
  "hits", "singles", "doubles", "triples", "reached_on_errors", "batting_outs",
] %}

{{ batter_pitcher_park_factor(rate_stats, "balls_in_play", filter_exp="balls_in_play = 1") }}
