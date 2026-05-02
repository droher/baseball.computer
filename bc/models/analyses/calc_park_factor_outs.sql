MODEL (
  name main_models.calc_park_factor_outs,
  kind VIEW
);

@batter_pitcher_park_factor(
  ('singles', 'doubles', 'triples', 'home_runs', 'strikeouts', 'reached_on_errors', 'walks', 'plate_appearances', 'runs', 'balls_in_play'),
  'batting_outs',
  use_odds := FALSE
)
