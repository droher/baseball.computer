MODEL (
  name main_models.calc_park_factor_plate_appearances,
  kind VIEW
);

@batter_pitcher_park_factor(
  ('singles', 'doubles', 'triples', 'home_runs', 'strikeouts', 'reached_on_errors', 'walks', 'batting_outs', 'runs', 'balls_in_play'),
  'plate_appearances'
)
