MODEL (
  name main_models.calc_park_factor_in_play,
  kind VIEW
);

@batter_pitcher_park_factor(
  ('hits', 'singles', 'doubles', 'triples', 'reached_on_errors', 'batting_outs'),
  'balls_in_play',
  filter_exp := 'balls_in_play = 1'
)
