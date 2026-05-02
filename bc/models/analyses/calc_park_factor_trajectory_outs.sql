MODEL (
  name main_models.calc_park_factor_trajectory_outs,
  kind VIEW
);

@batter_pitcher_park_factor(
  ('trajectory_broad_air_ball', 'trajectory_ground_ball', 'trajectory_fly_ball', 'trajectory_line_drive', 'trajectory_pop_up'),
  'plate_appearances',
  filter_exp := 'trajectory_known = 1 AND batting_outs > 0'
)
