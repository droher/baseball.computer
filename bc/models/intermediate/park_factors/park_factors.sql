MODEL (
  name main_models.park_factors,
  kind FULL,
  grain (park_id, season, league),
  columns (
    season SMALLINT,
    park_id PARK_ID,
    league VARCHAR,
    basic_park_factor DOUBLE,
    singles_park_factor DOUBLE,
    doubles_park_factor DOUBLE,
    triples_park_factor DOUBLE,
    home_runs_park_factor DOUBLE,
    strikeouts_park_factor DOUBLE,
    walks_park_factor DOUBLE,
    batting_outs_park_factor DOUBLE,
    runs_park_factor DOUBLE,
    balls_in_play_park_factor DOUBLE,
    trajectory_fly_ball_park_factor DOUBLE,
    trajectory_ground_ball_park_factor DOUBLE,
    trajectory_line_drive_park_factor DOUBLE,
    trajectory_pop_up_park_factor DOUBLE,
    trajectory_unknown_park_factor DOUBLE,
    batted_distance_infield_park_factor DOUBLE,
    batted_distance_outfield_park_factor DOUBLE,
    batted_distance_unknown_park_factor DOUBLE,
    batted_angle_left_park_factor DOUBLE,
    batted_angle_right_park_factor DOUBLE,
    batted_angle_middle_park_factor DOUBLE,
    overall_park_factor DOUBLE
  ),
  column_descriptions (
    season = @doc('season'),
    park_id = @doc('park_id'),
    league = @doc('league')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_park_factors.parquet'
  ),
);







WITH final AS (
    SELECT
        b.season,
        b.park_id,
        b.league,
        b.basic_park_factor,
        a.* EXCLUDE (season, park_id, league, sqrt_sample_size),
        COALESCE(a.runs_park_factor, b.basic_park_factor) AS overall_park_factor
    FROM main_models.calc_park_factors_basic AS b
    LEFT JOIN main_models.calc_park_factors_advanced AS a USING (season, park_id, league)
)

SELECT * FROM final
