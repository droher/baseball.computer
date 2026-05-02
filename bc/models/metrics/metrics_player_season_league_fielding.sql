MODEL (
  name main_models.metrics_player_season_league_fielding,
  kind FULL,
  description 'Aggregate fielding statistics and averages for each player-season, split if the player played in multiple leagues that year. Regular season only.',
  grain (player_id, season, league),
  columns (
    player_id VARCHAR,
    season SMALLINT,
    league VARCHAR,
    outs_played INTEGER,
    plate_appearances_in_field INTEGER,
    plate_appearances_in_field_with_ball_in_play INTEGER,
    unknown_putouts_while_fielding INTEGER,
    balls_hit_to INTEGER,
    putouts INTEGER,
    assists INTEGER,
    errors INTEGER,
    fielders_choices INTEGER,
    assisted_putouts INTEGER,
    in_play_putouts INTEGER,
    in_play_assists INTEGER,
    reaching_errors INTEGER,
    stolen_bases INTEGER,
    caught_stealing INTEGER,
    pickoffs INTEGER,
    passed_balls INTEGER,
    double_plays INTEGER,
    triple_plays INTEGER,
    ground_ball_double_plays INTEGER,
    double_plays_started INTEGER,
    ground_ball_double_plays_started INTEGER,
    fielding_percentage DOUBLE,
    range_factor DOUBLE,
    innings_played DOUBLE,
    event_coverage_rate DOUBLE,
    games_started INTEGER
  ),
  column_descriptions (
    player_id = @doc('player_id'),
    season = @doc('season'),
    league = @doc('league'),
    outs_played = @doc('outs_played'),
    plate_appearances_in_field = @doc('plate_appearances_in_field'),
    plate_appearances_in_field_with_ball_in_play = @doc('plate_appearances_in_field_with_ball_in_play'),
    unknown_putouts_while_fielding = @doc('unknown_putouts_while_fielding'),
    balls_hit_to = @doc('balls_hit_to'),
    putouts = @doc('putouts'),
    assists = @doc('assists'),
    errors = @doc('errors'),
    fielders_choices = @doc('fielders_choices'),
    assisted_putouts = @doc('assisted_putouts'),
    in_play_putouts = @doc('in_play_putouts'),
    in_play_assists = @doc('in_play_assists'),
    reaching_errors = @doc('reaching_errors'),
    stolen_bases = @doc('stolen_bases'),
    caught_stealing = @doc('caught_stealing'),
    pickoffs = @doc('pickoffs'),
    passed_balls = @doc('passed_balls'),
    double_plays = @doc('double_plays'),
    triple_plays = @doc('triple_plays'),
    ground_ball_double_plays = @doc('ground_ball_double_plays'),
    double_plays_started = @doc('double_plays_started'),
    ground_ball_double_plays_started = @doc('ground_ball_double_plays_started'),
    games_started = @doc('games_started')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_metrics_player_season_league_fielding.parquet'
  ),
);







@metric_table_body('fielding', 'player_id', 'season', 'league')
