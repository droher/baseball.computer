MODEL (
  name main_models.nlb_player_season_stat_ranges,
  kind FULL,
  description 'The range each Negro Leagues player-season counting stat can take, one row per season, team, player, game type, line type, fielding position, and stat: the sums of nlb_player_game_stat_ranges. Only seasons that nlb_season_sources assigns to Retrosheet are included.',
  grain (season, team_id, player_id, game_type, line_type, fielding_position, stat),
  columns (
    season SMALLINT,
    team_id VARCHAR,
    player_id VARCHAR,
    game_type GAME_TYPE,
    line_type VARCHAR,
    fielding_position VARCHAR,
    stat VARCHAR,
    lower_bound INTEGER,
    upper_bound INTEGER,
    games UINTEGER,
    exact_games UINTEGER,
    bounded_games UINTEGER,
    unknown_games UINTEGER
  ),
  column_descriptions (
    season = @doc('season'),
    team_id = 'Retrosheet team id from the Negro Leagues file.',
    player_id = @doc('player_id'),
    game_type = @doc('game_type'),
    line_type = 'Batting, Pitching, or Fielding.',
    fielding_position = 'Retrosheet''s position code for fielding lines; NULL for batting and pitching.',
    stat = 'Stat name, in the warehouse''s naming.',
    lower_bound = 'Sum of the game lower bounds.',
    upper_bound = 'Sum of the game upper bounds; NULL (unbounded) when any game is unbounded.',
    games = 'Games with a line of this type.',
    exact_games = 'Games where the stat is exact.',
    bounded_games = 'Games where Retrosheet gives a bound.',
    unknown_games = 'Games where the stat is unknown.'
  ),
  audits (
    not_null(columns := (season, team_id, player_id, line_type, stat, lower_bound, games)),
    unique_grain(columns := (season, team_id, player_id, game_type, line_type, fielding_position, stat)),
    bounded_range(column := lower_bound, min_v := 0, max_v := upper_bound, condition := upper_bound IS NOT NULL),
    forall(criteria := (exact_games + bounded_games + unknown_games = games))
  ),
);

SELECT
    season,
    team_id,
    player_id,
    game_type,
    line_type,
    fielding_position,
    stat,
    SUM(lower_bound)::INTEGER AS lower_bound,
    CASE WHEN COUNT(upper_bound) = COUNT(*) THEN SUM(upper_bound) END::INTEGER AS upper_bound,
    COUNT(*)::UINTEGER AS games,
    COUNT_IF(range_basis = 'Exact')::UINTEGER AS exact_games,
    COUNT_IF(range_basis = 'ReportedBound')::UINTEGER AS bounded_games,
    COUNT_IF(range_basis = 'Unknown')::UINTEGER AS unknown_games
FROM main_models.nlb_player_game_stat_ranges
GROUP BY season, team_id, player_id, game_type, line_type, fielding_position, stat
