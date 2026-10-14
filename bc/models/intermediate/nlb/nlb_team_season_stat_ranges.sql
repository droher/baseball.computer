MODEL (
  name main_models.nlb_team_season_stat_ranges,
  kind FULL,
  description 'The range each Negro Leagues team-season counting stat can take, one row per season, team, game type, line type, and stat: the sums of the team lines in nlb_team_game_stat_ranges. Only seasons that nlb_season_sources assigns to Retrosheet are included.',
  grain (season, team_id, game_type, line_type, stat),
  columns (
    season SMALLINT,
    team_id VARCHAR,
    game_type GAME_TYPE,
    line_type VARCHAR,
    stat VARCHAR,
    lower_bound INTEGER,
    upper_bound INTEGER,
    games UINTEGER,
    exact_games UINTEGER,
    bounded_games UINTEGER,
    unknown_games UINTEGER,
    games_outside_player_bounds UINTEGER
  ),
  column_descriptions (
    season = @doc('season'),
    team_id = 'Retrosheet team id from the Negro Leagues file.',
    game_type = @doc('game_type'),
    line_type = 'Batting, Pitching, or Fielding.',
    stat = 'Stat name, in the warehouse''s naming.',
    lower_bound = 'Sum of the game lower bounds.',
    upper_bound = 'Sum of the game upper bounds; NULL (unbounded) when any game is unbounded.',
    games = 'Team-games with a team line.',
    exact_games = 'Games where the team stat is exact.',
    bounded_games = 'Games where Retrosheet gives a bound.',
    unknown_games = 'Games where the team stat is unknown.',
    games_outside_player_bounds = 'Games where the team range does not overlap the summed player range.'
  ),
  audits (
    not_null(columns := (season, team_id, line_type, stat, lower_bound, games)),
    unique_grain(columns := (season, team_id, game_type, line_type, stat)),
    bounded_range(column := lower_bound, min_v := 0, max_v := upper_bound, condition := upper_bound IS NOT NULL),
    forall(criteria := (exact_games + bounded_games + unknown_games = games))
  ),
);

SELECT
    season,
    team_id,
    game_type,
    line_type,
    stat,
    SUM(lower_bound)::INTEGER AS lower_bound,
    CASE WHEN COUNT(upper_bound) = COUNT(*) THEN SUM(upper_bound) END::INTEGER AS upper_bound,
    COUNT(*)::UINTEGER AS games,
    COUNT_IF(range_basis = 'Exact')::UINTEGER AS exact_games,
    COUNT_IF(range_basis = 'ReportedBound')::UINTEGER AS bounded_games,
    COUNT_IF(range_basis = 'Unknown')::UINTEGER AS unknown_games,
    COUNT_IF(is_outside_player_bounds)::UINTEGER AS games_outside_player_bounds
FROM main_models.nlb_team_game_stat_ranges
GROUP BY season, team_id, game_type, line_type, stat
