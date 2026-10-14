MODEL (
  name main_models.nlb_player_career_stat_ranges,
  kind FULL,
  description 'The range each Negro Leagues player''s career counting stat can take, one row per player, game type, line type, fielding position, and stat: the sums of nlb_player_season_stat_ranges. Covers only seasons that nlb_season_sources assigns to Retrosheet.',
  grain (player_id, game_type, line_type, fielding_position, stat),
  columns (
    player_id VARCHAR,
    game_type GAME_TYPE,
    line_type VARCHAR,
    fielding_position VARCHAR,
    stat VARCHAR,
    first_season SMALLINT,
    last_season SMALLINT,
    lower_bound INTEGER,
    upper_bound INTEGER,
    games UINTEGER,
    exact_games UINTEGER,
    bounded_games UINTEGER,
    unknown_games UINTEGER
  ),
  column_descriptions (
    player_id = @doc('player_id'),
    game_type = @doc('game_type'),
    line_type = 'Batting, Pitching, or Fielding.',
    fielding_position = 'Retrosheet''s position code for fielding lines; NULL for batting and pitching.',
    stat = 'Stat name, in the warehouse''s naming.',
    first_season = 'First season included.',
    last_season = 'Last season included.',
    lower_bound = 'Sum of the season lower bounds.',
    upper_bound = 'Sum of the season upper bounds; NULL (unbounded) when any season is unbounded.',
    games = 'Games with a line of this type.',
    exact_games = 'Games where the stat is exact.',
    bounded_games = 'Games where Retrosheet gives a bound.',
    unknown_games = 'Games where the stat is unknown.'
  ),
  audits (
    not_null(columns := (player_id, line_type, stat, lower_bound, games)),
    unique_grain(columns := (player_id, game_type, line_type, fielding_position, stat)),
    bounded_range(column := lower_bound, min_v := 0, max_v := upper_bound, condition := upper_bound IS NOT NULL),
    forall(criteria := (exact_games + bounded_games + unknown_games = games))
  ),
);

SELECT
    player_id,
    game_type,
    line_type,
    fielding_position,
    stat,
    MIN(season)::SMALLINT AS first_season,
    MAX(season)::SMALLINT AS last_season,
    SUM(lower_bound)::INTEGER AS lower_bound,
    CASE WHEN COUNT(upper_bound) = COUNT(*) THEN SUM(upper_bound) END::INTEGER AS upper_bound,
    SUM(games)::UINTEGER AS games,
    SUM(exact_games)::UINTEGER AS exact_games,
    SUM(bounded_games)::UINTEGER AS bounded_games,
    SUM(unknown_games)::UINTEGER AS unknown_games
FROM main_models.nlb_player_season_stat_ranges
GROUP BY player_id, game_type, line_type, fielding_position, stat
