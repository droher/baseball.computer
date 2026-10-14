MODEL (
  name main_models.nlb_player_season_pitching_ranges,
  kind FULL,
  description 'Negro Leagues pitching ranges for common counting stats, one row per season, team, player, and game type: a wide pivot of nlb_player_season_stat_ranges. Each stat has a _lower and an _upper column; an _upper of NULL is unbounded. Rate stats are left out, since a range of counts does not give a meaningful range of rates. Only seasons that nlb_season_sources assigns to Retrosheet are included.',
  grain (season, team_id, player_id, game_type),
  columns (
    season SMALLINT,
    team_id VARCHAR,
    player_id VARCHAR,
    game_type GAME_TYPE,
    games UINTEGER,
    outs_recorded_lower INTEGER,
    outs_recorded_upper INTEGER,
    batters_faced_lower INTEGER,
    batters_faced_upper INTEGER,
    hits_lower INTEGER,
    hits_upper INTEGER,
    home_runs_lower INTEGER,
    home_runs_upper INTEGER,
    runs_lower INTEGER,
    runs_upper INTEGER,
    earned_runs_lower INTEGER,
    earned_runs_upper INTEGER,
    walks_lower INTEGER,
    walks_upper INTEGER,
    strikeouts_lower INTEGER,
    strikeouts_upper INTEGER,
    hit_by_pitches_lower INTEGER,
    hit_by_pitches_upper INTEGER
  ),
  column_descriptions (
    season = @doc('season'),
    team_id = 'Retrosheet team id from the Negro Leagues file.',
    player_id = @doc('player_id'),
    game_type = @doc('game_type'),
    games = 'Games with a pitching line.'
  ),
  audits (
    not_null(columns := (season, team_id, player_id, games)),
    unique_grain(columns := (season, team_id, player_id, game_type))
  ),
);

SELECT
    season,
    team_id,
    player_id,
    game_type,
    MAX(games)::UINTEGER AS games,
        MAX(lower_bound) FILTER (WHERE stat = 'outs_recorded') AS outs_recorded_lower,
        MAX(upper_bound) FILTER (WHERE stat = 'outs_recorded') AS outs_recorded_upper,
        MAX(lower_bound) FILTER (WHERE stat = 'batters_faced') AS batters_faced_lower,
        MAX(upper_bound) FILTER (WHERE stat = 'batters_faced') AS batters_faced_upper,
        MAX(lower_bound) FILTER (WHERE stat = 'hits') AS hits_lower,
        MAX(upper_bound) FILTER (WHERE stat = 'hits') AS hits_upper,
        MAX(lower_bound) FILTER (WHERE stat = 'home_runs') AS home_runs_lower,
        MAX(upper_bound) FILTER (WHERE stat = 'home_runs') AS home_runs_upper,
        MAX(lower_bound) FILTER (WHERE stat = 'runs') AS runs_lower,
        MAX(upper_bound) FILTER (WHERE stat = 'runs') AS runs_upper,
        MAX(lower_bound) FILTER (WHERE stat = 'earned_runs') AS earned_runs_lower,
        MAX(upper_bound) FILTER (WHERE stat = 'earned_runs') AS earned_runs_upper,
        MAX(lower_bound) FILTER (WHERE stat = 'walks') AS walks_lower,
        MAX(upper_bound) FILTER (WHERE stat = 'walks') AS walks_upper,
        MAX(lower_bound) FILTER (WHERE stat = 'strikeouts') AS strikeouts_lower,
        MAX(upper_bound) FILTER (WHERE stat = 'strikeouts') AS strikeouts_upper,
        MAX(lower_bound) FILTER (WHERE stat = 'hit_by_pitches') AS hit_by_pitches_lower,
        MAX(upper_bound) FILTER (WHERE stat = 'hit_by_pitches') AS hit_by_pitches_upper
FROM main_models.nlb_player_season_stat_ranges
WHERE line_type = 'Pitching'
GROUP BY season, team_id, player_id, game_type
