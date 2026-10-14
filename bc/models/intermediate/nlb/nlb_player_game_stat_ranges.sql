MODEL (
  name main_models.nlb_player_game_stat_ranges,
  kind FULL,
  description 'The range each Negro Leagues player-game counting stat can take, one row per game, player, team, line type, fielding position, and stat, from every line in Retrosheet''s negroleagues.zip. An exact value is its own lower and upper bound. A stat Retrosheet gives only as a bound takes Retrosheet''s lower bound (0 if it gives only an upper) and upper bound (NULL, unbounded, if it gives only a lower). An unknown stat runs from 0 to unbounded. A player''s split lines in one game are summed. Only seasons that nlb_season_sources assigns to Retrosheet are included.',
  grain (game_id, player_id, team_id, line_type, fielding_position, stat),
  columns (
    game_id VARCHAR,
    season SMALLINT,
    game_type GAME_TYPE,
    player_id VARCHAR,
    team_id VARCHAR,
    line_type VARCHAR,
    fielding_position VARCHAR,
    stat VARCHAR,
    value INTEGER,
    lower_bound INTEGER,
    upper_bound INTEGER,
    range_basis VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    season = @doc('season'),
    game_type = @doc('game_type'),
    player_id = @doc('player_id'),
    team_id = 'Retrosheet team id from the Negro Leagues file.',
    line_type = 'Batting, Pitching, or Fielding.',
    fielding_position = 'Retrosheet''s position code for fielding lines; NULL for batting and pitching.',
    stat = 'Stat name, in the warehouse''s naming.',
    value = 'The exact value, when Retrosheet knows it.',
    lower_bound = 'Smallest value the stat can take.',
    upper_bound = 'Largest value the stat can take; NULL means unbounded.',
    range_basis = 'Exact when every summed line is exact; ReportedBound when Retrosheet gives a bound on at least one; Unknown otherwise.'
  ),
  audits (
    not_null(columns := (game_id, season, player_id, team_id, line_type, stat, lower_bound, range_basis)),
    unique_grain(columns := (game_id, player_id, team_id, line_type, fielding_position, stat)),
    accepted_values(column := range_basis, is_in := ('Exact', 'ReportedBound', 'Unknown')),
    accepted_values(column := line_type, is_in := ('Batting', 'Pitching', 'Fielding')),
    bounded_range(column := lower_bound, min_v := 0, max_v := upper_bound, condition := upper_bound IS NOT NULL)
  ),
);

WITH retrosheet_sourced_games AS (
    SELECT game_id, season::SMALLINT AS season, game_type
    FROM main_models.game_start_info
    WHERE game_id IN (SELECT game_id FROM main_models.stg_nlb_games)
        AND season NOT IN (
            SELECT season FROM main_models.nlb_season_sources WHERE season_source = 'Databank'
        )
),

line_stats AS (
    SELECT
        game_id,
        player_id,
        team_id,
        'Batting' AS line_type,
        NULL::VARCHAR AS fielding_position,
        UNNEST(@nlb_stat_range_structs('Batting', 'player')) AS r
    FROM main_models.stg_nlb_batting
    UNION ALL
    SELECT
        game_id,
        player_id,
        team_id,
        'Pitching' AS line_type,
        NULL::VARCHAR AS fielding_position,
        UNNEST(@nlb_stat_range_structs('Pitching', 'player')) AS r
    FROM main_models.stg_nlb_pitching
    UNION ALL
    SELECT
        game_id,
        player_id,
        team_id,
        'Fielding' AS line_type,
        position AS fielding_position,
        UNNEST(@nlb_stat_range_structs('Fielding', 'player')) AS r
    FROM main_models.stg_nlb_fielding
),

line_ranges AS (
    SELECT
        game_id,
        player_id,
        team_id,
        line_type,
        fielding_position,
        r.stat,
        r.value,
        COALESCE(r.value, r.lower_bound, 0) AS lower_bound,
        COALESCE(r.value, r.upper_bound) AS upper_bound,
        CASE
            WHEN r.value IS NOT NULL THEN 'Exact'
            WHEN r.lower_bound IS NOT NULL OR r.upper_bound IS NOT NULL THEN 'ReportedBound'
            ELSE 'Unknown'
        END AS range_basis
    FROM line_stats
),

merged AS (
    SELECT
        game_id,
        player_id,
        team_id,
        line_type,
        fielding_position,
        stat,
        CASE WHEN COUNT(value) = COUNT(*) THEN SUM(value) END::INTEGER AS value,
        SUM(lower_bound)::INTEGER AS lower_bound,
        CASE WHEN COUNT(upper_bound) = COUNT(*) THEN SUM(upper_bound) END::INTEGER AS upper_bound,
        CASE
            WHEN BOOL_AND(range_basis = 'Exact') THEN 'Exact'
            WHEN BOOL_OR(range_basis = 'ReportedBound') THEN 'ReportedBound'
            ELSE 'Unknown'
        END AS range_basis
    FROM line_ranges
    GROUP BY game_id, player_id, team_id, line_type, fielding_position, stat
),

final AS (
    SELECT
        merged.game_id,
        games.season,
        games.game_type,
        merged.* EXCLUDE (game_id)
    FROM merged
    INNER JOIN retrosheet_sourced_games AS games USING (game_id)
)

SELECT * FROM final
