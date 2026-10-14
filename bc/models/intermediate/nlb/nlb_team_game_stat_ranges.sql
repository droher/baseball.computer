MODEL (
  name main_models.nlb_team_game_stat_ranges,
  kind FULL,
  description 'The range each Negro Leagues team-game counting stat can take, one row per game, team, line type, and stat, from Retrosheet''s team lines in negroleagues.zip, next to the sum of the team''s player ranges for the same stat. Ranges follow nlb_player_game_stat_ranges. is_outside_player_bounds flags a team range that cannot be reconciled with its players'' ranges, which points to an inconsistency in the source. Only seasons that nlb_season_sources assigns to Retrosheet are included.',
  grain (game_id, team_id, line_type, stat),
  columns (
    game_id VARCHAR,
    season SMALLINT,
    game_type GAME_TYPE,
    team_id VARCHAR,
    line_type VARCHAR,
    stat VARCHAR,
    value INTEGER,
    lower_bound INTEGER,
    upper_bound INTEGER,
    range_basis VARCHAR,
    player_lines UINTEGER,
    player_lower_bound INTEGER,
    player_upper_bound INTEGER,
    is_outside_player_bounds BOOLEAN
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    season = @doc('season'),
    game_type = @doc('game_type'),
    team_id = 'Retrosheet team id from the Negro Leagues file.',
    line_type = 'Batting, Pitching, or Fielding.',
    stat = 'Stat name, in the warehouse''s naming.',
    value = 'The team''s exact value, when Retrosheet knows it.',
    lower_bound = 'Smallest value the team stat can take.',
    upper_bound = 'Largest value the team stat can take; NULL means unbounded.',
    range_basis = 'Exact, ReportedBound, or Unknown, as in nlb_player_game_stat_ranges.',
    player_lines = 'Player-game rows summed into the player bounds; 0 when the game has no player lines for this stat, and always 0 for fielding double and triple plays, which every fielder in the play is credited with.',
    player_lower_bound = 'Sum of the players'' lower bounds; NULL when there are no player lines.',
    player_upper_bound = 'Sum of the players'' upper bounds; NULL when any is unbounded or there are no player lines.',
    is_outside_player_bounds = 'The team range and the summed player range do not overlap.'
  ),
  audits (
    not_null(columns := (game_id, season, team_id, line_type, stat, lower_bound, range_basis)),
    unique_grain(columns := (game_id, team_id, line_type, stat)),
    accepted_values(column := range_basis, is_in := ('Exact', 'ReportedBound', 'Unknown')),
    bounded_range(column := lower_bound, min_v := 0, max_v := upper_bound, condition := upper_bound IS NOT NULL),
    forall(criteria := (NOT is_outside_player_bounds), blocking := FALSE)
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

team_stats AS (
    SELECT game_id, team_id, 'Batting' AS line_type, UNNEST(@nlb_stat_range_structs('Batting', 'team')) AS r
    FROM main_models.stg_nlb_teams
    UNION ALL
    SELECT game_id, team_id, 'Pitching' AS line_type, UNNEST(@nlb_stat_range_structs('Pitching', 'team')) AS r
    FROM main_models.stg_nlb_teams
    UNION ALL
    SELECT game_id, team_id, 'Fielding' AS line_type, UNNEST(@nlb_stat_range_structs('Fielding', 'team')) AS r
    FROM main_models.stg_nlb_teams
),

team_ranges AS (
    SELECT
        game_id,
        team_id,
        line_type,
        r.stat,
        r.value,
        COALESCE(r.value, r.lower_bound, 0) AS lower_bound,
        COALESCE(r.value, r.upper_bound) AS upper_bound,
        CASE
            WHEN r.value IS NOT NULL THEN 'Exact'
            WHEN r.lower_bound IS NOT NULL OR r.upper_bound IS NOT NULL THEN 'ReportedBound'
            ELSE 'Unknown'
        END AS range_basis
    FROM team_stats
),

player_sums AS (
    SELECT
        game_id,
        team_id,
        line_type,
        stat,
        COUNT(*) AS player_lines,
        SUM(lower_bound) AS player_lower_bound,
        CASE WHEN COUNT(upper_bound) = COUNT(*) THEN SUM(upper_bound) END AS player_upper_bound
    FROM main_models.nlb_player_game_stat_ranges
    WHERE NOT (line_type = 'Fielding' AND stat IN ('double_plays', 'triple_plays'))
    GROUP BY game_id, team_id, line_type, stat
),

final AS (
    SELECT
        team_ranges.game_id,
        games.season,
        games.game_type,
        team_ranges.team_id,
        team_ranges.line_type,
        team_ranges.stat,
        team_ranges.value,
        team_ranges.lower_bound,
        team_ranges.upper_bound,
        team_ranges.range_basis,
        COALESCE(player_sums.player_lines, 0)::UINTEGER AS player_lines,
        player_sums.player_lower_bound::INTEGER AS player_lower_bound,
        player_sums.player_upper_bound::INTEGER AS player_upper_bound,
        COALESCE(
            team_ranges.lower_bound > player_sums.player_upper_bound
                OR team_ranges.upper_bound < player_sums.player_lower_bound,
            FALSE
        ) AS is_outside_player_bounds
    FROM team_ranges
    INNER JOIN retrosheet_sourced_games AS games USING (game_id)
    LEFT JOIN player_sums
        ON team_ranges.game_id = player_sums.game_id
            AND team_ranges.team_id = player_sums.team_id
            AND team_ranges.line_type = player_sums.line_type
            AND team_ranges.stat = player_sums.stat
)

SELECT * FROM final
