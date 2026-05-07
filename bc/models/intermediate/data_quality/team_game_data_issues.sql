MODEL (
  name main_models.team_game_data_issues,
  kind FULL,
  grain (game_id, team_id, issue_type),
  description 'Team-game cross-table data artifacts that ordinary game-grain audits would otherwise flag as bugs. The current population is `starting_pitcher_no_appearance`: PlayByPlay-source games where `game_start_info` records a starting pitcher who never threw a pitch (e.g. a last-minute scratch). Per MLB rules a scratched starter is still the SP, so `player_game_pitching_stats` legitimately has zero rows with `games_started = 1` for the team-game. Used by `team_game_has_one_starter_excluding_data_issues` and the relief-CG carve-out so the audits land clean.',
  column_descriptions (
    game_id = @doc('game_id'),
    team_id = @doc('team_id'),
    issue_type = 'Stable identifier for the data artifact. Audits filter on this column.',
    notes = 'Optional free-form context (e.g. the recorded SP that never appeared).'
  ),
  audits (
    not_null(columns := (game_id, team_id, issue_type)),
    unique_grain(columns := (game_id, team_id, issue_type))
  ),
);

WITH gsi_sp AS (
    SELECT
        g.game_id,
        gsi.home_team_id AS team_id,
        gsi.home_starting_pitcher_id AS starting_pitcher_id,
        g.source_type,
    FROM main_models.game_start_info AS gsi
    JOIN main_models.stg_games AS g USING (game_id)
    WHERE gsi.home_starting_pitcher_id IS NOT NULL
    UNION ALL
    SELECT
        g.game_id,
        gsi.away_team_id AS team_id,
        gsi.away_starting_pitcher_id AS starting_pitcher_id,
        g.source_type,
    FROM main_models.game_start_info AS gsi
    JOIN main_models.stg_games AS g USING (game_id)
    WHERE gsi.away_starting_pitcher_id IS NOT NULL
),

scratched AS (
    SELECT
        sp.game_id,
        sp.team_id,
        'starting_pitcher_no_appearance'::VARCHAR AS issue_type,
        ('recorded SP ' || sp.starting_pitcher_id || ' never appeared in events')::VARCHAR AS notes,
    FROM gsi_sp AS sp
    WHERE sp.source_type = 'PlayByPlay'
        AND NOT EXISTS (
            SELECT 1 FROM main_models.event_pitching_stats AS e
            WHERE e.game_id = sp.game_id
                AND e.team_id = sp.team_id
                AND e.player_id = sp.starting_pitcher_id
        )
)

SELECT * FROM scratched
