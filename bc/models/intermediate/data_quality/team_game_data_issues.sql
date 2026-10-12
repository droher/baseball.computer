MODEL (
  name main_models.team_game_data_issues,
  kind FULL,
  grain (game_id, team_id, issue_type),
  description 'Team-game cross-table data artifacts that ordinary game-grain audits would otherwise flag as bugs. `starting_pitcher_no_appearance`: PlayByPlay-source games where `game_start_info` records a starting pitcher who never threw a pitch (e.g. a last-minute scratch). Per MLB rules a scratched starter is still the SP, so `player_game_pitching_stats` legitimately has zero rows with `games_started = 1` for the team-game; `team_game_has_one_starter` and the relief-CG carve-out exclude these team-games. `nlb_box_score_starting_pitcher_order_ambiguous`: Negro League or pre-1875 box-score team-games where the recorded pitching lines do not mark exactly one pitcher as first. These rows are informational: they document why such team-games appear in the non-blocking `team_game_has_one_starter` warning, and no audit excludes them.',
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
),

box_score_pitcher_sides AS (
    SELECT
        game_id,
        'Home' AS side,
        home_team_id AS team_id
    FROM main_models.game_start_info
    WHERE source_type = 'BoxScore'
        AND @is_less_complete_source(game_start_info)
    UNION ALL
    SELECT
        game_id,
        'Away' AS side,
        away_team_id AS team_id
    FROM main_models.game_start_info
    WHERE source_type = 'BoxScore'
        AND @is_less_complete_source(game_start_info)
),

box_score_pitcher_order AS (
    SELECT
        sides.game_id,
        sides.team_id,
        'nlb_box_score_starting_pitcher_order_ambiguous'::VARCHAR AS issue_type,
        (
            'found '
            || COUNT(p.nth_pitcher) FILTER (WHERE p.nth_pitcher = 1)::VARCHAR
            || ' pitchers marked nth_pitcher=1; source rows: '
            || COALESCE(STRING_AGG(
                COALESCE(p.pitcher_id, 'NULL')
                || ':'
                || COALESCE(p.nth_pitcher::VARCHAR, 'NULL'),
                ', ' ORDER BY p.nth_pitcher, p.pitcher_id
            ), 'none')
        )::VARCHAR AS notes,
    FROM box_score_pitcher_sides AS sides
    LEFT JOIN main_models.stg_box_score_pitching_lines AS p
        ON p.game_id = sides.game_id
        AND p.side = sides.side
    GROUP BY sides.game_id, sides.team_id
    HAVING COUNT(p.nth_pitcher) FILTER (WHERE p.nth_pitcher = 1) != 1
)

SELECT * FROM scratched
UNION ALL BY NAME
SELECT * FROM box_score_pitcher_order
