MODEL (
  name main_models.player_team_season_roles,
  kind FULL,
  description 'Regular-season pitcher/position-player role for every player-team-season with a batting, pitching, or fielding line.',
  grain (season, team_id, player_id),
  column_descriptions (
    season = @doc('season'),
    team_id = @doc('team_id'),
    player_id = @doc('player_id'),
    pitching_outs = 'Outs recorded as a pitcher.',
    non_pitching_outs_played = 'Outs played in the field (or at DH) at any position other than pitcher.',
    is_primary_pitcher = 'Whether the player has a pitching line and at least half of their outs on the field came as a pitcher.'
  ),
  audits (
    not_null(columns := (season, team_id, player_id, pitching_outs, non_pitching_outs_played, is_primary_pitcher)),
    unique_grain(columns := (season, team_id, player_id)),
    valid_baseball_season(column := season)
  ),
);

WITH pitching AS (
    SELECT
        season,
        team_id,
        player_id,
        SUM(COALESCE(outs_recorded, 0)) AS pitching_outs,
    FROM main_models.player_team_season_pitching_stats
    WHERE game_type = 'RegularSeason'
    GROUP BY 1, 2, 3
),

fielding AS (
    SELECT
        season,
        team_id,
        player_id,
        SUM(COALESCE(outs_played, 0)) FILTER (WHERE fielding_position != 1) AS non_pitching_outs_played,
    FROM main_models.player_position_team_season_fielding_stats
    WHERE game_type = 'RegularSeason'
    GROUP BY 1, 2, 3
),

players AS (
    SELECT season, team_id, player_id FROM pitching
    UNION
    SELECT season, team_id, player_id FROM fielding
    UNION
    SELECT season, team_id, player_id
    FROM main_models.player_team_season_offense_stats
    WHERE game_type = 'RegularSeason'
),

final AS (
    SELECT
        players.season,
        players.team_id,
        players.player_id,
        COALESCE(pitching.pitching_outs, 0) AS pitching_outs,
        COALESCE(fielding.non_pitching_outs_played, 0) AS non_pitching_outs_played,
        pitching.player_id IS NOT NULL
        AND COALESCE(pitching.pitching_outs, 0) >= COALESCE(fielding.non_pitching_outs_played, 0)
            AS is_primary_pitcher,
    FROM players
    LEFT JOIN pitching USING (season, team_id, player_id)
    LEFT JOIN fielding USING (season, team_id, player_id)
)

SELECT * FROM final
