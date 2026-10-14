MODEL (
  name main_models.nlb_team_season_discrepancies,
  kind FULL,
  description 'Lahman and Retrosheet side by side for each Negro Leagues team-season either source has, one row per season and team. Lahman columns come from its teams and batting tables; Lahman team-seasons with no resolved Retrosheet team id appear with team_id NULL. Retrosheet columns come from negroleagues.zip; its at-bats, hits, and runs count only matched-scope games with a box score (see nlb_game_scopes), the scope Lahman''s batting lines cover. Retrosheet team-seasons appear when they have a regular-season or major-club exhibition game.',
  grain (season, team_id, databank_team_id),
  columns (
    season SMALLINT,
    team_id VARCHAR,
    databank_team_id VARCHAR,
    lahman_team_name VARCHAR,
    lahman_league_id VARCHAR,
    lahman_games USMALLINT,
    lahman_wins USMALLINT,
    lahman_losses USMALLINT,
    lahman_at_bats UINTEGER,
    lahman_hits UINTEGER,
    lahman_runs UINTEGER,
    retrosheet_games UINTEGER,
    retrosheet_box_score_games UINTEGER,
    retrosheet_appearances_only_games UINTEGER,
    retrosheet_regular_season_games UINTEGER,
    retrosheet_major_club_exhibition_games UINTEGER,
    retrosheet_other_exhibition_games UINTEGER,
    retrosheet_postseason_games UINTEGER,
    retrosheet_regular_season_wins UINTEGER,
    retrosheet_regular_season_losses UINTEGER,
    retrosheet_at_bats UINTEGER,
    retrosheet_hits UINTEGER,
    retrosheet_runs UINTEGER,
    at_bats_ratio DOUBLE,
    coverage_category VARCHAR
  ),
  column_descriptions (
    season = @doc('season'),
    team_id = 'Retrosheet team id; for Lahman rows, the id resolved by stg_databank_teams.',
    databank_team_id = 'Lahman teams.team_id; NULL for Retrosheet-only rows.',
    lahman_games = 'Lahman standings games.',
    lahman_at_bats = 'Sum of Lahman batting at-bats for the team-season.',
    retrosheet_games = 'All Retrosheet games for the team-season.',
    retrosheet_box_score_games = 'Retrosheet games with a box score.',
    retrosheet_appearances_only_games = 'Retrosheet games without a box score.',
    retrosheet_at_bats = 'Retrosheet at-bats in matched-scope games with a box score.',
    at_bats_ratio = 'lahman_at_bats / retrosheet_at_bats.',
    coverage_category = 'LahmanOnly, RetrosheetOnly, Within10Percent, RetrosheetHigher (by more than 10%), LahmanHigher (by 10 to 50%), or LahmanMuchHigher (by more than 50%).'
  ),
  audits (
    not_null(columns := (season, coverage_category)),
    unique_grain(columns := (season, team_id, databank_team_id)),
    accepted_values(column := coverage_category, is_in := ('LahmanOnly', 'RetrosheetOnly', 'Within10Percent', 'RetrosheetHigher', 'LahmanHigher', 'LahmanMuchHigher'))
  ),
);

WITH lahman_batting AS (
    SELECT
        season,
        databank_team_id,
        SUM(at_bats) AS at_bats,
        SUM(hits) AS hits,
        SUM(runs) AS runs
    FROM main_models.stg_databank_batting
    WHERE is_negro_leagues
    GROUP BY 1, 2
),

lahman AS (
    SELECT
        teams.season,
        teams.team_id,
        teams.databank_team_id,
        teams.team_name AS lahman_team_name,
        teams.league_id AS lahman_league_id,
        teams.games AS lahman_games,
        teams.wins AS lahman_wins,
        teams.losses AS lahman_losses,
        batting.at_bats AS lahman_at_bats,
        batting.hits AS lahman_hits,
        batting.runs AS lahman_runs
    FROM main_models.stg_databank_teams AS teams
    LEFT JOIN lahman_batting AS batting USING (season, databank_team_id)
    WHERE teams.is_negro_leagues
),

team_games AS (
    SELECT game_id, season, home_team_id AS team_id, game_scope, is_matched_scope, has_box_score
    FROM main_models.nlb_game_scopes
    UNION ALL
    SELECT game_id, season, away_team_id AS team_id, game_scope, is_matched_scope, has_box_score
    FROM main_models.nlb_game_scopes
),

team_batting AS (
    SELECT
        game_id,
        team_id,
        SUM(b_ab) AS at_bats,
        SUM(b_h) AS hits,
        SUM(b_r) AS runs
    FROM main_models.stg_nlb_batting
    GROUP BY 1, 2
),

retrosheet AS (
    SELECT
        team_games.season,
        team_games.team_id,
        COUNT(*) AS retrosheet_games,
        COUNT_IF(team_games.has_box_score) AS retrosheet_box_score_games,
        COUNT_IF(NOT team_games.has_box_score) AS retrosheet_appearances_only_games,
        COUNT_IF(team_games.game_scope = 'RegularSeason') AS retrosheet_regular_season_games,
        COUNT_IF(team_games.game_scope = 'MajorClubExhibition') AS retrosheet_major_club_exhibition_games,
        COUNT_IF(team_games.game_scope = 'OtherExhibition') AS retrosheet_other_exhibition_games,
        COUNT_IF(team_games.game_scope = 'Postseason') AS retrosheet_postseason_games,
        COUNT_IF(team_games.game_scope = 'RegularSeason' AND teams.is_win) AS retrosheet_regular_season_wins,
        COUNT_IF(team_games.game_scope = 'RegularSeason' AND teams.is_loss) AS retrosheet_regular_season_losses,
        COALESCE(SUM(batting.at_bats) FILTER (WHERE team_games.is_matched_scope AND team_games.has_box_score), 0)
            AS retrosheet_at_bats,
        COALESCE(SUM(batting.hits) FILTER (WHERE team_games.is_matched_scope AND team_games.has_box_score), 0)
            AS retrosheet_hits,
        COALESCE(SUM(batting.runs) FILTER (WHERE team_games.is_matched_scope AND team_games.has_box_score), 0)
            AS retrosheet_runs
    FROM team_games
    LEFT JOIN main_models.stg_nlb_teams AS teams USING (game_id, team_id)
    LEFT JOIN team_batting AS batting USING (game_id, team_id)
    GROUP BY 1, 2
    HAVING BOOL_OR(team_games.is_matched_scope)
),

joined AS (
    SELECT
        COALESCE(lahman.season, retrosheet.season) AS season,
        COALESCE(lahman.team_id, retrosheet.team_id) AS team_id,
        lahman.* EXCLUDE (season, team_id),
        retrosheet.* EXCLUDE (season, team_id),
        lahman.databank_team_id IS NOT NULL AS in_lahman,
        retrosheet.season IS NOT NULL AND retrosheet.retrosheet_at_bats > 0 AS in_retrosheet
    FROM lahman
    FULL OUTER JOIN retrosheet
        ON lahman.season = retrosheet.season AND lahman.team_id = retrosheet.team_id
),

final AS (
    SELECT
        * EXCLUDE (in_lahman, in_retrosheet),
        lahman_at_bats / NULLIF(retrosheet_at_bats, 0) AS at_bats_ratio,
        CASE
            WHEN NOT in_retrosheet THEN 'LahmanOnly'
            WHEN NOT in_lahman OR COALESCE(lahman_at_bats, 0) = 0 THEN 'RetrosheetOnly'
            WHEN at_bats_ratio > 1.5 THEN 'LahmanMuchHigher'
            WHEN at_bats_ratio > 1.1 THEN 'LahmanHigher'
            WHEN at_bats_ratio < 1 / 1.1 THEN 'RetrosheetHigher'
            ELSE 'Within10Percent'
        END AS coverage_category
    FROM joined
)

SELECT * FROM final
