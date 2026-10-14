MODEL (
  name main_models.stg_databank_teams,
  kind FULL,
  description 'Lahman team-seasons with the team id and league the Lahman staging models use. For major-league rows team_id is teams.team_id_retro. For Negro Leagues rows it is the seed_nlb_lahman_team_crosswalk row when one exists, otherwise team_id_retro when that id is in seed_franchises, otherwise Lahman''s own team id, so teams Retrosheet does not have keep their Lahman id. league_id is the seed_leagues code for Lahman''s lg_id, which is Retrosheet''s code where the two differ, and NULL for independent teams (lg_id IND).',
  grain (season, databank_team_id),
  columns (
    season SMALLINT,
    databank_team_id VARCHAR,
    databank_league_id VARCHAR,
    league_id VARCHAR,
    is_negro_leagues BOOLEAN,
    team_name VARCHAR,
    team_id_retro VARCHAR,
    crosswalk_team_id VARCHAR,
    team_id VARCHAR,
    is_databank_team_id BOOLEAN,
    games USMALLINT,
    wins USMALLINT,
    losses USMALLINT,
    league_rank UTINYINT,
    won_pennant BOOLEAN
  ),
  column_descriptions (
    season = @doc('season'),
    databank_team_id = 'Lahman teams.team_id.',
    databank_league_id = 'Lahman lg_id.',
    league_id = @doc('league'),
    is_negro_leagues = 'Whether lg_id is a Negro Leagues league rather than a major league (AL, NL, AA, UA, PL, FL, NA).',
    team_name = 'Lahman team name.',
    team_id_retro = 'Lahman''s Retrosheet team id.',
    crosswalk_team_id = 'Team id from seed_nlb_lahman_team_crosswalk, if the team-season has a row there.',
    team_id = 'Team id used by the Lahman staging models: Retrosheet''s id, or Lahman''s when Retrosheet has no team for the team-season.',
    is_databank_team_id = 'Whether team_id is Lahman''s team id because no crosswalk row or Retrosheet team id applies.',
    games = 'Lahman standings games.',
    wins = 'Lahman standings wins.',
    losses = 'Lahman standings losses.',
    league_rank = 'Lahman final rank in the league.',
    won_pennant = 'Lahman lg_win; it can differ from first place.'
  ),
  audits (
    unique_grain(columns := (season, databank_team_id)),
    not_null(columns := (season, databank_team_id, team_id, is_negro_leagues, is_databank_team_id)),
    not_null(columns := (league_id), condition := databank_league_id != 'IND'),
    team_season_in_franchises(condition := is_negro_leagues),
    databank_team_id_not_retrosheet_team()
  ),
);

WITH franchise_ids AS (
    SELECT DISTINCT team_id FROM main_seeds.seed_franchises
),

resolved AS (
    SELECT
        teams.*,
        @is_negro_leagues_league(teams.lg_id) AS is_negro_leagues,
        crosswalk.team_id AS crosswalk_team_id,
        CASE WHEN teams.team_id_retro IN (SELECT team_id FROM franchise_ids) THEN teams.team_id_retro END
            AS franchise_team_id_retro,
        leagues.league_id
    FROM baseballdatabank.teams AS teams
    LEFT JOIN main_seeds.seed_nlb_lahman_team_crosswalk AS crosswalk
        ON teams.year_id = crosswalk.season AND teams.team_id = crosswalk.databank_team_id
    LEFT JOIN main_seeds.seed_leagues AS leagues
        ON teams.lg_id = leagues.databank_league_id
),

final AS (
    SELECT
        year_id::SMALLINT AS season,
        team_id AS databank_team_id,
        lg_id AS databank_league_id,
        league_id,
        is_negro_leagues,
        name AS team_name,
        team_id_retro,
        crosswalk_team_id,
        CASE
            WHEN NOT is_negro_leagues THEN team_id_retro
            ELSE COALESCE(crosswalk_team_id, franchise_team_id_retro, team_id)
        END AS team_id,
        is_negro_leagues AND crosswalk_team_id IS NULL AND franchise_team_id_retro IS NULL
            AS is_databank_team_id,
        g::USMALLINT AS games,
        w::USMALLINT AS wins,
        l::USMALLINT AS losses,
        rank::UTINYINT AS league_rank,
        lg_win = 'Y' AS won_pennant
    FROM resolved
)

SELECT * FROM final
