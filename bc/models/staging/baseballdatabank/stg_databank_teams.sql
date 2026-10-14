MODEL (
  name main_models.stg_databank_teams,
  kind FULL,
  description 'Lahman team-seasons with the Retrosheet team id the Lahman staging models use. For major-league rows team_id is teams.team_id_retro. For Negro Leagues rows it is the seed_nlb_lahman_team_crosswalk row when one exists, otherwise team_id_retro, and it is NULL when that id is not in seed_franchises; nlb_lahman_team_resolution lists those team-seasons.',
  grain (season, databank_team_id),
  columns (
    season SMALLINT,
    databank_team_id VARCHAR,
    league_id VARCHAR,
    is_negro_leagues BOOLEAN,
    team_name VARCHAR,
    team_id_retro VARCHAR,
    crosswalk_team_id VARCHAR,
    team_id VARCHAR,
    games USMALLINT,
    wins USMALLINT,
    losses USMALLINT,
    league_rank UTINYINT,
    won_pennant BOOLEAN
  ),
  column_descriptions (
    season = @doc('season'),
    databank_team_id = 'Lahman teams.team_id.',
    league_id = 'Lahman lg_id.',
    is_negro_leagues = 'Whether lg_id is a Negro Leagues league rather than a major league (AL, NL, AA, UA, PL, FL, NA).',
    team_name = 'Lahman team name.',
    team_id_retro = 'Lahman''s Retrosheet team id.',
    crosswalk_team_id = 'Team id from seed_nlb_lahman_team_crosswalk, if the team-season has a row there.',
    team_id = 'Retrosheet team id used by the Lahman staging models.',
    games = 'Lahman standings games.',
    wins = 'Lahman standings wins.',
    losses = 'Lahman standings losses.',
    league_rank = 'Lahman final rank in the league.',
    won_pennant = 'Lahman lg_win; it can differ from first place.'
  ),
  audits (
    not_null(columns := (season, databank_team_id, is_negro_leagues)),
    unique_grain(columns := (season, databank_team_id)),
    not_null(columns := (team_id), condition := NOT is_negro_leagues)
  ),
);

WITH franchise_ids AS (
    SELECT DISTINCT team_id FROM main_seeds.seed_franchises
),

final AS (
    SELECT
        teams.year_id::SMALLINT AS season,
        teams.team_id AS databank_team_id,
        teams.lg_id AS league_id,
        @is_negro_leagues_league(teams.lg_id) AS is_negro_leagues,
        teams.name AS team_name,
        teams.team_id_retro,
        crosswalk.team_id AS crosswalk_team_id,
        CASE
            WHEN NOT is_negro_leagues THEN teams.team_id_retro
            WHEN COALESCE(crosswalk.team_id, teams.team_id_retro) IN (SELECT team_id FROM franchise_ids)
                THEN COALESCE(crosswalk.team_id, teams.team_id_retro)
        END AS team_id,
        teams.g::USMALLINT AS games,
        teams.w::USMALLINT AS wins,
        teams.l::USMALLINT AS losses,
        teams.rank::UTINYINT AS league_rank,
        teams.lg_win = 'Y' AS won_pennant
    FROM baseballdatabank.teams AS teams
    LEFT JOIN main_seeds.seed_nlb_lahman_team_crosswalk AS crosswalk
        ON teams.year_id = crosswalk.season AND teams.team_id = crosswalk.databank_team_id
)

SELECT * FROM final
