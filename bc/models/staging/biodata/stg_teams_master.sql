MODEL (
  name main_models.stg_teams_master,
  kind FULL,
  description 'Per-team master from Retrosheet''s biodata bundle (teams.csv). Replaces the per-year `teams/team{YYYY}*.csv` files which are no longer mirrored by the new fetcher.',
  grain (team_id),
  columns (
    team_id VARCHAR,
    league VARCHAR,
    city VARCHAR,
    nickname VARCHAR,
    first_year SMALLINT,
    last_year SMALLINT
  ),
  column_descriptions (
    team_id = 'Retrosheet team id.',
    first_year = 'First season the team existed.',
    last_year = 'Last season the team existed.'
  ),
);







WITH source AS (
    SELECT * FROM biodata.teams
),

renamed AS (
    SELECT
        team AS team_id,
        league,
        city,
        nickname,
        first_year,
        last_year

    FROM source
)

SELECT * FROM renamed
