MODEL (
  name main_models.stg_rosters,
  kind FULL,
  grain (year, player_id, team_id),
  columns (
    year INTEGER,
    player_id VARCHAR,
    last_name VARCHAR,
    first_name VARCHAR,
    bats HAND,
    throws HAND,
    team_id TEAM_ID,
    position VARCHAR
  ),
  column_descriptions (
    player_id = @doc('player_id'),
    team_id = @doc('team_id')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_rosters.parquet'
  ),
);







WITH source AS (
    SELECT * FROM misc.roster
),

renamed AS (
    SELECT
        year,
        player_id,
        last_name,
        first_name,
        NULLIF(bats, '?') AS bats,
        NULLIF(throws, '?') AS throws,
        team_id,
        position

    FROM source
    -- TODO: Modify upstream deduper to handle multiple positions on same team
    -- (or talk to retrosheet)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY year, team_id, player_id
        ORDER BY position, last_name, first_name, bats, throws
    ) = 1
)

SELECT * FROM renamed
