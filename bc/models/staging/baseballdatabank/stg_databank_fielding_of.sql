MODEL (
  name main_models.stg_databank_fielding_of,
  kind FULL,
  description 'Gives information about the number of games played at each outfield position by player, season, and stint with a given team. This table is important for 19th century data because fielding stats are aggregated across outfield positions, so we need to infer which stats they accumulated at each position.',
  grain (databank_player_id, season, stint),
  columns (
    databank_player_id VARCHAR,
    season SMALLINT,
    stint SMALLINT,
    games_left_field SMALLINT,
    games_center_field SMALLINT,
    games_right_field SMALLINT
  ),
  column_descriptions (
    databank_player_id = @doc('databank_player_id'),
    season = @doc('season'),
    stint = @doc('stint'),
    games_left_field = 'Total number of games played in left field',
    games_center_field = 'Total number of games played in center field',
    games_right_field = 'Total number of games played in right field'
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_databank_fielding_of.parquet'
  ),
);







WITH source AS (
    SELECT * FROM baseballdatabank.fielding_of
),

renamed AS (
    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        stint,
        g_lf AS games_left_field,
        g_cf AS games_center_field,
        g_rf AS games_right_field,
    FROM source
)

SELECT * FROM renamed
