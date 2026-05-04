MODEL (
  name main_models.stg_people,
  kind FULL,
  grain (databank_player_id),
  columns (
    retrosheet_player_id VARCHAR,
    baseball_reference_player_id VARCHAR,
    databank_player_id VARCHAR,
    birth_year SMALLINT,
    birth_month SMALLINT,
    birth_day SMALLINT,
    birth_country VARCHAR,
    birth_state VARCHAR,
    birth_city VARCHAR,
    death_year SMALLINT,
    death_month SMALLINT,
    death_day SMALLINT,
    death_country VARCHAR,
    death_state VARCHAR,
    death_city VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    given_name VARCHAR,
    weight_pounds SMALLINT,
    height_inches DOUBLE,
    bats VARCHAR,
    throws VARCHAR,
    debut TIMESTAMP,
    final_game TIMESTAMP,
    internal_id BIGINT
  ),
  column_descriptions (
    databank_player_id = @doc('databank_player_id')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_people.parquet'
  ),
);







WITH source AS (
    SELECT * FROM baseballdatabank.people
),

renamed AS (
    SELECT
        retro_id AS retrosheet_player_id,
        bbref_id AS baseball_reference_player_id,
        player_id AS databank_player_id,
        birth_year AS birth_year,
        birth_month AS birth_month,
        birth_day AS birth_day,
        birth_country AS birth_country,
        birth_state AS birth_state,
        birth_city AS birth_city,
        death_year AS death_year,
        death_month AS death_month,
        death_day AS death_day,
        death_country AS death_country,
        death_state AS death_state,
        death_city AS death_city,
        name_first AS first_name,
        name_last AS last_name,
        name_given AS given_name,
        weight AS weight_pounds,
        height AS height_inches,
        bats,
        throws,
        debut,
        final_game AS final_game,
        ROW_NUMBER() OVER (order by player_id) AS internal_id
    FROM source
)

SELECT * FROM renamed
