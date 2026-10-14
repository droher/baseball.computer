MODEL (
  name main_models.stg_people,
  kind FULL,
  description 'Lahman people, one row per Databank player. retrosheet_player_id and baseball_reference_player_id are Lahman''s retro_id and bbref_id unless seed_lahman_people_id_corrections has a row for the player, which replaces both. The corrections fix ids that Lahman gives to more than one person, so both ids are unique here. player_id is the Retrosheet id, or the Databank id for people Retrosheet does not have.',
  grain (databank_player_id),
  columns (
    player_id VARCHAR,
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
    player_id = 'Player id the Lahman-sourced models use: retrosheet_player_id, or databank_player_id when the person has no Retrosheet id.',
    databank_player_id = @doc('databank_player_id')
  ),
  audits (
    unique_values(columns := (databank_player_id)),
    unique_values(columns := (player_id)),
    unique_values(columns := (retrosheet_player_id), condition := retrosheet_player_id IS NOT NULL),
    unique_values(columns := (baseball_reference_player_id), condition := baseball_reference_player_id IS NOT NULL)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_people.parquet'
  ),
);







WITH source AS (
    SELECT
        people.* REPLACE (
            CASE WHEN corrections.databank_player_id IS NOT NULL
                THEN corrections.retrosheet_player_id ELSE people.retro_id
            END AS retro_id,
            CASE WHEN corrections.databank_player_id IS NOT NULL
                THEN corrections.baseball_reference_player_id ELSE people.bbref_id
            END AS bbref_id
        )
    FROM baseballdatabank.people AS people
    LEFT JOIN main_seeds.seed_lahman_people_id_corrections AS corrections
        ON people.player_id = corrections.databank_player_id
),

renamed AS (
    SELECT
        COALESCE(retro_id, player_id) AS player_id,
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
