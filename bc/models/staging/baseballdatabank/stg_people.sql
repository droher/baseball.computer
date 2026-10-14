MODEL (
  name main_models.stg_people,
  kind FULL,
  description 'Lahman people, one row per Databank player. retrosheet_player_id is Lahman''s retro_id unless seed_nlb_lahman_player_crosswalk has a row for the player. When several Lahman people share a Retrosheet id, only one keeps it: a crosswalk row first, then a birth year matching the Retrosheet biofile, then a first initial matching it, then Lahman''s own bbref_id matching the player id; the others get NULL and appear in nlb_lahman_player_resolution.',
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
  audits (
    unique_values(columns := (databank_player_id)),
    unique_values(columns := (retrosheet_player_id), condition := retrosheet_player_id IS NOT NULL)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_people.parquet'
  ),
);







WITH linked AS (
    SELECT
        people.*,
        COALESCE(crosswalk.retrosheet_player_id, people.retro_id) AS linked_retro_id,
        crosswalk.retrosheet_player_id IS NOT NULL AS is_crosswalk_link
    FROM baseballdatabank.people AS people
    LEFT JOIN main_seeds.seed_nlb_lahman_player_crosswalk AS crosswalk
        ON people.player_id = crosswalk.databank_player_id
),

ranked AS (
    SELECT
        linked.*,
        ROW_NUMBER() OVER (
            PARTITION BY linked.linked_retro_id
            ORDER BY
                linked.is_crosswalk_link DESC,
                COALESCE(linked.birth_year = TRY_CAST(REGEXP_EXTRACT(bio.birth_date, '(\d{4})', 1) AS INTEGER), FALSE) DESC,
                COALESCE(
                    LEFT(LOWER(linked.name_first), 1) IN (
                        LEFT(LOWER(bio.official_name), 1), LEFT(LOWER(bio.first_name), 1)
                    ),
                    FALSE
                ) DESC,
                COALESCE(linked.bbref_id = linked.player_id, FALSE) DESC,
                linked.player_id
        ) AS retro_id_rank
    FROM linked
    LEFT JOIN main_models.stg_bio AS bio ON linked.linked_retro_id = bio.player_id
),

source AS (
    SELECT
        * EXCLUDE (linked_retro_id, is_crosswalk_link, retro_id_rank)
            REPLACE (CASE WHEN retro_id_rank = 1 THEN linked_retro_id END AS retro_id)
    FROM ranked
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
