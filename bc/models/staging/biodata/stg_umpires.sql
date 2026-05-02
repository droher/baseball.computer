MODEL (
  name main_models.stg_umpires,
  kind FULL,
  description 'Umpire career index from biodata/umpires0.csv. Replaces per-year `umpires/UMPIRES{YYYY}.txt` files which the new fetcher does not mirror.',
  column_descriptions (
    person_id = 'Retrosheet personnel id (joins stg_bio.player_id).',
    first_game_date = 'Date of first umpired game.',
    last_game_date = 'Date of last umpired game.'
  ),
  audits (
    not_null(columns := (person_id)),
    unique_values(columns := (person_id))
  ),
);







WITH source AS (
    SELECT * FROM biodata.umpires0
),

renamed AS (
    SELECT
        id AS person_id,
        lastname AS last_name,
        firstname AS first_name,
        first_g AS first_game_date,
        last_g AS last_game_date

    FROM source
)

SELECT * FROM renamed
