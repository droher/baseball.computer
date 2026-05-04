MODEL (
  name main_models.stg_relatives,
  kind FULL,
  description 'Player-family relationships graph. From biodata/relatives.csv.',
  column_descriptions (
    person_id = 'First personnel id (joins stg_bio.player_id).',
    relation = 'Relationship type (Brother, Father, etc.).',
    related_person_id = 'Second personnel id (joins stg_bio.player_id).'
  ),
);






WITH source AS (
    SELECT * FROM biodata.relatives
),

renamed AS (
    SELECT
        id1 AS person_id,
        relation,
        id2 AS related_person_id

    FROM source
)

SELECT * FROM renamed
