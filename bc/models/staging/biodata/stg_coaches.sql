MODEL (
  name main_models.stg_coaches,
  kind FULL,
  description 'Per-coach assignments by year. From biodata/coaches.csv.',
  column_descriptions (
    person_id = 'Retrosheet personnel id (joins stg_bio.player_id).'
  ),
);






WITH source AS (
    SELECT * FROM biodata.coaches
),

renamed AS (
    SELECT
        id AS person_id,
        year,
        team AS team_id,
        role,
        start_date,
        end_date

    FROM source
)

SELECT * FROM renamed
