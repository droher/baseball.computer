WITH source AS (
    SELECT * FROM {{ source('biodata', 'coaches') }}
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
