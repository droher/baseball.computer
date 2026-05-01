WITH source AS (
    SELECT * FROM {{ source('biodata', 'relatives') }}
),

renamed AS (
    SELECT
        id1 AS person_id,
        relation,
        id2 AS related_person_id

    FROM source
)

SELECT * FROM renamed
