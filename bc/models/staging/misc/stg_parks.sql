WITH source AS (
    SELECT * FROM {{ source('misc', 'park') }}
),

renamed AS (
    SELECT
        park_id::park_id AS park_id,
        name,
        aka,
        city,
        state,
        start_date,
        end_date,
        league,
        notes

    FROM source
)

SELECT * FROM renamed
