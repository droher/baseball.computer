WITH source AS (
    SELECT * FROM "timeball"."misc"."park"
),

renamed AS (
    SELECT
        park_id,
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