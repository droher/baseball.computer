WITH source AS (
    SELECT * FROM "timeball"."event"."event_flags"
),

renamed AS (
    SELECT
        event_key,
        sequence_id,
        flag,

    FROM source
)

SELECT * FROM renamed