WITH source AS (
    SELECT * FROM {{ source('event', 'event_out') }}
),

renamed AS (
    SELECT
        event_key,
        sequence_id,
        baserunner_out,

    FROM source
)

SELECT * FROM renamed
