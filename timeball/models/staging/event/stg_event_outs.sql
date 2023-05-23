WITH source AS (
    SELECT * FROM {{ source('event', 'event_out') }}
),

renamed AS (
    SELECT
        event_key::INT AS event_key,
        sequence_id,
        baserunner_out,
        event_key || '-' || sequence_id AS sequence_key

    FROM source
)

SELECT * FROM renamed
