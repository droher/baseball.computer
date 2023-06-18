WITH source AS (
    SELECT * FROM {{ source('event', 'event_flag') }}
),

renamed AS (
    SELECT
        event_key,
        sequence_id,
        flag,

    FROM source
)

SELECT * FROM renamed
