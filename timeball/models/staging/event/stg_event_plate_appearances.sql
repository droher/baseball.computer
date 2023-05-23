WITH source AS (
    SELECT * FROM {{ source('event', 'event_plate_appearance') }}
),

renamed AS (
    SELECT
        event_key::INT AS event_key,
        plate_appearance_result,
    FROM source
)

SELECT * FROM renamed
