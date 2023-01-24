WITH source AS (
    SELECT * FROM {{ source('event', 'event_plate_appearance') }}
),

renamed AS (
    SELECT
        event_key,
        plate_appearance_result,
        contact,
        hit_to_fielder,

    FROM source
)

SELECT * FROM renamed
