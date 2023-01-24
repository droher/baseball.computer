WITH source AS (
    SELECT * FROM {{ source('event', 'event_hit_location') }}
),

renamed AS (
    SELECT
        event_key,
        general_location,
        depth,
        angle,
        strength
    FROM source
)

SELECT * FROM renamed
