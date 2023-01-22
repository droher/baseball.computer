WITH source AS (
    SELECT * FROM {{ source('event', 'event_hit_location') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        general_location,
        depth,
        angle,
        strength,
        game_id || '-' || event_id AS event_key

    FROM source
)

SELECT * FROM renamed
