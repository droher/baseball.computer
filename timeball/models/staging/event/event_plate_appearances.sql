WITH source AS (
    SELECT * FROM {{ source('event', 'event_plate_appearance') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        plate_appearance_result,
        contact,
        hit_to_fielder,
        game_id || '-' || event_id AS event_key

    FROM source
)

SELECT * FROM renamed
