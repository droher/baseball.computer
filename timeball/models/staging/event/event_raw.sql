WITH source AS (
    SELECT * FROM {{ source('event', 'event_plate_appearance') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        filename,
        line_number,
        raw_play,
        game_id || '-' || event_id AS event_key
    FROM source
)

SELECT * FROM renamed
