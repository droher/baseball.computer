WITH source AS (
    SELECT * FROM {{ source('event', 'event_raw') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        filename,
        line_number,
        raw_play,
        event_key
    FROM source
)

SELECT * FROM renamed
