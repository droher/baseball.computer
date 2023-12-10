WITH source AS (
    SELECT * FROM {{ source('event', 'event_audit') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        filename,
        line_number,
        event_key,
        raw_play
    FROM source
)

SELECT * FROM renamed
