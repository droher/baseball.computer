WITH source AS (
    SELECT * FROM {{ source('event', 'event_fielding_play') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        sequence_id,
        fielding_position,
        fielding_play,
        event_key,
        event_key || '-' || sequence_id AS sequence_key

    FROM source
)

SELECT * FROM renamed
