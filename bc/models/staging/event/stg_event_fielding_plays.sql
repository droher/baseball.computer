WITH source AS (
    SELECT * FROM {{ source('event', 'event_fielding_play') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        event_key,
        sequence_id,
        fielding_position,
        fielding_play,

    FROM source
)

SELECT * FROM renamed
