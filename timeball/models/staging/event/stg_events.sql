WITH source AS (
    SELECT * FROM {{ source('event', 'event') }}
),

renamed AS (
    SELECT
        game_id,
        event_id::UTINYINT AS event_id,
        event_key::INT AS event_key,
        batting_side,
        inning::TINYINT AS inning,
        frame,
        at_bat::TINYINT AS at_bat,
        outs::TINYINT AS outs,
        count_balls::TINYINT AS count_balls,
        count_strikes::TINYINT AS count_strikes,

    FROM source
)

SELECT * FROM renamed
