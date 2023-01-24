WITH source AS (
    SELECT * FROM {{ source('event', 'event') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        event_key,
        batting_side,
        inning,
        frame,
        at_bat,
        outs,
        count_balls,
        count_strikes,

    FROM source
)

SELECT * FROM renamed
