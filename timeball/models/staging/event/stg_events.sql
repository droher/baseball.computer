WITH source AS (
    SELECT * FROM {{ source('event', 'event') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        batting_side,
        inning,
        frame,
        at_bat,
        outs,
        count_balls,
        count_strikes,
        event_key

    FROM source
)

SELECT * FROM renamed
