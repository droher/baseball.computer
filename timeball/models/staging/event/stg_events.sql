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
        LEFT(specified_batter_hand, 1) AS specified_batter_hand,
        LEFT(specified_pitcher_hand, 1) AS specified_pitcher_hand,
        strikeout_responsible_batter AS strikeout_responsible_batter_id,
        walk_responsible_pitcher AS walk_responsible_pitcher_id,
    FROM source
)

SELECT * FROM renamed
