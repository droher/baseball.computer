WITH source AS (
    SELECT * FROM {{ source('game', 'game_lineup_appearances') }}
),

renamed AS (
    SELECT
        game_id,
        player_id,
        side,
        lineup_position,
        entered_game_as,
        start_event_id,
        end_event_id,

    FROM source
)

SELECT * FROM renamed
