WITH source AS (
    SELECT * FROM {{ source('game', 'game_lineup_appearance') }}
),

renamed AS (
    SELECT
        game_id,
        player_id,
        side,
        lineup_position::TINYINT AS lineup_position,
        entered_game_as,
        start_event_id::UTINYINT AS start_event_id,
        end_event_id::UTINYINT AS end_event_id,

    FROM source
)

SELECT * FROM renamed
