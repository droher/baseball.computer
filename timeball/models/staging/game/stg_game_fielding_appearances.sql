WITH source AS (
    SELECT * FROM {{ source('game', 'game_fielding_appearance') }}
),

renamed AS (
    SELECT
        game_id,
        player_id,
        side,
        fielding_position::TINYINT AS fielding_position,
        start_event_id::UTINYINT AS start_event_id,
        end_event_id::UTINYINT AS end_event_id,
    FROM source
)

SELECT * FROM renamed
