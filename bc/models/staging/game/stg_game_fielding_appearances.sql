WITH source AS (
    SELECT * FROM {{ source('game', 'game_fielding_appearances') }}
),

renamed AS (
    SELECT
        game_id,
        player_id,
        side,
        fielding_position,
        start_event_id,
        end_event_id,
    FROM source
)

SELECT * FROM renamed
