WITH source AS (
    SELECT * FROM {{ source('game', 'game_earned_runs') }}
),

renamed AS (
    SELECT
        game_id,
        player_id,
        earned_runs
    FROM source
)

SELECT * FROM renamed
