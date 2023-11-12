WITH source AS (
    SELECT * FROM {{ source('game', 'game_earned_runs') }}
),

renamed AS (
    -- TODO: Dedupe in raw data
    SELECT DISTINCT
        game_id,
        player_id,
        earned_runs
    FROM source
)

SELECT * FROM renamed
