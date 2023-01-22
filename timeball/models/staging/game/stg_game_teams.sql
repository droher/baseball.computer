WITH source AS (
    SELECT * FROM {{ source('game', 'game_team') }}
),

renamed AS (
    SELECT
        game_id,
        team_id,
        side

    FROM source
)

SELECT * FROM renamed
