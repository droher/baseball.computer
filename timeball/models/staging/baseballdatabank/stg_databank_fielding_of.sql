WITH source AS (
    SELECT * FROM {{ source('baseballdatabank', 'fieldingof') }}
),

renamed AS (
    SELECT
        playerid AS databank_player_id,
        yearid AS season,
        stint,
        glf AS games_left_field,
        gcf AS games_center_field,
        grf AS games_right_field,
    FROM source
)

SELECT * FROM renamed
