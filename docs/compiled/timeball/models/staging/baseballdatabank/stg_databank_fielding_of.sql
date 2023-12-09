WITH source AS (
    SELECT * FROM "timeball"."baseballdatabank"."fielding_of"
),

renamed AS (
    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        stint,
        g_lf AS games_left_field,
        g_cf AS games_center_field,
        g_rf AS games_right_field,
    FROM source
)

SELECT * FROM renamed