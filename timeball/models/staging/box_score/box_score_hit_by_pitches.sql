WITH source AS (
    SELECT * FROM {{ source('box_score', 'box_score_hit_by_pitches') }}
),

renamed AS (
    SELECT
        game_id,
        pitching_side,
        pitcher_id,
        batter_id

    FROM source
)

SELECT * FROM renamed
