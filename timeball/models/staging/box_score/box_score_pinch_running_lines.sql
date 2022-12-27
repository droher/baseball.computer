WITH source AS (
    SELECT * FROM {{ source('box_score', 'box_score_pinch_running_lines') }}
),

renamed AS (
    SELECT
        game_id,
        pinch_runner_id,
        inning,
        side,
        runs,
        stolen_bases,
        caught_stealing

    FROM source
)

SELECT * FROM renamed
