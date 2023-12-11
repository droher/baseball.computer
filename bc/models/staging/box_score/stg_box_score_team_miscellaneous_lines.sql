WITH source AS (
    SELECT * FROM {{ source('box_score', 'box_score_team_miscellaneous_lines') }}
),

renamed AS (
    SELECT
        game_id,
        side,
        left_on_base,
        team_earned_runs,
        double_plays_turned,
        triple_plays_turned

    FROM source
)

SELECT * FROM renamed
