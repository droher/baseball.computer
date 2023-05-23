WITH source AS (
    SELECT * FROM {{ source('event', 'event_starting_base_state') }}
),

renamed AS (
    SELECT
        event_key::INT AS event_key,
        baserunner,
        runner_lineup_position::TINYINT AS runner_lineup_position,
        charged_to_pitcher_id,
        event_key || '-' || baserunner AS baserunner_key

    FROM source
)

SELECT * FROM renamed
