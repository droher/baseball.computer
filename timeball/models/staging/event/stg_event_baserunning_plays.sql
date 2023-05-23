WITH source AS (
    SELECT * FROM {{ source('event', 'event_baserunning_play') }}
),

renamed AS (
    SELECT
        event_key::INT AS event_key,
        sequence_id,
        baserunning_play_type,
        baserunner,
        event_key || '-' || sequence_id AS sequence_key,
        event_key || '-' || baserunner AS baserunner_key

    FROM source
)

SELECT * FROM renamed
