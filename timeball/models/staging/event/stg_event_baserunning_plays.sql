WITH source AS (
    SELECT * FROM {{ source('event', 'event_baserunning_play') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        sequence_id,
        baserunning_play_type,
        baserunner,
        event_key,
        event_key || '-' || sequence_id AS sequence_key,
        event_key || '-' || baserunner AS baserunner_key

    FROM source
)

SELECT * FROM renamed
