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
        game_id || '-' || event_id AS event_key,
        game_id || '-' || event_id || '-' || sequence_id AS sequence_key,
        game_id || '-' || event_id || '-' || baserunner AS baserunner_key

    FROM source
)

SELECT * FROM renamed
