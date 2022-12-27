WITH source AS (
    SELECT * FROM {{ source('event', 'event_pitch') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        sequence_id,
        sequence_item,
        runners_going_flag,
        blocked_by_catcher_flag,
        catcher_pickoff_attempt_at_base,
        game_id || '-' || event_id AS event_key,
        game_id || '-' || event_id || '-' || sequence_id AS sequence_key

    FROM source
)

SELECT * FROM renamed
