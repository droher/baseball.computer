WITH source AS (
    SELECT * FROM {{ source('event', 'event_pitch_sequences') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        event_key,
        sequence_id,
        sequence_item,
        runners_going_flag,
        blocked_by_catcher_flag,
        catcher_pickoff_attempt_at_base,

    FROM source
)

SELECT * FROM renamed
