WITH source AS (
    SELECT * FROM {{ source('event', 'event_baserunning_play') }}
),

renamed AS (
    SELECT
        event_key,
        sequence_id,
        baserunning_play_type,
        baserunner,

    FROM source
)

SELECT * FROM renamed
