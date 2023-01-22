WITH source AS (
    SELECT * FROM {{ source('event', 'event_out') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        sequence_id,
        baserunner_out,
        game_id || '-' || event_id AS event_key,
        game_id || '-' || event_id || '-' || sequence_id AS sequence_key

    FROM source
)

SELECT * FROM renamed
