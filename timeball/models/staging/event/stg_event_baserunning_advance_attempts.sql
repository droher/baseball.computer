WITH source AS (
    SELECT * FROM {{ source('event', 'event_baserunning_advance_attempt') }}
),

renamed AS (
    SELECT
        event_key,
        sequence_id,
        baserunner,
        attempted_advance_to,
        is_successful,
        advanced_on_error_flag,
        explicit_out_flag,

    FROM source
)

SELECT * FROM renamed
