WITH source AS (
    SELECT * FROM {{ source('event', 'event_run') }}
),

renamed AS (
    SELECT
        event_key,
        runner,
        rbi_flag,
        explicit_unearned_run_status,
    FROM source
)

SELECT * FROM renamed
