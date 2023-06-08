WITH source AS (
    SELECT * FROM {{ source('event', 'event_comment') }}
),

renamed AS (
    SELECT
        event_key,
        comment
    FROM source
)

SELECT * FROM renamed
