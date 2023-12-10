WITH source AS (
    SELECT * FROM {{ source('event', 'event_comments') }}
),

renamed AS (
    SELECT DISTINCT
        event_key,
        comment
    FROM source
    WHERE comment IS NOT NULL
)

SELECT * FROM renamed
