WITH source AS (
    SELECT * FROM {{ source('event', 'event_comments') }}
),

renamed AS (
    SELECT
        event_key,
        comment
    FROM source
)

SELECT * FROM renamed
