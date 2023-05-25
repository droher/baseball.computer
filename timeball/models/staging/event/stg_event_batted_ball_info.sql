WITH source AS (
    SELECT * FROM {{ source('event', 'event_batted_ball_info') }}
),

renamed AS (
    SELECT
        event_key,
        contact,
        hit_to_fielder,
        general_location,
        depth,
        angle,
        strength
    FROM source
)

SELECT * FROM renamed
