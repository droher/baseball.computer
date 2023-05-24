WITH source AS (
    SELECT * FROM {{ source('misc', 'roster') }}
),

renamed AS (
    SELECT
        year,
        player_id,
        last_name,
        first_name,
        NULLIF(bats, '?') AS bats,
        NULLIF(throws, '?') AS throws,
        team_id,
        position

    FROM source
)

SELECT * FROM renamed
