WITH source AS (
    SELECT * FROM {{ source('biodata', 'teams') }}
),

renamed AS (
    SELECT
        team AS team_id,
        league,
        city,
        nickname,
        first_year,
        last_year

    FROM source
)

SELECT * FROM renamed
