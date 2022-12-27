WITH source AS (
    SELECT * FROM {{ source('misc', 'franchise') }}
),

renamed AS (
    SELECT
        current_franchise_id,
        team_id,
        league,
        division,
        location,
        nickname,
        alternate_nicknames,
        date_start,
        date_end,
        city,
        state

    FROM source
)

SELECT * FROM renamed
