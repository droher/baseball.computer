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
        state,
        location || ' ' || nickname AS full_name,
        EXTRACT(YEAR FROM date_start) AS season_start,
        CASE
            WHEN date_end IS NULL THEN 9999
            -- There is one case in history of a team switching
            -- names during a season. Retrosheet handles this by
            -- using the team ID at the end of the season
            WHEN team_id = 'LAA' THEN 1964
            ELSE EXTRACT(YEAR FROM date_end)
        END AS season_end
    FROM source
)

SELECT * FROM renamed
