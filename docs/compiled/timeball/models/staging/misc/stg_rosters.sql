WITH source AS (
    SELECT * FROM "timeball"."misc"."roster"
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
    -- TODO: Modify upstream deduper to handle multiple positions on same team
    -- (or talk to retrosheet)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY year, team_id, player_id ORDER BY position) = 1
)

SELECT * FROM renamed