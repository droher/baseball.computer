WITH fielding_plays AS (
    SELECT * FROM {{ ref('event_fielding_plays') }}
)
-- TODO

SELECT * FROM fielding_plays
