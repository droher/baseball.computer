WITH fielding_plays AS (
    SELECT * FROM {{ source('event', 'event_fielding_play') }}
),

fielding_agg AS ()



SELECT * FROM fielding_plays