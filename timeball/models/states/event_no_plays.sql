WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

plate_appearances AS (
    SELECT * FROM {{ ref('stg_event_plate_appearances') }}
),

baserunning_plays AS (
    SELECT * FROM {{ ref('stg_event_baserunning_plays') }}
),

fielding_plays AS (
    SELECT * FROM {{ ref('stg_event_fielding_plays')}}
),

final AS (
    SELECT event_key
    FROM events
    WHERE event_key NOT IN (SELECT event_key FROM plate_appearances)
        AND event_key NOT IN (SELECT event_key FROM baserunning_plays)
        AND event_key NOT IN (SELECT event_key FROM fielding_plays)
)

SELECT * FROM final
