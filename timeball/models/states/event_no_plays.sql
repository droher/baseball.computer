WITH final AS (
    SELECT event_key
    FROM {{ ref('stg_events') }}
    WHERE event_key NOT IN (SELECT event_key FROM {{ ref('stg_event_plate_appearances') }})
        AND event_key NOT IN (SELECT event_key FROM {{ ref('stg_event_baserunning_plays') }})
        AND event_key NOT IN (SELECT event_key FROM {{ ref('stg_event_fielding_plays')}})
)

SELECT * FROM final
