WITH source_event AS (
    SELECT
        event_key,
        STRING_SPLIT(REPLACE(comment, 'Suspend=', ''), ',') AS suspension_info
    FROM {{ ref('stg_event_comments') }}
    WHERE comment ILIKE '%Suspend=%'
),

event_joined AS (
    SELECT
        events.game_id,
        event_key AS event_key_at_suspension,
        source_event.suspension_info
    FROM {{ ref('stg_events') }} AS events
    INNER JOIN source_event USING (event_key)
),

source_box AS (
    SELECT
        game_id,
        STRING_SPLIT(REPLACE(comment, 'Suspend=', ''), ',') AS suspension_info
    FROM {{ ref('stg_box_score_comments') }}
    WHERE comment ILIKE '%Suspend=%'
        AND game_id NOT IN (SELECT game_id FROM event_joined)
),

unioned AS (
    SELECT
        game_id,
        event_key_at_suspension,
        suspension_info
    FROM event_joined
    UNION ALL
    SELECT
        game_id,
        NULL AS event_key_at_suspension,
        suspension_info
    FROM source_box
)



SELECT
    game_id,
    event_key_at_suspension,
    STRPTIME(suspension_info[1], '%Y%m%d')::DATE AS date_resumed,
    suspension_info[2] AS new_park_id,
    suspension_info[3]::INT AS away_score_at_suspension,
    suspension_info[4]::INT AS home_score_at_suspension,
    suspension_info[5]::INT AS game_outs_recorded_at_suspension,
FROM unioned
