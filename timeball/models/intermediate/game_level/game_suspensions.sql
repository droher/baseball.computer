WITH suspensions AS (
    SELECT
        event_key,
        STRING_SPLIT(REPLACE(com, 'Suspend=', ''), ',') AS suspension_info
    FROM {{ ref('stg_event_comments') }},
        UNNEST(STRING_SPLIT(comment, '$')) AS t (com)
    WHERE com ILIKE '%Suspend=%'
)

SELECT
    events.game_id,
    event_key AS event_key_at_suspension,
    STRPTIME(s.suspension_info[1], '%Y%m%d')::DATE AS date_resumed,
    s.suspension_info[2] AS new_park_id,
    s.suspension_info[3]::INT AS away_score_at_suspension,
    s.suspension_info[4]::INT AS home_score_at_suspension,
    s.suspension_info[5]::INT AS game_outs_recorded_at_suspension,
FROM {{ ref('stg_events') }} AS events
INNER JOIN suspensions AS s USING (event_key)
