SELECT * FROM {{ ref('event_raw') }}
WHERE raw_play LIKE '%99%'