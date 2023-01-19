SELECT * FROM {{ ref('event_raw') }}
WHERE event_key = 'BOS190310010-3'
