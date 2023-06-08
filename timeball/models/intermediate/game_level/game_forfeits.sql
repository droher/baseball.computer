WITH forfeits AS (
    SELECT
        event_key,
        REGEXP_EXTRACT(com, 'Forfeit=(.*)', 1) AS forfeit_info
    FROM {{ ref('stg_event_comments') }}, UNNEST(STRING_SPLIT(comment, '$')) AS t (com)
    WHERE com ILIKE '%Forfeit=%'
)


SELECT
    events.game_id,
    event_key AS event_key_at_forfeit,
    CASE forfeits.forfeit_info
        WHEN 'H' THEN 'Home'
        WHEN 'V' THEN 'Away'
        WHEN 'T' THEN 'Tie'
    END AS winning_side,
    CASE WHEN events.game_id = 'CLE197406040' THEN .10 END AS price_of_beer
FROM {{ ref('stg_events') }} AS events
INNER JOIN forfeits USING (event_key)
