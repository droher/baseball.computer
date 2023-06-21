WITH source_event AS (
    SELECT
        event_key,
        REGEXP_EXTRACT(comment, 'Forfeit=(.*)', 1) AS forfeit_info
    FROM {{ ref('stg_event_comments') }}
    WHERE comment ILIKE '%Forfeit=%'
),

event_joined AS (
    SELECT
        events.game_id,
        event_key AS event_key_at_forfeit,
        source_event.forfeit_info
    FROM {{ ref('stg_events') }} AS events
    INNER JOIN source_event USING (event_key)
),

source_box AS (
    SELECT
        game_id,
        REGEXP_EXTRACT(comment, 'Forfeit=(.*)', 1) AS forfeit_info
    FROM {{ ref('stg_box_score_comments') }}
    WHERE comment ILIKE '%Forfeit=%'
        AND game_id NOT IN (SELECT game_id FROM event_joined)
),

source_gamelog AS (
    SELECT
        game_id,
        forfeit_info
    FROM {{ ref('stg_gamelog') }}
    WHERE forfeit_info IS NOT NULL
        AND game_id NOT IN (SELECT game_id FROM event_joined)
        AND game_id NOT IN (SELECT game_id FROM source_box)
)

unioned AS (
    SELECT
        game_id,
        event_key_at_forfeit,
        forfeit_info
    FROM event_joined
    UNION ALL
    SELECT
        game_id,
        NULL AS event_key_at_forfeit,
        forfeit_info
    FROM source_box
    UNION ALL
    SELECT
        game_id,
        NULL AS event_key_at_forfeit,
        forfeit_info
    FROM source_gamelog
)

SELECT
    game_id,
    event_key_at_forfeit,
    CASE forfeit_info
        WHEN 'H' THEN 'Home'
        WHEN 'V' THEN 'Away'
        WHEN 'T' THEN 'Tie'
    END AS winning_side,
    CASE WHEN game_id = 'CLE197406040' THEN .10 END AS price_of_beer
FROM unioned
