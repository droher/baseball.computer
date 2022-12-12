WITH game_lineup_appearances AS (
    SELECT * FROM {{ source('game', 'game_lineup_appearance') }}
),
events AS (
    SELECT * FROM {{ source('event', 'event') }}
)
SELECT e.game_id,
    e.event_id,
    e.batting_side,
    e.inning,
    e.outs,
    gla.player_id,
    gla.lineup_position,
    e.at_bat = gla.lineup_position AS is_at_bat,
    (gla.lineup_position - e.at_bat + 9) % 9 AS nth_next_batter_up,
FROM game_lineup_appearances gla
JOIN events e 
    ON gla.game_id = e.game_id
    AND e.event_id BETWEEN gla.start_event_id AND gla.end_event_id
    AND e.batting_side = gla.side
