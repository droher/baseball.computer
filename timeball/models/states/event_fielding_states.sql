SELECT
    e.game_id,
    e.event_id,
    gfa.side AS fielding_side,
    gfa.player_id,
    gfa.fielding_position
FROM {{ ref('game_fielding_appearances') }} AS gfa
INNER JOIN {{ ref('events') }} AS e
    ON gfa.game_id = e.game_id
        AND e.event_id BETWEEN gfa.start_event_id AND gfa.end_event_id
        AND e.batting_side != gfa.side
