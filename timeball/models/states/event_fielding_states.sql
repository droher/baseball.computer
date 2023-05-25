WITH teams AS (
    SELECT * FROM {{ ref('stg_game_teams') }}
),

game_fielding_appearances AS (
    SELECT * FROM {{ ref('stg_game_fielding_appearances') }}
),

events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

no_plays AS (
    SELECT * FROM {{ ref('event_no_plays')  }}
),

final AS (
    SELECT
        e.game_id,
        e.event_id,
        e.event_key,
        gfa.side AS fielding_side,
        t.team_id,
        gfa.player_id,
        gfa.fielding_position,
    FROM game_fielding_appearances AS gfa
    INNER JOIN teams AS t
        ON gfa.game_id = t.game_id
            AND gfa.side = t.side
    INNER JOIN events AS e
        ON gfa.game_id = e.game_id
            AND e.event_id BETWEEN gfa.start_event_id AND gfa.end_event_id
            AND e.batting_side != gfa.side
    -- TODO: Investigate dupes on no-plays
    WHERE e.event_key NOT IN (SELECT event_key FROM no_plays)
)

SELECT * FROM final
