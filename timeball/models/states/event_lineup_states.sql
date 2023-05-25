{{
  config(
    materialized = 'table',
    )
}}
WITH appearances AS (
    SELECT * FROM {{ ref('stg_game_lineup_appearances') }}
),

teams AS (
    SELECT * FROM {{ ref('stg_game_teams') }}
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
        e.event_key,
        t.team_id,
        t.side,
        a.player_id,
        a.lineup_position,
        (e.at_bat = a.lineup_position) AS is_at_bat,
        -- Cast to avoid overflow from unsigned int
        (a.lineup_position::INT - e.at_bat::INT + 9) % 9 AS nth_next_batter_up,
    FROM appearances AS a
    INNER JOIN teams AS t
        ON a.side = t.side
            AND a.game_id = t.game_id
    INNER JOIN events AS e
        ON a.game_id = e.game_id
            AND e.event_id BETWEEN a.start_event_id AND a.end_event_id
            AND a.side = e.batting_side
    WHERE e.event_key NOT IN (SELECT event_key FROM no_plays)
)

SELECT * FROM final
