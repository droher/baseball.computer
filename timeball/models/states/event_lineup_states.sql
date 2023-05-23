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

final AS (
    SELECT
        e.game_id,
        e.event_id,
        e.event_key,
        t.team_id,
        t.side,
        a.player_id,
        a.lineup_position,
        a.start_event_id = e.event_id AS is_start_of_appearance,
        a.end_event_id = e.event_id AS is_end_of_appearance,
        (e.at_bat = a.lineup_position) AS is_at_bat,
        (a.lineup_position - e.at_bat + 9) % 9 AS nth_next_batter_up,
    FROM appearances AS a
    INNER JOIN teams AS t
        ON a.side = t.side
            AND a.game_id = t.game_id
    INNER JOIN events AS e
        ON a.game_id = e.game_id
            AND e.event_id BETWEEN a.start_event_id AND a.end_event_id
            AND a.side = e.batting_side
)

SELECT * FROM final
