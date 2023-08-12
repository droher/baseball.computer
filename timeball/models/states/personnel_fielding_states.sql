{{
  config(
    materialized = 'table',
    )
}}
-- We create the concept of a personnel state as a range of events
-- where none of the players in the lineup change. This allows us
-- to store personnel info at a lower cardinality than one row per event-player.
WITH ranges AS (
    SELECT DISTINCT
        game_id,
        side,
        start_event_id,
        COALESCE(
            MIN(start_event_id - 1) OVER w,
            end_event_id
        ) AS end_event_id
    FROM {{ ref('stg_game_fielding_appearances') }}
    -- The MIN over this window is the next-largest value
    WINDOW w AS (
        PARTITION BY game_id, side
        ORDER BY start_event_id
        RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
    )
),

final AS (
    SELECT
        appearances.game_id,
        teams.team_id AS fielding_team_id,
        side AS fielding_side,
        (games.game_key + ranges.start_event_id) * CASE WHEN side = 'Home' THEN 1 ELSE -1 END
        AS personnel_fielding_key,
        ranges.start_event_id,
        ranges.end_event_id,
        appearances.player_id,
        appearances.fielding_position,
    FROM {{ ref('stg_game_fielding_appearances') }} AS appearances
    INNER JOIN {{ ref('stg_game_teams') }} AS teams USING (game_id, side)
    INNER JOIN {{ ref('stg_games') }} AS games USING (game_id)
    INNER JOIN ranges
        ON ranges.game_id = appearances.game_id
            AND ranges.side = appearances.side
            AND ranges.start_event_id <= appearances.end_event_id
            AND ranges.end_event_id >= appearances.start_event_id
)

SELECT * FROM final
