
-- We create the concept of a personnel state as a range of events
-- where none of the players in the lineup change. This allows us
-- to store personnel info at a lower cardinality than one row per event-player.
WITH ranges AS (
    SELECT DISTINCT
        game_id,
        side,
        start_event_id,
        COALESCE(
            MIN(start_event_id - 1) OVER next_largest,
            -- This can almos always just be the end_event_id of the same line,
            -- except for the fun edge case where the final substitution
            -- of the game is a move that moves the DH into the field
            MAX(end_event_id) OVER full_game
        )::UINTEGER AS end_event_id
    FROM "timeball"."main_models"."stg_game_lineup_appearances"
    -- The MIN over this window is the next-largest value
    WINDOW
        next_largest AS (
            PARTITION BY game_id, side
            ORDER BY start_event_id
            RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
        ),
        full_game AS (
            PARTITION BY game_id, side
        )
),

final AS (
    SELECT
        appearances.game_id,
        CASE WHEN ranges.side = 'Home' THEN games.home_team_id ELSE games.away_team_id END AS batting_team_id,
        appearances.side AS batting_side,
        (games.game_key + ranges.start_event_id) * CASE WHEN appearances.side = 'Home' THEN 1 ELSE -1 END::INT
        AS personnel_lineup_key,
        ranges.start_event_id,
        ranges.end_event_id,
        appearances.player_id,
        appearances.lineup_position,
    FROM "timeball"."main_models"."stg_game_lineup_appearances" AS appearances
    INNER JOIN "timeball"."main_models"."stg_games" AS games USING (game_id)
    INNER JOIN ranges
        ON ranges.game_id = appearances.game_id
            AND ranges.side = appearances.side
            AND ranges.start_event_id <= appearances.end_event_id
            AND ranges.end_event_id >= appearances.start_event_id
)

SELECT * FROM final