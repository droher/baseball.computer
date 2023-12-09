
WITH lineup AS (
    SELECT
        game_id,
        batting_side,
        personnel_lineup_key::INT AS personnel_lineup_key,
        ANY_VALUE(start_event_id) AS start_event_id,
        ANY_VALUE(end_event_id) AS end_event_id
    FROM "timeball"."main_models"."personnel_lineup_states"
    GROUP BY 1, 2, 3
),

fielding AS (
    SELECT
        game_id,
        fielding_side,
        personnel_fielding_key::INT AS personnel_fielding_key,
        ANY_VALUE(start_event_id) AS start_event_id,
        ANY_VALUE(end_event_id) AS end_event_id
    FROM "timeball"."main_models"."personnel_fielding_states"
    GROUP BY 1, 2, 3
),

final AS (
    SELECT
        events.game_id,
        events.event_id,
        events.event_key,
        lineup.personnel_lineup_key,
        fielding.personnel_fielding_key,
    FROM "timeball"."main_models"."stg_events" AS events
    LEFT JOIN lineup
        ON events.game_id = lineup.game_id
            AND events.batting_side = lineup.batting_side
            AND events.event_id BETWEEN lineup.start_event_id AND lineup.end_event_id
    LEFT JOIN fielding
        ON events.game_id = fielding.game_id
            AND events.batting_side != fielding.fielding_side
            AND events.event_id BETWEEN fielding.start_event_id AND fielding.end_event_id
    WHERE NOT events.no_play_flag
)

SELECT * FROM final