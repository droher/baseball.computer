
WITH fielders AS (
    SELECT
        personnel_fielding_key::INT AS personnel_fielding_key,
        ANY_VALUE(CASE WHEN fielding_position = 1 THEN player_id END)::PLAYER_ID AS pitcher_id,
        ANY_VALUE(CASE WHEN fielding_position = 2 THEN player_id END)::PLAYER_ID AS catcher_id,
        ANY_VALUE(CASE WHEN fielding_position = 3 THEN player_id END)::PLAYER_ID AS first_base_id,
        ANY_VALUE(CASE WHEN fielding_position = 4 THEN player_id END)::PLAYER_ID AS second_base_id,
        ANY_VALUE(CASE WHEN fielding_position = 5 THEN player_id END)::PLAYER_ID AS third_base_id,
        ANY_VALUE(CASE WHEN fielding_position = 6 THEN player_id END)::PLAYER_ID AS shortstop_id,
        ANY_VALUE(CASE WHEN fielding_position = 7 THEN player_id END)::PLAYER_ID AS left_field_id,
        ANY_VALUE(CASE WHEN fielding_position = 8 THEN player_id END)::PLAYER_ID AS center_field_id,
        ANY_VALUE(CASE WHEN fielding_position = 9 THEN player_id END)::PLAYER_ID AS right_field_id       
    FROM "timeball"."main_models"."personnel_fielding_states"
    GROUP BY 1
),

final AS (
    SELECT
        epl.game_id,
        epl.event_id,
        epl.event_key,
        fielders.*
    FROM "timeball"."main_models"."event_personnel_lookup" AS epl
    INNER JOIN fielders USING (personnel_fielding_key)
)

SELECT * FROM final