{{
  config(
    materialized = 'table',
    )
}}
WITH init_flags AS (
    SELECT
        game_id,
        event_key,
        event_id,
        batting_side,
        pitcher_id,
        batting_team_margin_start,
        inning_in_outs_start,
        runners_count_start,
        pitching_team_starting_pitcher_id = pitcher_id AS starting_pitcher_flag,
        LAG(pitcher_id) OVER game_side AS previous_pitcher_id,
        COALESCE(previous_pitcher_id != pitcher_id, TRUE) AS new_pitcher_flag,
        -- This specifically excludes finishing pitchers
        COALESCE(LEAD(pitcher_id) OVER game_side != pitcher_id, FALSE) AS pitcher_exit_flag,
        LEAD(pitcher_id) OVER game_side IS NULL AS pitcher_finish_flag,

        CASE WHEN new_pitcher_flag
                THEN runners_count_start
            ELSE 0
        END AS inherited_runners,

        CASE WHEN pitcher_exit_flag
                THEN LEAD(runners_count_start) OVER game_side
            ELSE 0
        END AS bequeathed_runners,

        -- A new relief pitcher can enter the game as the first pitcher in rare cases
        new_pitcher_flag
        AND pitching_team_starting_pitcher_id != pitcher_id
        AS new_relief_pitcher_flag,

        new_pitcher_flag
        AND previous_pitcher_id = pitching_team_starting_pitcher_id
        AS starting_pitcher_exit_flag,

        starting_pitcher_exit_flag
        AND inning_in_outs_start < 15
        AS starting_pitcher_early_exit_flag,

        -- Conditions (necessary but not sufficient) for all 3 save situation types:
        -- A new pitcher enters the game with a lead unless they are replacing a starter who
        -- has pitched fewer than 5 innings
        new_relief_pitcher_flag
        AND NOT starting_pitcher_early_exit_flag
        AND batting_team_margin_start < 0
        AS save_situation_base,

        save_situation_base
        AND batting_team_margin_start >= -3
        -- In this situation, the pitcher must pitch a full inning,
        -- which is only possible if he enters before the 9th inning
        -- or at the start of a frame (or if he enters without a lead,
        -- in which case it would not be a save situation)
        AND (inning_in_outs_start <= 24 OR inning_in_outs_start % 3 = 0)
        AS save_situation_1_flag,

        save_situation_base
        AND batting_team_margin_start >= -5
        -- Tying run is on deck, at the plate, or on the bases
        AND batting_team_margin_start + runners_count_start + 2 >= 0
        AS save_situation_2_flag,

        -- There's a bit of an arcane distinction between #1-2 and 3:
        -- The first two are classified as a "save situation" while the third
        -- makes the pitcher eligible for a save if he finishes the game.
        -- The main difference is that a pitcher qualfiies for a hold in the first two,
        -- but not in the third.
        CASE WHEN save_situation_base
                THEN inning_in_outs_start <= 18
        END AS long_save_eligible_start_flag,

        -- This is non-null only on the first event for each new pitcher,
        -- which allows LAG to work properly in the subsequent query
        CASE WHEN new_pitcher_flag
                THEN save_situation_1_flag OR save_situation_2_flag
        END AS save_situation_start_flag,

        CASE WHEN new_pitcher_flag
                THEN save_situation_1_flag OR save_situation_2_flag OR long_save_eligible_start_flag
        END AS save_eligible_start_flag,

        -- These flags only apply if the exiting/finishing pitcher
        -- came in in a save situation
        pitcher_exit_flag
        AND batting_team_margin_end < 0
        AS conditional_hold_flag,

        pitcher_finish_flag
        AND batting_team_margin_end < 0
        AS conditional_save_flag,

        -- A blown save happens as soon as the game is tied, not
        -- when the pitcher leaves
        batting_team_margin_end >= 0 AS conditional_blown_save_flag

    FROM {{ ref('event_states_full') }}
    WINDOW
        game_side AS (
            PARTITION BY game_id, batting_side
            ORDER BY event_id
        )
),

save_flags AS (
    SELECT
        *,
        CASE WHEN LAG(save_situation_start_flag IGNORE NULLS) OVER pitcher_appearance
                THEN conditional_hold_flag
            ELSE FALSE
        END AS hold_flag,
        CASE WHEN LAG(save_eligible_start_flag IGNORE NULLS) OVER pitcher_appearance
                THEN conditional_save_flag
            ELSE FALSE
        END AS save_flag,
        -- TODO: Is it possible to blow a 3-inning save? Baseball Reference and MLB.com appear
        -- to differ - see the Cardinals-Pirates 2022-06-13 game for an example.
        -- We'll say that it is possible because it's funnier.
        CASE WHEN LAG(save_eligible_start_flag IGNORE NULLS) OVER pitcher_appearance
                -- This ensures that only a single event is marked as a blown save,
                -- and that the event is the one on which the save was blown
                AND NOT LAG(conditional_blown_save_flag) OVER pitcher_appearance
                THEN conditional_blown_save_flag
            ELSE FALSE
        END AS blown_save_flag,
        CASE WHEN LAG(long_save_eligible_start_flag IGNORE NULLS) OVER pitcher_appearance
                AND NOT LAG(save_situation_start_flag IGNORE NULLS) OVER pitcher_appearance
                AND NOT LAG(conditional_blown_save_flag) OVER pitcher_appearance
                THEN conditional_blown_save_flag
            ELSE FALSE
        END AS blown_long_save_flag,
    FROM init_flags
    WINDOW
        pitcher_appearance AS (
            PARTITION BY game_id, batting_side, pitcher_id
            ORDER BY event_id
        )
),

final AS (
    SELECT
        game_id,
        event_key,
        event_id,
        previous_pitcher_id,
        pitcher_id,
        starting_pitcher_flag,
        bequeathed_runners::UTINYINT AS bequeathed_runners,
        inherited_runners::UTINYINT AS inherited_runners,
        new_relief_pitcher_flag,
        pitcher_exit_flag,
        pitcher_finish_flag,
        starting_pitcher_exit_flag,
        starting_pitcher_early_exit_flag,
        save_situation_start_flag,
        hold_flag,
        save_flag,
        blown_save_flag,
        blown_long_save_flag,
    FROM save_flags
)

SELECT * FROM final
