{{
  config(
    materialized = 'table',
    )
}}
WITH joined AS (
    SELECT
        event_key,
        b.baserunner,
        b.game_id,
        e.batting_team_id,
        e.fielding_team_id,
        e.base_state,
        b.runner_id,
        e.pitcher_id AS current_pitcher_id,
        b.runner_lineup_position,
        b.reached_on_event_key,
        b.charge_event_key,
        b.explicit_charged_pitcher_id,
        b.baserunner != 'Batter' AS is_on_base,
        b.attempted_advance_to_base IS NOT NULL AS is_advance_attempt,
        part.plate_appearance_result IS NOT NULL AS is_plate_appearance,
        b.is_out,
        b.is_advance_attempt AND NOT b.is_out AS is_successful,
        COALESCE(b.advanced_on_error_flag, FALSE) AS advanced_on_error_flag,
        COALESCE(b.explicit_out_flag, FALSE) AS explicit_out_flag,
        baserunner_meta.numeric_value AS number_base_from,
        bases_meta.numeric_value AS number_base_to,
        COALESCE(part.is_in_play, FALSE) AS is_in_play,
        COALESCE(b.baserunning_play_type, 'None') AS baserunning_play_type,
        COALESCE(part.total_bases, 0) AS batter_total_bases,
        CASE WHEN b.baserunner = 'Batter'
                THEN e.base_state & 1 = 0 
            WHEN b.baserunner = 'First'
                THEN e.base_state >> 1 & 1 = 0
            WHEN b.baserunner = 'Second'
                THEN e.base_state >> 2 & 1 = 0
            ELSE FALSE
        END AS is_next_base_empty,
        CASE WHEN b.baserunner = 'Batter'
                -- By convention we say that the batter is never the lead runner
                THEN FALSE
            WHEN b.baserunner = 'First'
                THEN e.base_state = 1
            WHEN b.baserunner = 'Second'
                THEN e.base_state < 4
            ELSE TRUE
        END AS is_lead_runner,
        CASE WHEN b.baserunner = 'Second'
                THEN e.base_state & 1 = 1
            WHEN b.baserunner = 'Third'
                THEN e.base_state = 7
            ELSE TRUE
        END AS is_force_on_runner,
    FROM {{ ref('stg_event_baserunners') }} b
    LEFT JOIN {{ ref('stg_events') }} e USING (event_key)
    LEFT JOIN {{ ref('seed_plate_appearance_result_types') }} AS part
        USING (plate_appearance_result)
    LEFT JOIN {{ ref('seed_baserunner_info') }} AS baserunner_meta
        ON b.baserunner = baserunner_meta.baserunner
    LEFT JOIN {{ ref('seed_bases_info') }} AS bases_meta
        ON b.attempted_advance_to_base = bases_meta.base
),

final AS (
    SELECT
        event_key,
        game_id,
        batting_team_id,
        fielding_team_id,
        runner_id,
        current_pitcher_id,
        baserunner,
        runner_lineup_position,
        reached_on_event_key,
        charge_event_key,
        explicit_charged_pitcher_id,
        (is_successful AND number_base_to = 4)::INT AS runs,
        -- Note that this is different from OBP - it includes fielders choices, errors, etc.
        (is_successful AND baserunner = 'Batter')::INT AS times_reached_base,
        (is_lead_runner)::INT AS times_lead_runner,
        (is_force_on_runner)::INT AS times_force_on_runner,
        (is_next_base_empty)::INT AS times_next_base_empty,
        (is_next_base_empty AND is_on_base)::INT AS stolen_base_opportunities,
        (is_next_base_empty AND baserunner = 'First')::INT AS stolen_base_opportunities_second,
        (is_next_base_empty AND baserunner = 'Second')::INT AS stolen_base_opportunities_third,
        (is_next_base_empty AND baserunner = 'Third')::INT AS stolen_base_opportunities_home,
        (baserunning_play_type = 'StolenBase')::INT AS stolen_bases,
        (baserunning_play_type = 'StolenBase' AND baserunner = 'First')::INT AS stolen_bases_second,
        (baserunning_play_type = 'StolenBase' AND baserunner = 'Second')::INT AS stolen_bases_third,
        (baserunning_play_type = 'StolenBase' AND baserunner = 'Third')::INT AS stolen_bases_home,
        (baserunning_play_type LIKE '%CaughtStealing')::INT AS caught_stealing,
        (baserunning_play_type LIKE '%CaughtStealing' AND baserunner = 'First')::INT AS caught_stealing_second,
        (baserunning_play_type LIKE '%CaughtStealing' AND baserunner = 'Second')::INT AS caught_stealing_third,
        (baserunning_play_type LIKE '%CaughtStealing' AND baserunner = 'Third')::INT AS caught_stealing_home,
        (baserunning_play_type LIKE 'PickedOff%')::INT AS picked_off,
        (baserunning_play_type = 'PickedOff' AND baserunner = 'First')::INT AS picked_off_first,
        (baserunning_play_type = 'PickedOff' AND baserunner = 'Second')::INT AS picked_off_second,
        (baserunning_play_type = 'PickedOff' AND baserunner = 'Third')::INT AS picked_off_third,
        (baserunning_play_type = 'PickedOffCaughtStealing')::INT AS picked_off_caught_stealing,
        explicit_out_flag::INT AS outs_on_basepaths,

        (baserunning_play_type = 'WildPitch' AND is_successful)::INT AS advances_on_wild_pitches,
        (baserunning_play_type = 'PassedBall' AND is_successful)::INT AS advances_on_passed_balls,
        (baserunning_play_type = 'Balk' AND is_successful)::INT AS advances_on_balks,
        (
            baserunning_play_type = 'OtherAdvance' AND is_successful
        )::INT AS advances_on_unspecified_plays,
        (
            baserunning_play_type = 'DefensiveIndifference' AND is_successful
        )::INT AS advances_on_defensive_indifference,
        (
            (baserunning_play_type = 'AdvancedOnError' OR advanced_on_error_flag) AND is_successful
        )::INT AS advances_on_errors,

        (is_plate_appearance AND is_on_base)::INT AS plate_appearances_while_on_base,
        (is_in_play)::INT AS balls_in_play_while_running,
        (is_in_play AND is_on_base)::INT AS balls_in_play_while_on_base,
        batter_total_bases AS batter_total_bases_while_running,
        CASE WHEN is_on_base
                THEN batter_total_bases
            ELSE 0
        END AS batter_total_bases_while_on_base,
        CASE WHEN is_in_play AND number_base_to - number_base_from > batter_total_bases
                THEN 1
            ELSE 0
        END AS extra_base_advance_attempts,
        CASE WHEN is_successful
                THEN number_base_to - number_base_from
            ELSE 0
        END AS bases_advanced,
        CASE WHEN is_successful AND is_in_play AND NOT advanced_on_error_flag
                THEN number_base_to - number_base_from
            ELSE 0
        END AS bases_advanced_on_balls_in_play,
        CASE WHEN is_successful AND is_in_play AND NOT advanced_on_error_flag
                THEN number_base_to
                    - number_base_from
                    - LEAST(4 - number_base_from, batter_total_bases)
            ELSE 0
        END AS surplus_bases_advanced_on_balls_in_play,
        CASE WHEN explicit_out_flag AND number_base_to - number_base_from > 1
                THEN 1
            ELSE 0
        END AS outs_on_extra_base_advance_attempts,

    FROM joined
)

SELECT * FROM final
