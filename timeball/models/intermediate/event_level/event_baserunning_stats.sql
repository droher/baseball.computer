{{
  config(
    materialized = 'table',
    )
}}
WITH states_full AS (
    SELECT
        event_key,
        baserunner,
        runner_lineup_position,
        reached_on_event_id
    FROM {{ ref('stg_event_base_states') }}
    WHERE base_state_type = 'Starting'
    UNION ALL
    SELECT
        event_key,
        'Batter' AS baserunner,
        at_bat AS runner_lineup_position,
        NULL AS reached_on_event_id
    FROM {{ ref('stg_events') }}
    WHERE event_key IN (
            SELECT event_key
            FROM {{ ref('stg_event_baserunning_advance_attempts') }}
            WHERE baserunner = 'Batter'
        )
),

-- When baserunner is NULL, it means the play is generic and applies to all
-- baserunners. When it is not NULL, it means the play is specific to that
-- baserunner. This means that the join keys are different for each of these
-- two cases (event_key, baserunner) vs (event_key).
runner_specific_plays AS (
    SELECT *
    FROM {{ ref('stg_event_baserunning_plays') }}
    WHERE baserunner IS NOT NULL
),

runner_generic_plays AS (
    SELECT *
    FROM {{ ref('stg_event_baserunning_plays') }}
    WHERE baserunner IS NULL
),

joined AS (
    SELECT
        event_key,
        baserunner,
        sf.runner_lineup_position,
        sf.reached_on_event_id,
        baserunner != 'Batter' AS is_on_base,
        a.event_key IS NOT NULL as is_advance_attempt,
        part.plate_appearance_result IS NOT NULL AS is_plate_appearance,
        COALESCE(a.is_successful, FALSE) AS is_successful,
        COALESCE(a.advanced_on_error_flag, FALSE) AS advanced_on_error_flag,
        COALESCE(a.explicit_out_flag, FALSE) AS explicit_out_flag,
        baserunner_meta.numeric_value AS number_base_from,
        bases_meta.numeric_value AS number_base_to,
        COALESCE(part.is_in_play, FALSE) AS is_in_play,
        COALESCE(
            rsp.baserunning_play_type, rgp.baserunning_play_type, 'None'
        ) AS baserunning_play_type,
        COALESCE(part.total_bases, 0) AS batter_total_bases
    FROM states_full AS sf
    LEFT JOIN {{ ref('stg_event_baserunning_advance_attempts') }} AS a USING (event_key, baserunner)
    LEFT JOIN runner_specific_plays AS rsp USING (event_key, baserunner)
    LEFT JOIN runner_generic_plays AS rgp USING (event_key)
    LEFT JOIN {{ ref('stg_event_plate_appearances') }} USING (event_key)
    LEFT JOIN {{ ref('seed_plate_appearance_result_types') }} AS part
        USING (plate_appearance_result)
    LEFT JOIN {{ ref('seed_baserunner_info') }} AS baserunner_meta
        ON a.baserunner = baserunner_meta.baserunner
    LEFT JOIN {{ ref('seed_bases_info') }} AS bases_meta
        ON a.attempted_advance_to = bases_meta.base
),

final AS (
    SELECT
        event_key,
        baserunner,
        runner_lineup_position,
        reached_on_event_id,
        (is_successful AND number_base_to = 4)::INT AS runs,
        (baserunning_play_type = 'StolenBase')::INT AS stolen_bases,
        (baserunning_play_type LIKE '%CaughtStealing')::INT AS caught_stealing,
        (baserunning_play_type LIKE 'PickedOff%')::INT AS pickoffs,
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
