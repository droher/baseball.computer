WITH states_full AS (
    SELECT
        event_key,
        baserunner,
        runner_lineup_position
    FROM {{ ref('stg_event_starting_base_states') }}
    UNION ALL
    SELECT
        event_key,
        'Batter' AS baserunner,
        at_bat AS runner_lineup_position
    FROM {{ ref('stg_events') }}
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
        a.event_key,
        a.baserunner,
        sf.runner_lineup_position,
        a.is_successful,
        a.advanced_on_error_flag,
        a.explicit_out_flag,
        baserunner_meta.numeric_value AS number_base_from,
        bases_meta.numeric_value AS number_base_to,
        part.is_in_play,
        COALESCE(
            rsp.baserunning_play_type, rgp.baserunning_play_type, 'None'
        ) AS baserunning_play_type,
        COALESCE(part.total_bases, 0) AS batter_total_bases
    FROM {{ ref('stg_event_baserunning_advance_attempts') }} AS a
    INNER JOIN states_full AS sf USING (event_key, baserunner)
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
        -- TODO: Get this for batter as well or just use event_id
        -- responsible_pitcher_id,
        (is_successful AND number_base_to = 4)::INT AS runs_scored,
        (baserunning_play_type = 'StolenBase')::INT AS stolen_bases,
        (baserunning_play_type LIKE '%CaughtStealing')::INT AS caught_stealing,
        (baserunning_play_type LIKE 'PickedOff%')::INT AS pickoffs,
        explicit_out_flag::INT AS caught_on_basepaths,

        (baserunning_play_type = 'WildPitch')::INT AS advances_on_wild_pitches,
        (baserunning_play_type = 'PassedBall')::INT AS advances_on_passed_balls,
        (baserunning_play_type = 'Balk')::INT AS advances_on_balks,
        (baserunning_play_type = 'OtherAdvance')::INT AS advances_on_unspecified_plays,
        (
            baserunning_play_type = 'DefensiveIndifference'
        )::INT AS advances_on_defensive_indifference,
        (
            baserunning_play_type = 'AdvancedOnError' OR advanced_on_error_flag
        )::INT AS advances_on_errors,

        is_in_play::INT AS balls_in_play_while_running,

        CASE WHEN is_successful
                THEN number_base_to - number_base_from
            ELSE 0
        END AS bases_advanced,
        CASE WHEN is_successful AND is_in_play AND NOT advanced_on_error_flag
                THEN number_base_to - number_base_from
            ELSE 0
        END AS bases_advanced_on_balls_in_play,
        CASE WHEN is_successful AND NOT advanced_on_error_flag
                THEN number_base_to
                    - number_base_from
                    - LEAST(4 - number_base_from, batter_total_bases)
            ELSE 0
        END AS extra_bases_advanced_on_balls_in_play
    FROM joined
)

SELECT * FROM final
