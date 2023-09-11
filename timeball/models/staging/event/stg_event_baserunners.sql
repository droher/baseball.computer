WITH source AS (
    SELECT * FROM {{ source('event', 'event_baserunners') }}
),
renamed AS (
    SELECT
        game_id,
        event_id,
        event_key,
        baserunner,
        runner_lineup_position,
        runner_id,
        charge_event_id,
        reached_on_event_id,
        explicit_charged_pitcher_id,
        attempted_advance_to_base,
        baserunning_play_type,
        is_out,
        base_end,
        advanced_on_error_flag,
        explicit_out_flag,
        run_scored_flag,
        rbi_flag,
        {{ event_id_to_key("reached_on_event_id", "event_key") }} AS reached_on_event_key,
        {{ event_id_to_key("charge_event_id", "event_key") }} AS charge_event_key,
        -- Bitwise agg of this gives us the full base_state
        CASE baserunner
            WHEN 'First' THEN 1
            WHEN 'Second' THEN 2
            WHEN 'Third' THEN 4
        END AS baserunner_bit,
        attempted_advance_to_base IS NOT NULL AS is_advance_attempt,

    FROM source
)

SELECT * FROM renamed
