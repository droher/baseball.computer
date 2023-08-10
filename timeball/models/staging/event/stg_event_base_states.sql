WITH source AS (
    SELECT * FROM {{ source('event', 'event_base_state') }}
),

renamed AS (
    SELECT
        event_key,
        base_state_type,
        baserunner,
        runner_lineup_position,
        reached_on_event_id,
        charge_event_id,
        explicit_charged_pitcher_id,
        {{ event_id_to_key("reached_on_event_id", "event_key") }} AS reached_on_event_key,
        {{ event_id_to_key("charge_event_id", "event_key") }} AS charge_event_key,
        -- Bitwise agg of this gives us the full base_state
        CASE baserunner
            WHEN 'First' THEN 1
            WHEN 'Second' THEN 2
            WHEN 'Third' THEN 4
        END AS baserunner_bit,

    FROM source
)

SELECT * FROM renamed
