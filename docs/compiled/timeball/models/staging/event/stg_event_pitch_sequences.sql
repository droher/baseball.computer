WITH source AS (
    SELECT * FROM "timeball"."event"."event_pitch_sequences"
),

renamed AS (
    SELECT
        game_id,
        event_id,
        event_key,
        sequence_id,
        sequence_item,
        runners_going_flag,
        blocked_by_catcher_flag,
        catcher_pickoff_attempt_at_base,
        STRPTIME(SUBSTRING(game_id, 4, 8), '%Y%m%d')::DATE AS date,
        SUBSTRING(game_id, 4, 4)::INT2 AS season,

    FROM source
)

SELECT * FROM renamed