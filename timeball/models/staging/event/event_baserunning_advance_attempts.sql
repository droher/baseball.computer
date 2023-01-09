WITH source AS (
    SELECT * FROM {{ source('event', 'event_baserunning_advance_attempt') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        sequence_id,
        baserunner,
        attempted_advance_to,
        is_successful,
        advanced_on_error_flag,
        safe_on_error_flag,
        rbi_flag,
        team_unearned_flag,
        game_id || '-' || event_id AS event_key,
        game_id || '-' || event_id || '-' || sequence_id AS sequence_key,
        game_id || '-' || event_id || '-' || baserunner AS baserunner_key

    FROM source
)

SELECT * FROM renamed
