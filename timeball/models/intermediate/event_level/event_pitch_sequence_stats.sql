{{
  config(
    materialized = 'table',
    )
}}
WITH add_meta AS (
    SELECT
        pitch_meta.*,
        pitches.event_key,
        pitches.runners_going_flag,
        pitches.blocked_by_catcher_flag,
        pitches.catcher_pickoff_attempt_at_base
    FROM {{ ref('stg_event_pitch_sequences') }} AS pitches
    INNER JOIN {{ ref('seed_pitch_types') }} AS pitch_meta USING (sequence_item)
),

other_events AS (
    SELECT
        event_key,
        BOOL_OR(baserunning_play_type = 'PassedBall')::UTINYINT AS passed_balls,
        BOOL_OR(baserunning_play_type = 'WildPitch')::UTINYINT AS wild_pitches,
        BOOL_OR(baserunning_play_type = 'Balk')::UTINYINT AS balks,
    FROM {{ ref('stg_event_baserunners') }}
    GROUP BY 1
),

grouped_sequence AS (
    SELECT
        event_key,
        COUNT(*) FILTER (WHERE is_pitch)::UTINYINT AS pitches,

        COUNT(*) FILTER (WHERE is_swing)::UTINYINT AS swings,
        COUNT(*) FILTER (WHERE is_contact)::UTINYINT AS swings_with_contact,

        COUNT(*) FILTER (WHERE is_strike)::UTINYINT AS strikes,
        COUNT(*) FILTER (WHERE is_strike AND NOT is_swing)::UTINYINT AS strikes_called,
        COUNT(*) FILTER (WHERE is_swing AND NOT is_contact)::UTINYINT AS strikes_swinging,
        COUNT(*) FILTER (
            WHERE is_swing AND is_contact AND NOT is_in_play AND NOT can_be_strike_three
        )::UTINYINT AS strikes_foul,
        COUNT(*) FILTER (WHERE sequence_item LIKE 'FoulTip%')::UTINYINT AS strikes_foul_tip,
        COUNT(*) FILTER (WHERE is_in_play)::UTINYINT AS strikes_in_play,
        COUNT(*) FILTER (WHERE sequence_item = 'StrikeUnknownType')::UTINYINT AS strikes_unknown,

        COUNT(*) FILTER (WHERE category = 'Ball')::UTINYINT AS balls,
        COUNT(*) FILTER (WHERE sequence_item = 'Ball')::UTINYINT AS balls_called,
        COUNT(*) FILTER (WHERE sequence_item = 'IntentionalBall')::UTINYINT AS balls_intentional,
        COUNT(*) FILTER (WHERE sequence_item = 'AutomaticBall')::UTINYINT AS balls_automatic,

        COUNT(*) FILTER (WHERE category = 'Unknown')::UTINYINT AS unknown_pitches,

        COUNT(*) FILTER (WHERE sequence_item LIKE '%Pitchout')::UTINYINT AS pitchouts,
        COUNT(*) FILTER (WHERE sequence_item LIKE 'Pickoff%')::UTINYINT AS pitcher_pickoff_attempts,
        COUNT(*) FILTER (
            WHERE catcher_pickoff_attempt_at_base IS NOT NULL
        )::UTINYINT AS catcher_pickoff_attempts,
        COUNT(*) FILTER (WHERE blocked_by_catcher_flag)::UTINYINT AS pitches_blocked_by_catcher,
        COUNT(*) FILTER (WHERE is_pitch AND runners_going_flag)::UTINYINT AS pitches_with_runners_going,
    FROM add_meta
    GROUP BY 1
),

final AS (
    SELECT
        grouped_sequence.*,
        other_events.passed_balls,
        other_events.wild_pitches,
        other_events.balks,
    FROM grouped_sequence
    LEFT JOIN other_events USING (event_key)
)

SELECT * FROM final
