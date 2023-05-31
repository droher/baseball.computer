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

final AS (
    SELECT
        event_key,
        COUNT(*) FILTER (WHERE is_pitch) AS pitches_thrown,

        COUNT(*) FILTER (WHERE is_swing) AS swings,
        COUNT(*) FILTER (WHERE is_contact) AS swings_with_contact,

        COUNT(*) FILTER (WHERE is_strike) AS strikes_thrown,
        COUNT(*) FILTER (WHERE is_swing AND NOT is_contact) AS strikes_swinging,
        COUNT(*) FILTER (
            WHERE is_swing AND is_contact AND NOT is_in_play AND NOT can_be_strike_three
        ) AS strikes_foul,
        COUNT(*) FILTER (WHERE sequence_item LIKE 'FoulTip%') AS strikes_foul_tip,
        COUNT(*) FILTER (WHERE is_in_play) AS strikes_in_play,
        COUNT(*) FILTER (WHERE sequence_item = 'StrikeUnknownType') AS strikes_unknown,

        COUNT(*) FILTER (WHERE is_pitch AND NOT is_strike) AS balls_thrown,
        COUNT(*) FILTER (WHERE sequence_item = 'Ball') AS balls_called,
        COUNT(*) FILTER (WHERE sequence_item = 'IntentionalBall') AS balls_intentional,
        COUNT(*) FILTER (WHERE sequence_item = 'AutomaticBall') AS balls_automatic,

        COUNT(*) FILTER (WHERE category = 'Unknown') AS unknown_pitches_thrown,

        COUNT(*) FILTER (WHERE sequence_item LIKE '%Pitchout') AS pitchouts,
        COUNT(*) FILTER (WHERE sequence_item LIKE 'Pickoff%') AS pitcher_pickoff_attempts,
        COUNT(*) FILTER (
            WHERE catcher_pickoff_attempt_at_base IS NOT NULL
        ) AS catcher_pickoff_attempts,
        COUNT(*) FILTER (WHERE blocked_by_catcher_flag) AS pitches_blocked_by_catcher,
        COUNT(*) FILTER (WHERE is_pitch AND runners_going_flag) AS pitches_with_runners_going,

    FROM add_meta
    GROUP BY 1
)

SELECT * FROM final
