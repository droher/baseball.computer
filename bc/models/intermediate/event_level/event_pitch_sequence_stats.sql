MODEL (
  name main_models.event_pitch_sequence_stats,
  kind FULL,
  grain (event_key),
  columns (
    event_key UINTEGER,
    pitches UTINYINT,
    swings UTINYINT,
    swings_with_contact UTINYINT,
    strikes UTINYINT,
    strikes_called UTINYINT,
    strikes_swinging UTINYINT,
    strikes_foul UTINYINT,
    strikes_foul_tip UTINYINT,
    strikes_in_play UTINYINT,
    strikes_unknown UTINYINT,
    balls UTINYINT,
    balls_called UTINYINT,
    balls_intentional UTINYINT,
    balls_automatic UTINYINT,
    unknown_pitches UTINYINT,
    pitchouts UTINYINT,
    pitcher_pickoff_attempts UTINYINT,
    catcher_pickoff_attempts UTINYINT,
    pitches_blocked_by_catcher UTINYINT,
    pitches_with_runners_going UTINYINT,
    passed_balls UTINYINT,
    wild_pitches UTINYINT,
    balks UTINYINT
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    pitches = @doc('pitches'),
    swings = @doc('swings'),
    swings_with_contact = @doc('swings_with_contact'),
    strikes = @doc('strikes'),
    strikes_called = @doc('strikes_called'),
    strikes_swinging = @doc('strikes_swinging'),
    strikes_foul = @doc('strikes_foul'),
    strikes_foul_tip = @doc('strikes_foul_tip'),
    strikes_in_play = @doc('strikes_in_play'),
    strikes_unknown = @doc('strikes_unknown'),
    balls = @doc('balls'),
    balls_called = @doc('balls_called'),
    balls_intentional = @doc('balls_intentional'),
    balls_automatic = @doc('balls_automatic'),
    unknown_pitches = @doc('unknown_pitches'),
    pitchouts = @doc('pitchouts'),
    pitcher_pickoff_attempts = @doc('pitcher_pickoff_attempts'),
    catcher_pickoff_attempts = @doc('catcher_pickoff_attempts'),
    pitches_blocked_by_catcher = @doc('pitches_blocked_by_catcher'),
    pitches_with_runners_going = @doc('pitches_with_runners_going'),
    passed_balls = @doc('passed_balls'),
    wild_pitches = @doc('wild_pitches'),
    balks = @doc('balks')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_pitch_sequence_stats.parquet'
  ),
);







WITH add_meta AS (
    SELECT
        pitch_meta.*,
        pitches.event_key,
        pitches.runners_going_flag,
        pitches.blocked_by_catcher_flag,
        pitches.catcher_pickoff_attempt_at_base
    FROM main_models.stg_event_pitch_sequences AS pitches
    INNER JOIN main_seeds.seed_pitch_types AS pitch_meta USING (sequence_item)
),

other_events AS (
    SELECT
        event_key,
        BOOL_OR(baserunning_play_type = 'PassedBall')::UTINYINT AS passed_balls,
        BOOL_OR(baserunning_play_type = 'WildPitch')::UTINYINT AS wild_pitches,
        BOOL_OR(baserunning_play_type = 'Balk')::UTINYINT AS balks,
    FROM main_models.stg_event_baserunners
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
        COALESCE(other_events.passed_balls, 0)::UTINYINT AS passed_balls,
        COALESCE(other_events.wild_pitches, 0)::UTINYINT AS wild_pitches,
        COALESCE(other_events.balks, 0)::UTINYINT AS balks
    FROM grouped_sequence
    LEFT JOIN other_events USING (event_key)
)

SELECT * FROM final
