WITH plate_appearances AS (
    -- We don't use baserunning-only plays as criteria
    -- for determining whether pitch data is missing,
    -- as it is neither necessary nor sufficient for complete data.
    SELECT * FROM {{ ref('stg_event_plate_appearances') }}
),

events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

pitch_meta AS (
    SELECT * FROM {{ ref('pitch_types') }}
),

counts AS (
    SELECT
        e.game_id,
        e.event_id,
        event_key,
        plate_appearances.game_id IS NOT NULL AS has_plate_appearance,
        e.count_balls IS NOT NULL AS has_count_balls,
        e.count_strikes IS NOT NULL AS has_count_strikes,
        e.count_balls + e.count_strikes IS NOT NULL AS has_count
    FROM events AS e
    LEFT JOIN plate_appearances USING (event_key)
),

pitch_sequences AS (
    SELECT * FROM {{ ref('stg_event_pitch_sequences') }}
),

pitch_agg AS (
    SELECT
        ps.event_key,
        BOOL_OR(pm.is_pitch) AS has_pitches,
        BOOL_AND(pm.category != 'Unknown') AS has_pitch_calls,
        BOOL_AND(pm.category != 'Unknown' AND pm.sequence_item != 'StrikeUnknownType') AS has_strike_types
    FROM pitch_sequences AS ps
    INNER JOIN pitch_meta AS pm USING (sequence_item)
    WHERE pm.is_pitch
    GROUP BY 1
),

final AS (
    SELECT
        event_key,
        counts.game_id,
        counts.event_id,
        counts.has_count_balls,
        counts.has_count_strikes,
        counts.has_count,
        COALESCE(pitch_agg.has_pitches, FALSE) AS has_pitches,
        COALESCE(pitch_agg.has_pitch_calls, FALSE) AS has_pitch_calls,
        COALESCE(pitch_agg.has_strike_types, FALSE) AS has_strike_types
    FROM counts
    LEFT JOIN pitch_agg USING (event_key)
    WHERE counts.has_plate_appearance
        OR pitch_agg.event_key IS NOT NULL
)

SELECT * FROM final
