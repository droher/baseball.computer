WITH plate_appearances AS (
    -- We don't use baserunning-only plays as criteria
    -- for determining whether pitch data is missing,
    -- as it is neither necessary nor sufficient for complete data.
    SELECT * FROM {{ source('event', 'event_plate_appearance') }}
),

events AS (
    SELECT * FROM {{ source('event', 'event') }}
),

counts AS (
    SELECT
        game_id,
        event_id,
        plate_appearances.game_id IS NOT NULL AS has_plate_appearance,
        e.count_balls IS NOT NULL AS has_count_balls,
        e.count_strikes IS NOT NULL AS has_count_strikes,
        e.count_balls + e.count_strikes IS NOT NULL AS has_count
    FROM events AS e
    LEFT JOIN plate_appearances USING (game_id, event_id)
),

pitch_sequences AS (
    SELECT * FROM {{ source('event', 'event_pitch') }}
),

pitch_agg AS (
    SELECT
        ps.game_id,
        ps.event_id,
        BOOL_OR(pt.is_pitch) AS has_pitches,
        BOOL_AND(pt.category != 'Unknown') AS has_pitch_calls,
        BOOL_AND(pt.category != 'Unknown' AND pt.name != 'StrikeUnknownType') AS has_strike_types
    FROM pitch_sequences AS ps
    INNER JOIN {{ ref('pitch_types') }} AS pt
        ON pt.name = ps.sequence_item
    WHERE pt.is_pitch
    GROUP BY 1, 2
),

final AS (
    SELECT
        game_id,
        event_id,
        counts.has_count_balls,
        counts.has_count_strikes,
        counts.has_count,
        COALESCE(pitch_agg.has_pitches, FALSE) AS has_pitches,
        COALESCE(pitch_agg.has_pitch_calls, FALSE) AS has_pitch_calls,
        COALESCE(pitch_agg.has_strike_types, FALSE) AS has_strike_types
    FROM counts
    LEFT JOIN pitch_agg USING (game_id, event_id)
    WHERE counts.has_plate_appearance
        OR pitch_agg.game_id IS NOT NULL
)

SELECT * FROM final
