WITH counts AS (
    SELECT
        event_key,
        pa.event_key IS NOT NULL AS has_plate_appearance,
        e.count_balls IS NOT NULL AS has_count_balls,
        e.count_strikes IS NOT NULL AS has_count_strikes,
        e.count_balls + e.count_strikes IS NOT NULL AS has_count
    FROM {{ ref('stg_events') }} AS e
    -- We don't use baserunning-only plays as criteria
    -- for determining whether pitch data is missing,
    -- as it is neither necessary nor sufficient for complete data.
    LEFT JOIN {{ ref('stg_event_plate_appearances') }} AS pa USING (event_key)
),

pitch_agg AS (
    SELECT
        ps.event_key,
        BOOL_AND(spt.category != 'Unknown') AS has_pitch_results,
        BOOL_AND(
            spt.category != 'Unknown' AND spt.sequence_item != 'StrikeUnknownType'
        ) AS has_strike_types
    FROM {{ ref('stg_event_pitch_sequences') }} AS ps
    INNER JOIN {{ ref('seed_pitch_types') }} AS spt USING (sequence_item)
    WHERE spt.is_pitch
    GROUP BY 1
),

final AS (
    SELECT
        event_key,
        counts.has_count_balls,
        counts.has_count_strikes,
        counts.has_count,
        COALESCE(pitch_agg.event_key IS NOT NULL) AS has_pitches,
        COALESCE(pitch_agg.has_pitch_results, FALSE) AS has_pitch_results,
        COALESCE(pitch_agg.has_strike_types, FALSE) AS has_strike_types
    FROM counts
    LEFT JOIN pitch_agg USING (event_key)
    WHERE counts.has_plate_appearance
        OR pitch_agg.event_key IS NOT NULL
)

SELECT * FROM final
