WITH batted_balls AS (
    SELECT
        events.game_id,
        BOOL_AND(bb.has_contact_type) AS has_contact_type,
        BOOL_AND(bb.has_general_location) AS has_general_location,
        BOOL_AND(bb.has_hit_to_fielder) AS has_hit_to_fielder,
        BOOL_AND(bb.has_any_location) AS has_any_location,
    FROM {{ ref('stg_events') }} AS events
    LEFT JOIN {{ ref('event_completeness_batted_balls') }} AS bb USING (event_key)
    GROUP BY 1
),

fielding_credit AS (
    SELECT
        events.game_id,
        BOOL_AND(fc.has_fielder_putouts) AS has_fielder_putouts,
        BOOL_AND(fc.has_fielder_assists) AS has_fielder_assists,
        BOOL_AND(fc.has_fielder_errors) AS has_fielder_errors,
    FROM {{ ref('stg_events') }} AS events
    LEFT JOIN {{ ref('event_completeness_fielding_credit') }} AS fc USING (event_key)
    GROUP BY 1
),

pitches AS (
    SELECT
        events.game_id,
        BOOL_AND(p.has_count_balls) AS has_count_balls,
        BOOL_AND(p.has_count_strikes) AS has_count_strikes,
        BOOL_AND(p.has_count) AS has_count,
        BOOL_AND(p.has_pitches) AS has_pitches,
        BOOL_AND(p.has_pitch_results) AS has_pitch_results,
        BOOL_AND(p.has_strike_types) AS has_pitch_strike_types,
    FROM {{ ref('stg_events') }} AS events
    LEFT JOIN {{ ref('event_completeness_pitches') }} AS p USING (event_key)
    GROUP BY 1
),

joined AS (
    SELECT
        game_id,
        game_start_info.source_type = 'Event' AS has_play_by_play,
        game_start_info.source_type IN ('Event', 'BoxScore') AS has_box_score,
        COALESCE(batted_balls.has_contact_type, FALSE) AS has_contact_type,
        COALESCE(batted_balls.has_general_location, FALSE) AS has_general_location,
        COALESCE(batted_balls.has_hit_to_fielder, FALSE) AS has_hit_to_fielder,
        COALESCE(batted_balls.has_any_location, FALSE) AS has_any_location,
        COALESCE(fielding_credit.has_fielder_putouts, FALSE) AS has_fielder_putouts,
        COALESCE(fielding_credit.has_fielder_assists, FALSE) AS has_fielder_assists,
        COALESCE(fielding_credit.has_fielder_errors, FALSE) AS has_fielder_errors,
        COALESCE(pitches.has_count_balls, FALSE) AS has_count_balls,
        COALESCE(pitches.has_count_strikes, FALSE) AS has_count_strikes,
        COALESCE(pitches.has_count, FALSE) AS has_count,
        COALESCE(pitches.has_pitches, FALSE) AS has_pitches,
        COALESCE(pitches.has_pitch_results, FALSE) AS has_pitch_results,
        COALESCE(pitches.has_pitch_strike_types, FALSE) AS has_pitch_strike_types
    FROM {{ ref('game_start_info') }} AS game_start_info
    LEFT JOIN batted_balls USING (game_id)
    LEFT JOIN fielding_credit USING (game_id)
    LEFT JOIN pitches USING (game_id)
)

SELECT * FROM joined
