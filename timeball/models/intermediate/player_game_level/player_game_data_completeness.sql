{{
  config(
    materialized = 'table',
    )
}}
WITH events AS (
    SELECT
        *,
        'BATTING' AS player_type
    FROM {{ ref('stg_events') }}
    UNION ALL
    SELECT
        *,
        'PITCHING' AS player_type
    FROM {{ ref('stg_events') }}
),

batted_balls AS (
    SELECT
        events.game_id,
        events.player_type,
        CASE WHEN events.player_type = 'BATTING' THEN batter_id ELSE pitcher_id END AS player_id,
        ANY_VALUE(CASE WHEN events.player_type = 'BATTING' THEN batting_team_id ELSE fielding_team_id END) AS team_id,
        COALESCE(BOOL_AND(bb.trajectory != 'Unknown'), TRUE) AS has_trajectory,
        COALESCE(BOOL_AND(bb.recorded_location != 'Unknown'), TRUE) AS has_scoresheet_location,
        COALESCE(BOOL_AND(bb.location_side != 'Unknown'), TRUE) AS has_location,
        COALESCE(BOOL_AND(bb.batted_to_fielder != 0 OR NOT rt.is_in_play), TRUE) AS has_batted_to_fielder,
    FROM events
    LEFT JOIN {{ ref('calc_batted_ball_type') }} AS bb USING (event_key)
    LEFT JOIN {{ ref('seed_plate_appearance_result_types') }} AS rt
        ON rt.plate_appearance_result = events.plate_appearance_result
    GROUP BY 1, 2, 3
),

pitches AS (
    SELECT
        events.game_id,
        events.player_type,
        CASE WHEN player_type = 'BATTING' THEN batter_id ELSE pitcher_id END AS player_id,
        ANY_VALUE(CASE WHEN player_type = 'BATTING' THEN batting_team_id ELSE fielding_team_id END) AS team_id,
        BOOL_AND(p.has_count_balls) AS has_count_balls,
        BOOL_AND(p.has_count_strikes) AS has_count_strikes,
        BOOL_AND(p.has_count) AS has_count,
        BOOL_AND(p.has_pitches) AS has_pitches,
        BOOL_AND(p.has_pitch_results) AS has_pitch_results,
        BOOL_AND(p.has_strike_types) AS has_pitch_strike_types,
    FROM events
    LEFT JOIN {{ ref('event_completeness_pitches') }} AS p USING (event_key)
    GROUP BY 1, 2, 3
),

-- TODO: Add fielding credit by comparing event-based totals to box totals
joined AS (
    SELECT
        game_id,
        player_id,
        player_type,
        game_start_info.season,
        game_start_info.date,
        game_start_info.home_league AS league,
        COALESCE(batted_balls.has_trajectory, FALSE) AS has_trajectory,
        COALESCE(batted_balls.has_scoresheet_location, FALSE) AS has_scoresheet_location,
        COALESCE(batted_balls.has_location, FALSE) AS has_location,
        COALESCE(batted_balls.has_batted_to_fielder, FALSE) AS has_batted_to_fielder,
        COALESCE(pitches.has_count_balls, FALSE) AS has_count_balls,
        COALESCE(pitches.has_count_strikes, FALSE) AS has_count_strikes,
        COALESCE(pitches.has_count, FALSE) AS has_count,
        COALESCE(pitches.has_pitches, FALSE) AS has_pitches,
        COALESCE(pitches.has_pitch_results, FALSE) AS has_pitch_results,
        COALESCE(pitches.has_pitch_strike_types, FALSE) AS has_pitch_strike_types
    FROM {{ ref('game_start_info') }} AS game_start_info
    FULL OUTER JOIN batted_balls USING (game_id)
    FULL OUTER JOIN pitches USING (game_id, player_type, player_id)
    WHERE player_id IS NOT NULL
)

SELECT * FROM joined
