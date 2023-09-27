SELECT
    player_id,
    player_type,
    COUNT(*) AS total_games,
    COUNT_IF(has_play_by_play) AS play_by_play,
    COUNT_IF(has_contact_type) AS contact_type,
    COUNT_IF(has_location) AS location,
    COUNT_IF(has_batted_to_fielder) AS batted_to_fielder,
    COUNT_IF(has_count_balls) AS count_balls,
    COUNT_IF(has_count_strikes) AS count_strikes,
    COUNT_IF(has_count) AS count,
    COUNT_IF(has_pitches) AS pitches,
    COUNT_IF(has_pitch_results) AS pitch_results,
    COUNT_IF(has_pitch_strike_types) AS pitch_strike_types
FROM {{ ref('player_game_data_completeness') }}
GROUP BY 1, 2
ORDER BY total_games DESC
