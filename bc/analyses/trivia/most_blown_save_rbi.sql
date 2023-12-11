SELECT  
    o.player_id,
    COUNT(*)
FROM {{ ref('event_offense_stats') }} AS o
WHERE runs_batted_in > 0
    AND event_key IN (SELECT event_key FROM {{ ref('event_pitching_flags') }} WHERE blown_save_flag)
GROUP BY 1