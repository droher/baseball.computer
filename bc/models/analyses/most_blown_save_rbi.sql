MODEL (
  name main_models.most_blown_save_rbi,
  kind VIEW
);

SELECT  
    o.player_id,
    COUNT(*)
FROM main_models.event_offense_stats AS o
WHERE runs_batted_in > 0
    AND event_key IN (SELECT event_key FROM main_models.event_pitching_flags WHERE blown_save_flag)
GROUP BY 1
