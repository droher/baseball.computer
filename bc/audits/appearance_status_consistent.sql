AUDIT (
  name appearance_status_consistent
);

SELECT game_id, appearance_start_event_id
FROM @this_model
GROUP BY 1, 2
HAVING COUNT(DISTINCT pitch_sequence_resolution_status) != 1
