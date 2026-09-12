AUDIT (
  name issue_events_unresolved
);

SELECT issues.event_key, issues.sequence_id, status.pitch_sequence_resolution_status
FROM @this_model AS issues
LEFT JOIN main_models.stg_event_pitch_sequence_status AS status USING (event_key)
WHERE status.pitch_sequence_resolution_status IS DISTINCT FROM 'Unresolved'
