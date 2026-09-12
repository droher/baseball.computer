AUDIT (
  name pitch_rows_only_for_resolved_events
);

SELECT pitches.event_key, status.pitch_sequence_resolution_status
FROM main_models.stg_event_pitch_sequences AS pitches
LEFT JOIN @this_model AS status USING (event_key)
WHERE status.pitch_sequence_resolution_status IS DISTINCT FROM 'Resolved'
