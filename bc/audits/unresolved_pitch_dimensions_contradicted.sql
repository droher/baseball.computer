AUDIT (
  name unresolved_pitch_dimensions_contradicted
);

SELECT
  event_key,
  dimension,
  pitch_sequence_resolution_status,
  observed_status,
  sentinel_type,
  raw_value,
  source_acquisition_status,
  model_input_eligible
FROM @this_model
WHERE dimension IN ('pitch_sequence', 'pitch_results', 'strike_types', 'pitch_count_total')
  AND (
    (pitch_sequence_resolution_status = 'Unresolved') IS DISTINCT FROM (observed_status = 'contradicted')
    OR (
      pitch_sequence_resolution_status = 'Unresolved'
      AND (
        raw_value IS NOT NULL
        OR sentinel_type != 'null'
        OR source_acquisition_status != 'contradicted'
        OR model_input_eligible
      )
    )
  )
