AUDIT (
  name pitch_counters_null_unless_resolved
);

SELECT *
FROM @this_model
WHERE (
    pitch_sequence_resolution_status = 'Resolved'
    AND (@REDUCE(@EACH(@normalized_pitch_counters(), c -> @c IS NULL), (a, b) -> a OR b))
)
OR (
    pitch_sequence_resolution_status IS DISTINCT FROM 'Resolved'
    AND (@REDUCE(@EACH(@normalized_pitch_counters(), c -> @c IS NOT NULL), (a, b) -> a OR b))
)
