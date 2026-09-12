AUDIT (
  name pitch_totals_match_resolution_status
);

SELECT *
FROM @this_model
WHERE (
    pitch_sequence_resolution_status = 'Resolved'
    AND (@REDUCE(@EACH(@normalized_pitch_counters(), c -> @c IS NULL), (a, b) -> a OR b))
)
OR (
    pitch_sequence_resolution_status IN ('Unavailable', 'Unresolved')
    AND (@REDUCE(@EACH(@normalized_pitch_counters(), c -> @c IS NOT NULL), (a, b) -> a OR b))
)
