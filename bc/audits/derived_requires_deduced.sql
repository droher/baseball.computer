AUDIT (
  name derived_requires_deduced
);

SELECT
  event_key,
  dimension,
  observed_status,
  deduced_value
FROM @this_model
WHERE (observed_status = 'derived' AND deduced_value IS NULL)
   OR (observed_status != 'derived' AND deduced_value IS NOT NULL)
