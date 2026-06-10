AUDIT (
  name deduced_value_in_domain
);

SELECT
  event_key,
  dimension,
  deduced_value
FROM @this_model
WHERE dimension = @dimension
  AND deduced_value IS NOT NULL
  AND deduced_value NOT IN @allowed
