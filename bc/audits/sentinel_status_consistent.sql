AUDIT (
  name sentinel_status_consistent
);

SELECT
  event_key,
  dimension,
  sentinel_type,
  observed_status
FROM @this_model
WHERE NOT (
  (sentinel_type = 'valid_value' AND observed_status IN ('observed', 'derived'))
  OR (sentinel_type = 'unknown' AND observed_status IN ('unknown_code', 'derived'))
  OR (sentinel_type = 'zero' AND observed_status = 'unknown_code')
  OR (sentinel_type = 'not_applicable' AND observed_status = 'not_applicable')
  OR (sentinel_type = 'null' AND observed_status IN ('missing', 'derived'))
  OR (sentinel_type = 'empty_sequence' AND observed_status = 'missing')
  OR (
    sentinel_type = 'default'
    AND (
      (dimension = 'location_angle' AND observed_status = 'default_code')
      OR (dimension != 'location_angle' AND observed_status = 'observed')
    )
  )
)
