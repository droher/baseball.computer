AUDIT (
  name min_row_count
);

SELECT COUNT(*) AS row_count
FROM @this_model
HAVING COUNT(*) < @threshold
