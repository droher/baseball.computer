AUDIT (
  name sum_consistency,
  defaults (
    tolerance = 0
  )
);

WITH this_sum AS (
  SELECT SUM(@column) AS s FROM @this_model
),
other_sum AS (
  SELECT SUM(@to_column) AS s FROM @to_model
)
SELECT this_sum.s AS this_total, other_sum.s AS other_total
FROM this_sum, other_sum
WHERE ABS(COALESCE(this_sum.s, 0) - COALESCE(other_sum.s, 0)) > @tolerance
