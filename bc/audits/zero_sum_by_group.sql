AUDIT (
  name zero_sum_by_group,
  defaults (
    tolerance = 0.001
  )
);

SELECT @EACH(@group_by, c -> c), SUM(@measure) AS total
FROM @this_model
GROUP BY @EACH(@group_by, c -> c)
HAVING ABS(SUM(@measure)) > @tolerance
