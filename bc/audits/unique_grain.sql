AUDIT (
  name unique_grain
);

SELECT @EACH(@columns, c -> c)
FROM @this_model
GROUP BY @EACH(@columns, c -> c)
HAVING COUNT(*) > 1
