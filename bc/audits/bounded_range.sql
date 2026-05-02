AUDIT (
  name bounded_range,
  defaults (
    condition = TRUE,
    inclusive = TRUE
  )
);

SELECT @column
FROM @this_model AS src
WHERE @AND(
  @column IS NOT NULL,
  @condition,
  @IF(@inclusive, @column < @min_v OR @column > @max_v, @column <= @min_v OR @column >= @max_v)
)
