AUDIT (
  name relationships,
  defaults (
    condition = TRUE
  )
);

SELECT @column
FROM @this_model AS src
WHERE @AND(
  @column IS NOT NULL,
  @column NOT IN (SELECT @to_column FROM @to_model),
  @condition
)
