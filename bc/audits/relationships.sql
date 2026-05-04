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
  @relationships_check(@column, @to_column, @to_model),
  @condition
)
