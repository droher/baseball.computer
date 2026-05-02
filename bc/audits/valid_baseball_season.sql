AUDIT (
  name valid_baseball_season,
  defaults (
    column = season
  )
);

SELECT @column
FROM @this_model
WHERE @column IS NOT NULL
  AND (@column < 1871 OR @column > EXTRACT(YEAR FROM CURRENT_DATE))
