AUDIT (
  name bounded_excluding_data_issues,
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
  @IF(@inclusive, @column < @min_v OR @column > @max_v, @column <= @min_v OR @column >= @max_v),
  NOT @box_score_data_issue_match(src.game_id, src.player_id, @issue_type)
)
