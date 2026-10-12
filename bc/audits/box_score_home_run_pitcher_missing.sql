AUDIT (
  name box_score_home_run_pitcher_missing
);

SELECT
  src.game_id,
  src.batting_side,
  src.batter_id,
  src.pitcher_id
FROM @this_model AS src
LEFT JOIN main_models.game_start_info AS info USING (game_id)
WHERE src.pitcher_id IS NULL
  AND (@is_less_complete_source(info) IS TRUE) = @excused_population
