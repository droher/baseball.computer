AUDIT (
  name residual_value_matches_status
);

SELECT
  game_id,
  team_id,
  player_id,
  fielding_position,
  stat_name,
  aggregate_status,
  aggregate_value,
  event_value,
  residual_value
FROM @this_model
WHERE
  (
    aggregate_status = 'present_clean'
    AND aggregate_value IS NOT NULL
    AND event_value IS NOT NULL
    AND residual_value != 0
  )
  OR (aggregate_status = 'negative_residual' AND (residual_value IS NULL OR residual_value >= 0))
  OR (aggregate_status = 'contradicted' AND (residual_value IS NULL OR residual_value <= 0))
  OR (aggregate_status = 'missing' AND aggregate_value IS NOT NULL)
