AUDIT (
  name franchise_span_covers_game
);

SELECT game_id, date, side, team_id
FROM (
  SELECT game_id, date, 'home' AS side, home_team_id AS team_id, home_franchise_id AS franchise_id
  FROM @this_model
  UNION ALL
  SELECT game_id, date, 'away' AS side, away_team_id AS team_id, away_franchise_id AS franchise_id
  FROM @this_model
) AS sides
WHERE team_id IS NOT NULL
  AND franchise_id IS NULL
