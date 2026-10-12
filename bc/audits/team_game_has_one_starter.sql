AUDIT (
  name team_game_has_one_starter
);

WITH grouped AS (
  SELECT game_id, team_id, SUM(games_started) AS n_starters
  FROM @this_model
  GROUP BY game_id, team_id
)

SELECT
  COALESCE(grouped.game_id, info.game_id) AS game_id,
  COALESCE(grouped.team_id, info.team_id) AS team_id,
  COALESCE(grouped.n_starters, 0) AS n_starters
FROM grouped
FULL OUTER JOIN main_models.team_game_start_info AS info USING (game_id, team_id)
WHERE COALESCE(grouped.n_starters, 0) != 1
  AND (
    grouped.game_id IS NOT NULL
    OR info.source_type IS NULL
    OR info.source_type != 'GameLog'
  )
  AND (@is_less_complete_source(info) IS TRUE) = @excused_population
  AND NOT @team_game_data_issue_match(
    COALESCE(grouped.game_id, info.game_id),
    COALESCE(grouped.team_id, info.team_id),
    'starting_pitcher_no_appearance'
  )
