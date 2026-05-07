AUDIT (
  name team_game_has_one_starter
);

WITH grouped AS (
  SELECT game_id, team_id, SUM(games_started) AS n_starters
  FROM @this_model
  GROUP BY game_id, team_id
)

SELECT g.game_id, g.team_id, g.n_starters FROM grouped AS g
WHERE g.n_starters != 1
  AND NOT @team_game_data_issue_match(g.game_id, g.team_id, 'starting_pitcher_no_appearance')
