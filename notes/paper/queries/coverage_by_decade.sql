SELECT
  (g.season // 10) * 10 AS decade,
  SUM(e.trajectory_unknown) AS trajectory_unknown,
  SUM(e.trajectory_known) AS trajectory_known,
  SUM(e.batted_location_unknown) AS location_unknown,
  SUM(e.batted_location_known) AS location_known,
  SUM(e.trajectory_unknown) * 1.0 / NULLIF(SUM(e.trajectory_known) + SUM(e.trajectory_unknown), 0) AS share_unknown_trajectory,
  SUM(e.batted_location_unknown) * 1.0 / NULLIF(SUM(e.batted_location_known) + SUM(e.batted_location_unknown), 0) AS share_unknown_location
FROM main_models.event_offense_stats e
JOIN main_models.team_game_start_info g
  ON e.team_id = g.team_id AND e.game_id = g.game_id
GROUP BY 1
ORDER BY 1;
