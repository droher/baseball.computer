MODEL (
  name main_models.stg_nlb_players,
  kind FULL,
  description 'Negro Leagues player seasons (allplayers.csv), one row per player, team, and season, from Retrosheet''s negroleagues.zip. A first or last game of 0, which Retrosheet gives where it has no game for the season, is NULL. Columns other than the ids and dates keep Retrosheet''s names.',
  grain (player_id, team_id, season),
  audits (
    unique_combination_of_columns(columns := (player_id, team_id, season), blocking := FALSE)
  ),
);

SELECT
    id AS player_id,
    team AS team_id,
    STRPTIME(NULLIF(first_g, 0)::VARCHAR, '%Y%m%d')::DATE AS first_game_date,
    STRPTIME(NULLIF(last_g, 0)::VARCHAR, '%Y%m%d')::DATE AS last_game_date,
    * EXCLUDE (id, team, first_g, last_g)
FROM nlb_stats.allplayers
