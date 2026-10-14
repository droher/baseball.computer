MODEL (
  name main_models.stg_nlb_games,
  kind FULL,
  description 'Negro Leagues games (gameinfo.csv), one row per game, from Retrosheet''s negroleagues.zip. Columns other than game_id and date keep Retrosheet''s names; attendance stays text, since Retrosheet marks estimates such as 6500? and <1000.',
  grain (game_id),
  audits (
    unique_combination_of_columns(columns := (game_id))
  ),
);

SELECT
    gid AS game_id,
    STRPTIME(date::VARCHAR, '%Y%m%d')::DATE AS date,
    * EXCLUDE (gid, date)
FROM nlb_stats.gameinfo
