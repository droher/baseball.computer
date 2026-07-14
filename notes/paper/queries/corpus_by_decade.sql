PIVOT (
  SELECT (season // 10) * 10 AS decade, source_type, game_id
  FROM main_models.game_start_info
) ON source_type USING COUNT(game_id)
GROUP BY decade
ORDER BY decade;
