AUDIT (
  name park_factor_bounds
);

WITH canonical_population AS (
  SELECT park_id, season, home_league AS league,
    BOOL_AND(is_negro_leagues IS TRUE) AS confirmed_nlb
  FROM main_models.game_start_info
  WHERE game_type = 'RegularSeason'
    AND home_league = away_league
  GROUP BY park_id, season, home_league
  HAVING COUNT(*) > 25
)
SELECT src.*
FROM @this_model AS src
LEFT JOIN canonical_population AS population USING (park_id, season, league)
WHERE @column IS NOT NULL
  AND (
    @IF(
      @excused_population,
      (ISFINITE(@column) AND @column > @max_v AND population.confirmed_nlb IS TRUE),
      (
        NOT ISFINITE(@column)
        OR @column < 0
        OR (@column > @max_v AND population.confirmed_nlb IS NOT TRUE)
      )
    )
  )
