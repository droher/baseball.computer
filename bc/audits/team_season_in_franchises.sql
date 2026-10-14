AUDIT (
  name team_season_in_franchises,
  defaults (
    condition = TRUE
  )
);

SELECT season, team_id
FROM @this_model AS src
WHERE @AND(
  team_id IS NOT NULL,
  NOT EXISTS (
    SELECT 1
    FROM main_seeds.seed_franchises AS franchises
    WHERE franchises.team_id = src.team_id
      AND YEAR(franchises.date_start) <= src.season
      AND (franchises.date_end IS NULL OR YEAR(franchises.date_end) >= src.season)
  ),
  @condition
)
