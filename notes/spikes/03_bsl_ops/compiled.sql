-- BSL-emitted SQL for sm.group_by("season", "league").aggregate("ops")
-- Captured 2026-05-01 with boring-semantic-layer 0.3.12 + ibis-framework 11.0.x.
SELECT
  "t6"."season",
  "t6"."league",
  (
    CAST("t6"."total_obs" AS DOUBLE) / CAST("t6"."total_obo" AS DOUBLE)
  ) + (
    CAST("t6"."total_tb" AS DOUBLE) / CAST("t6"."total_ab" AS DOUBLE)
  ) AS "ops"
FROM (
  SELECT
    "t5"."season",
    "t5"."league",
    SUM("t5"."total_bases") AS "total_tb",
    SUM("t5"."at_bats") AS "total_ab",
    SUM("t5"."on_base_successes") AS "total_obs",
    SUM("t5"."on_base_opportunities") AS "total_obo"
  FROM (
    SELECT
      *
    FROM (
      SELECT
        "t3"."season",
        "t3"."league",
        "t3"."game_type",
        "t2"."on_base_successes",
        "t2"."on_base_opportunities",
        "t2"."total_bases",
        "t2"."at_bats"
      FROM "main_models"."event_offense_stats" AS "t2"
      INNER JOIN "main_models"."event_states_full" AS "t3"
        ON "t2"."event_key" = "t3"."event_key"
    ) AS "t4"
    WHERE
      "t4"."game_type" = 'RegularSeason'
  ) AS "t5"
  GROUP BY
    1,
    2
) AS "t6"
