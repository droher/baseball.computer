MODEL (
  name main_models.nlb_season_sources,
  kind FULL,
  description 'Which source supplies Negro Leagues regular-season player totals, one row per season with Negro Leagues data in Lahman or Retrosheet. retrosheet_coverage is Retrosheet''s at-bats in games with a box score (regular season, plus exhibitions between two clubs Lahman lists that season) divided by Lahman''s Negro Leagues at-bats. A season with Lahman rows uses Lahman (Databank) when the ratio is below the threshold in bc/macros/_nlb.py or Lahman has no at-bats; it uses Retrosheet otherwise or when Lahman has no rows.',
  grain (season),
  columns (
    season SMALLINT,
    lahman_at_bats UINTEGER,
    retrosheet_at_bats UINTEGER,
    retrosheet_coverage DOUBLE,
    season_source VARCHAR
  ),
  column_descriptions (
    season = @doc('season'),
    lahman_at_bats = 'Lahman Negro Leagues batting at-bats.',
    retrosheet_at_bats = 'Retrosheet Negro Leagues at-bats in games with a box score: regular season, plus exhibitions between two clubs Lahman lists that season.',
    retrosheet_coverage = 'retrosheet_at_bats / lahman_at_bats; NULL when Lahman has no at-bats.',
    season_source = 'Databank when Lahman supplies the season''s Negro Leagues regular-season player totals, Retrosheet otherwise.'
  ),
  audits (
    not_null(columns := (season, season_source)),
    unique_grain(columns := (season)),
    accepted_values(column := season_source, is_in := ('Databank', 'Retrosheet'))
  ),
);

WITH lahman AS (
    SELECT
        season,
        SUM(at_bats) AS lahman_at_bats,
        TRUE AS has_lahman_rows
    FROM main_models.stg_databank_batting
    WHERE is_negro_leagues
    GROUP BY season
),

retrosheet AS (
    SELECT
        scopes.season,
        COALESCE(SUM(bat.b_ab) FILTER (WHERE scopes.is_matched_scope AND scopes.has_box_score), 0) AS retrosheet_at_bats
    FROM main_models.nlb_game_scopes AS scopes
    LEFT JOIN main_models.stg_nlb_batting AS bat USING (game_id)
    GROUP BY scopes.season
),

final AS (
    SELECT
        season,
        COALESCE(lahman.lahman_at_bats, 0)::UINTEGER AS lahman_at_bats,
        COALESCE(retrosheet.retrosheet_at_bats, 0)::UINTEGER AS retrosheet_at_bats,
        retrosheet.retrosheet_at_bats / NULLIF(lahman.lahman_at_bats, 0) AS retrosheet_coverage,
        CASE
            WHEN lahman.has_lahman_rows AND COALESCE(retrosheet_coverage, 0) < @nlb_lahman_coverage_threshold()
                THEN 'Databank'
            ELSE 'Retrosheet'
        END AS season_source
    FROM lahman
    FULL OUTER JOIN retrosheet USING (season)
)

SELECT * FROM final
