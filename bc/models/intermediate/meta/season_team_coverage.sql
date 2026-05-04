MODEL (
  name main_models.season_team_coverage,
  kind FULL,
  grain (season, team_id),
  columns (
    season SMALLINT,
    team_id TEAM_ID,
    league VARCHAR,
    least_granular_source_type VARCHAR
  ),
  column_descriptions (
    season = @doc('season'),
    team_id = @doc('team_id'),
    league = @doc('league')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_season_team_coverage.parquet'
  ),
);







WITH final AS (
    SELECT
        season,
        team_id,
        ANY_VALUE(league) AS league,
        CASE WHEN BOOL_AND(source_type = 'PlayByPlay')
                THEN 'PlayByPlay'
            WHEN BOOL_AND(source_type = 'PlayByPlay' OR source_type = 'BoxScore')
                THEN 'BoxScore'
            ELSE 'GameLog'
        END AS least_granular_source_type
    FROM main_models.team_game_start_info
    WHERE game_id NOT IN (SELECT game_id FROM main_models.game_forfeits)
        AND (game_type != 'Exhibition' OR league IS NULL)
    GROUP BY 1, 2
)

SELECT * FROM final
