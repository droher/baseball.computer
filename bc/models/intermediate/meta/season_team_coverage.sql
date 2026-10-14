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
    league = @doc('league'),
    least_granular_source_type = 'The least detailed source among the team''s non-exhibition games: PlayByPlay, BoxScore, GameLog, or NegroLeaguesStats. A game recorded only in negroleagues.zip counts as BoxScore when Retrosheet has its box score, and as NegroLeaguesStats when it has only appearances.'
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_season_team_coverage.parquet'
  ),
);







WITH nlb_box_scores AS (
    SELECT game_id
    FROM main_models.stg_nlb_games
    WHERE LOWER(box) = 'y'
),

team_games AS (
    SELECT
        season,
        team_id,
        league,
        CASE
            WHEN source_type = 'NegroLeaguesStats' AND game_id IN (SELECT game_id FROM nlb_box_scores)
                THEN 'BoxScore'
            ELSE source_type
        END AS source_type
    FROM main_models.team_game_start_info
    WHERE game_id NOT IN (SELECT game_id FROM main_models.game_forfeits)
        AND (game_type != 'Exhibition' OR league IS NULL)
),

final AS (
    SELECT
        season,
        team_id,
        ANY_VALUE(league) AS league,
        CASE WHEN BOOL_AND(source_type = 'PlayByPlay')
                THEN 'PlayByPlay'
            WHEN BOOL_AND(source_type = 'PlayByPlay' OR source_type = 'BoxScore')
                THEN 'BoxScore'
            WHEN BOOL_OR(source_type = 'GameLog')
                THEN 'GameLog'
            ELSE 'NegroLeaguesStats'
        END AS least_granular_source_type
    FROM team_games
    GROUP BY 1, 2
)

SELECT * FROM final
