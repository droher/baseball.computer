MODEL (
  name main_models.stg_game_earned_runs,
  kind FULL,
  grain (game_id, player_id),
  columns (
    game_id VARCHAR,
    player_id VARCHAR,
    earned_runs UTINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    player_id = @doc('player_id'),
    earned_runs = @doc('earned_runs')
  ),
  audits (
    relationships(column := game_id, to_column := game_id, to_model := main_models.stg_games)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_game_earned_runs.parquet'
  ),
);







WITH source AS (
    SELECT * FROM game.game_earned_runs
),

renamed AS (
    SELECT
        game_id,
        player_id,
        earned_runs
    FROM source
)

SELECT * FROM renamed
