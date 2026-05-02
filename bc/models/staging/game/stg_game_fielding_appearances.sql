MODEL (
  name main_models.stg_game_fielding_appearances,
  kind FULL,
  grain (game_id, player_id, fielding_position, start_event_id),
  columns (
    game_id VARCHAR,
    player_id VARCHAR,
    side SIDE,
    fielding_position UTINYINT,
    start_event_id UTINYINT,
    end_event_id UTINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    player_id = @doc('player_id'),
    side = @doc('side'),
    fielding_position = @doc('fielding_position')
  ),
  audits (
    relationships(column := game_id, to_column := game_id, to_model := main_models.stg_games),
    relationships(column := player_id, to_column := player_id, to_model := main_models.stg_bio)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_game_fielding_appearances.parquet'
  ),
);







WITH source AS (
    SELECT * FROM game.game_fielding_appearances
),

renamed AS (
    SELECT
        game_id,
        player_id,
        side,
        fielding_position,
        start_event_id,
        end_event_id,
    FROM source
)

SELECT * FROM renamed
