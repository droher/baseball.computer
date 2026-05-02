MODEL (
  name main_models.stg_game_lineup_appearances,
  kind FULL,
  grain (game_id, player_id, lineup_position, start_event_id),
  columns (
    game_id VARCHAR,
    player_id VARCHAR,
    side SIDE,
    lineup_position UTINYINT,
    entered_game_as VARCHAR,
    start_event_id UTINYINT,
    end_event_id UTINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    player_id = @doc('player_id'),
    side = @doc('side'),
    lineup_position = @doc('lineup_position')
  ),
  audits (
    relationships(column := game_id, to_column := game_id, to_model := main_models.stg_games),
    relationships(column := player_id, to_column := player_id, to_model := main_models.stg_bio)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_game_lineup_appearances.parquet'
  ),
);







WITH source AS (
    SELECT * FROM game.game_lineup_appearances
),

renamed AS (
    SELECT
        game_id,
        player_id,
        side,
        lineup_position,
        entered_game_as,
        start_event_id,
        end_event_id,

    FROM source
)

SELECT * FROM renamed
