MODEL (
  name main_models.stg_box_score_pinch_running_lines,
  kind FULL,
  description 'Box score batting lines that are specific to stats accumulated while a player is pinch hitting.',
  columns (
    game_id VARCHAR,
    pinch_runner_id VARCHAR,
    inning BIGINT,
    side VARCHAR,
    runs BIGINT,
    stolen_bases BIGINT,
    caught_stealing BIGINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    inning = @doc('inning'),
    side = @doc('side'),
    runs = @doc('runs'),
    stolen_bases = @doc('stolen_bases'),
    caught_stealing = @doc('caught_stealing')
  ),
  audits (
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id),
    relationships(column := pinch_runner_id, to_model := main_models.people, to_column := player_id)
  ),
);






WITH source AS (
    SELECT * FROM box_score.box_score_pinch_running_lines
),

renamed AS (
    SELECT
        game_id,
        pinch_runner_id,
        inning,
        side,
        runs,
        stolen_bases,
        caught_stealing

    FROM source
)

SELECT * FROM renamed
