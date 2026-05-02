MODEL (
  name main_models.stg_box_score_triple_plays,
  kind FULL,
  description 'Individually noted triple play events from box score accounts, with varying degrees of detail.',
  columns (
    game_id VARCHAR,
    defense_side VARCHAR,
    fielders VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id')
  ),
  audits (
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  ),
);






WITH source AS (
    SELECT * FROM box_score.box_score_triple_plays
),

renamed AS (
    SELECT
        game_id,
        defense_side,
        fielders

    FROM source
)

SELECT * FROM renamed
