MODEL (
  name main_models.stg_box_score_double_plays,
  kind FULL,
  description 'Individually noted double play events from box score accounts, with varying degrees of detail.',
  column_descriptions (
    game_id = @doc('game_id')
  ),
);






WITH source AS (
    SELECT * FROM box_score.box_score_double_plays
),

renamed AS (
    SELECT
        game_id,
        defense_side,
        fielders

    FROM source
)

SELECT * FROM renamed
