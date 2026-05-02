MODEL (
  name main_models.stg_box_score_hit_by_pitches,
  kind FULL,
  description 'Individually noted hit by pitch events from box score accounts, with varying degrees of detail.',
  columns (
    game_id VARCHAR,
    pitching_side VARCHAR,
    pitcher_id VARCHAR,
    batter_id VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    pitcher_id = @doc('pitcher_id'),
    batter_id = @doc('batter_id')
  ),
);






WITH source AS (
    SELECT * FROM box_score.box_score_hit_by_pitches
),

renamed AS (
    SELECT
        game_id,
        pitching_side,
        pitcher_id,
        batter_id

    FROM source
)

SELECT * FROM renamed
