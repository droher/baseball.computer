MODEL (
  name main_models.stg_box_score_caught_stealing,
  kind FULL,
  description 'Individually noted caught stealing events from box score accounts, with varying degrees of detail.',
  column_descriptions (
    game_id = @doc('game_id'),
    runner_id = @doc('runner_id'),
    pitcher_id = @doc('pitcher_id'),
    inning = @doc('inning')
  ),
);






WITH source AS (
    SELECT * FROM box_score.box_score_caught_stealing
),

renamed AS (
    SELECT
        game_id,
        running_side,
        runner_id,
        pitcher_id,
        catcher_id,
        inning

    FROM source
)

SELECT * FROM renamed
WHERE substring(game_id, 4,4)::INT < 1919
