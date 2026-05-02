MODEL (
  name main_models.stg_box_score_stolen_bases,
  kind FULL,
  description 'Individually noted stolen base events from box score accounts, with varying degrees of detail.',
  column_descriptions (
    game_id = @doc('game_id'),
    runner_id = @doc('runner_id'),
    pitcher_id = @doc('pitcher_id'),
    inning = @doc('inning')
  ),
);






WITH source AS (
    SELECT * FROM box_score.box_score_stolen_bases
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
