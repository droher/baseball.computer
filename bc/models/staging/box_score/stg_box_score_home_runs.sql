MODEL (
  name main_models.stg_box_score_home_runs,
  kind FULL,
  description 'Individually noted home run events from box score accounts, with varying degrees of detail.',
  column_descriptions (
    game_id = @doc('game_id'),
    batting_side = @doc('batting_side'),
    batter_id = @doc('batter_id'),
    pitcher_id = @doc('pitcher_id'),
    inning = @doc('inning')
  ),
);






WITH source AS (
    SELECT * FROM box_score.box_score_home_runs
),

renamed AS (
    SELECT
        game_id,
        batting_side,
        batter_id,
        pitcher_id,
        inning,
        runners_on,
        outs

    FROM source
)

SELECT * FROM renamed
