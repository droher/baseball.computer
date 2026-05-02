MODEL (
  name main_models.stg_box_score_line_scores,
  kind FULL,
  description 'Inning-by-inning run totals from box score accounts.',
  grain (game_id, batting_side, inning),
  columns (
    game_id VARCHAR,
    batting_side VARCHAR,
    inning BIGINT,
    runs BIGINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    batting_side = @doc('batting_side'),
    inning = @doc('inning'),
    runs = @doc('runs')
  ),
  audits (
    not_null(columns := (game_id, batting_side, inning)),
    unique_grain(columns := (game_id, batting_side, inning)),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_box_score_line_scores.parquet'
  ),
);







WITH source AS (
    SELECT * FROM box_score.box_score_line_scores
),

renamed AS (
    SELECT
        game_id,
        side AS batting_side,
        inning,
        runs
    FROM source
)

SELECT * FROM renamed
