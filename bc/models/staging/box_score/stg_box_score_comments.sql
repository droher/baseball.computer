MODEL (
  name main_models.stg_box_score_comments,
  kind FULL,
  description 'Comment lines from box score files, along with their associated game',
  grain (game_id, sequence_id),
  columns (
    game_id VARCHAR,
    sequence_id BIGINT,
    comment VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    sequence_id = @doc('sequence_id'),
    comment = @doc('comment')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_box_score_comments.parquet'
  ),
);







WITH source AS (
    SELECT * FROM box_score.box_score_comments
),

renamed AS (
    SELECT
        game_id,
        sequence_id,
        comment
    FROM source
)

SELECT * FROM renamed
