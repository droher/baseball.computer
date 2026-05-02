MODEL (
  name main_models.stg_event_comments,
  kind FULL,
  description 'Comment lines from event files, along with their associated event',
  grain (event_key, comment),
  columns (
    event_key UINTEGER,
    comment VARCHAR
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    comment = @doc('comment')
  ),
  audits (
    relationships(column := event_key, to_column := event_key, to_model := main_models.stg_events)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_event_comments.parquet'
  ),
);







WITH source AS (
    SELECT * FROM event.event_comments
),

renamed AS (
    SELECT DISTINCT
        event_key,
        comment
    FROM source
    WHERE comment IS NOT NULL
)

SELECT * FROM renamed
