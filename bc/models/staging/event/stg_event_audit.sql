MODEL (
  name main_models.stg_event_audit,
  kind FULL,
  description 'Connects each event to the raw Retrosheet data to enable QA/debugging. See the Retosheet event file spec for more info: https://www.retrosheet.org/eventfile.htm',
  grain (event_key),
  columns (
    game_id VARCHAR,
    event_id UTINYINT,
    filename VARCHAR,
    line_number UINTEGER,
    event_key UINTEGER,
    raw_play VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_id = @doc('event_id'),
    filename = @doc('filename'),
    line_number = 'Line number of the file on which the event was found',
    event_key = @doc('event_key'),
    raw_play = 'The string representation of the play'
  ),
  audits (
    relationships(column := event_key, to_column := event_key, to_model := main_models.stg_events)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_event_audit.parquet'
  ),
);







WITH source AS (
    SELECT * FROM event.event_audit
),

renamed AS (
    SELECT
        game_id,
        event_id,
        filename,
        line_number,
        event_key,
        raw_play
    FROM source
)

SELECT * FROM renamed
