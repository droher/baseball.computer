MODEL (
  name main_models.stg_event_flags,
  kind FULL,
  description 'Representation of flags that can appear on each event in a Retrosheet file. They are helpful for identifying plays like sacrifices, double plays, and other miscellany. See the Retrosheet event file spec for more info: https://www.retrosheet.org/eventfile.htm',
  grain (event_key, sequence_id),
  columns (
    event_key UINTEGER,
    sequence_id UTINYINT,
    flag VARCHAR
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    sequence_id = @doc('sequence_id'),
    flag = 'Prettified version of the flag as it appeared in the raw play'
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_event_flags.parquet'
  ),
);







WITH source AS (
    SELECT * FROM event.event_flags
),

renamed AS (
    SELECT
        event_key,
        sequence_id,
        flag,

    FROM source
)

SELECT * FROM renamed
