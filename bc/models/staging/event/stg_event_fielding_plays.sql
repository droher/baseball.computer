MODEL (
  name main_models.stg_event_fielding_plays,
  kind FULL,
  description 'Each fielding play that occurs during an event, in the order in which it occured. Unless you have a specific interest in the order in which the plays occurred on an event, it is recommended that you use `calc_fielding_play_agg` table instead, as there are some quirks in the way that some metrics are calculated that require knowledge of other parts of the event (most notably that a player can be credited with at most one assist on any given putout).',
  grain (event_key, sequence_id),
  columns (
    game_id VARCHAR,
    event_id UTINYINT,
    event_key UINTEGER,
    sequence_id UTINYINT,
    fielding_position UTINYINT,
    fielding_play VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_id = @doc('event_id'),
    event_key = @doc('event_key'),
    sequence_id = @doc('sequence_id'),
    fielding_position = @doc('fielding_position'),
    fielding_play = 'Enum representation of the type of fielding play'
  ),
  audits (
    relationships(column := game_id, to_column := game_id, to_model := main_models.stg_games),
    relationships(column := event_key, to_column := event_key, to_model := main_models.stg_events)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_event_fielding_plays.parquet'
  ),
);







WITH source AS (
    SELECT * FROM event.event_fielding_play
),

renamed AS (
    SELECT
        game_id,
        event_id,
        event_key,
        sequence_id,
        fielding_position,
        fielding_play,

    FROM source
)

SELECT * FROM renamed
