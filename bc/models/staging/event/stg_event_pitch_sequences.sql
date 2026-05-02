MODEL (
  name main_models.stg_event_pitch_sequences,
  kind FULL,
  description 'Pitch-by-pitch sequence for events where we have pitch-level information. A pitch sequence includes ball/strike info, as well as items that are not themselves pitches, such as pickoff attempts and balls blocked in the dirt. Pitch data is very well populated from the 90s on, but is spotty before that. Presence of pitch data for one event does not guarantee that it will be present for other events in the same game. Non-pitch items have sporadic coverage, so their absence is not necessarily an indicator of real-world absence.',
  grain (event_key, sequence_id),
  columns (
    game_id VARCHAR,
    event_id UTINYINT,
    event_key UINTEGER,
    sequence_id UTINYINT,
    sequence_item PITCH_SEQUENCE_ITEM,
    runners_going_flag BOOLEAN,
    blocked_by_catcher_flag BOOLEAN,
    catcher_pickoff_attempt_at_base BASE,
    date DATE,
    season SMALLINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_id = @doc('event_id'),
    event_key = @doc('event_key'),
    sequence_id = @doc('sequence_id'),
    sequence_item = 'See `seed_pitch_types` for metadata on each item',
    runners_going_flag = 'True if any runners are attempting to advance during the pitch',
    blocked_by_catcher_flag = 'True if the pitch was blocked by the catcher.',
    catcher_pickoff_attempt_at_base = 'The base at which the catcher attempted a pickoff after the pitch, if applicable',
    date = @doc('date'),
    season = @doc('season')
  ),
  audits (
    relationships(column := game_id, to_column := game_id, to_model := main_models.stg_games),
    relationships(column := event_key, to_column := event_key, to_model := main_models.stg_events)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_event_pitch_sequences.parquet'
  ),
);







WITH source AS (
    SELECT * FROM event.event_pitch_sequences
),

renamed AS (
    SELECT
        game_id,
        event_id,
        event_key,
        sequence_id,
        sequence_item,
        runners_going_flag,
        blocked_by_catcher_flag,
        catcher_pickoff_attempt_at_base,
        STRPTIME(SUBSTRING(game_id, 4, 8), '%Y%m%d')::DATE AS date,
        SUBSTRING(game_id, 4, 4)::INT2 AS season,

    FROM source
)

SELECT * FROM renamed
