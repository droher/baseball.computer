MODEL (
  name main_models.stg_event_pitch_sequence_status,
  kind FULL,
  description 'Pitch-sequence resolution status for every play-by-play event, one row per event. The parser reconciles each plate appearance''s cumulative pitch-sequence history across its events; every event in the same appearance shares one status. Resolved means the history reconciled (a resolved empty sequence is a trusted zero pitches, not missing data). Unavailable means the source carried no parsed sequence items. Unresolved means the appearance''s history conflicted and the parser quarantined it: `stg_event_pitch_sequences` holds no rows for its events and its normalized pitch counters are unknown. The exact Retrosheet pitch field is preserved here, including empty strings. See `stg_event_pitch_sequence_issues` for the conflict evidence.',
  grain (event_key),
  columns (
    game_id VARCHAR,
    event_id UTINYINT,
    event_key UINTEGER,
    appearance_start_event_id UTINYINT,
    pitch_sequence_resolution_status VARCHAR,
    raw_pitch_sequence VARCHAR,
    date DATE,
    season SMALLINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_id = @doc('event_id'),
    event_key = @doc('event_key'),
    appearance_start_event_id = 'event_id of the first event in the plate appearance this event belongs to. Every event sharing a (game_id, appearance_start_event_id) pair has the same status.',
    pitch_sequence_resolution_status = @doc('pitch_sequence_resolution_status'),
    raw_pitch_sequence = 'The exact pitch-sequence field from the Retrosheet play record, unnormalized. An empty string is a real value.',
    date = @doc('date'),
    season = @doc('season')
  ),
  audits (
    not_null(columns := (game_id, event_id, event_key, appearance_start_event_id, pitch_sequence_resolution_status, raw_pitch_sequence)),
    unique_values(columns := (event_key)),
    accepted_values(column := pitch_sequence_resolution_status, is_in := ('Resolved', 'Unavailable', 'Unresolved')),
    appearance_status_consistent(),
    relationships(column := game_id, to_column := game_id, to_model := main_models.stg_games),
    relationships(column := event_key, to_column := event_key, to_model := main_models.stg_events)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_event_pitch_sequence_status.parquet'
  ),
);







WITH source AS (
    SELECT * FROM event.event_pitch_sequence_status
),

renamed AS (
    SELECT
        game_id,
        event_id,
        event_key,
        appearance_start_event_id,
        status AS pitch_sequence_resolution_status,
        raw_pitch_sequence,
        STRPTIME(SUBSTRING(game_id, 4, 8), '%Y%m%d')::DATE AS date,
        SUBSTRING(game_id, 4, 4)::INT2 AS season,

    FROM source
)

SELECT * FROM renamed
