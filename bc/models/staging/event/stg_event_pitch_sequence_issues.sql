MODEL (
  name main_models.stg_event_pitch_sequence_issues,
  kind FULL,
  description 'Conflict evidence for plate appearances whose cumulative pitch-sequence history could not be reconciled. One row per conflict found on an event; an event can carry several. Every issue event has status Unresolved in `stg_event_pitch_sequence_status`. The prior and current Retrosheet pitch fields are preserved exactly, including empty strings.',
  grain (event_key, sequence_id),
  depends_on (main_models.stg_event_pitch_sequence_status),
  columns (
    game_id VARCHAR,
    event_id UTINYINT,
    event_key UINTEGER,
    appearance_start_event_id UTINYINT,
    sequence_id UTINYINT,
    reason VARCHAR,
    prior_event_id UTINYINT,
    prior_raw_pitch_sequence VARCHAR,
    current_raw_pitch_sequence VARCHAR,
    date DATE,
    season SMALLINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_id = @doc('event_id'),
    event_key = @doc('event_key'),
    appearance_start_event_id = 'event_id of the first event in the quarantined plate appearance',
    sequence_id = 'Order of the issue within the event, starting at 1',
    reason = 'Structured diagnostic reason: TokenMismatch (the current pitch field does not extend the prior one), CatcherPickoffConflict (a catcher pickoff token contradicts the prior history), or AmbiguousPickoffReplay (a repeated pickoff token cannot be placed in the history)',
    prior_event_id = 'event_id of the earlier event in the appearance whose pitch field conflicts with this one. NULL when there is no prior event.',
    prior_raw_pitch_sequence = 'The exact Retrosheet pitch field of the prior event, unnormalized. An empty string is a real value.',
    current_raw_pitch_sequence = 'The exact Retrosheet pitch field of this event, unnormalized. An empty string is a real value.',
    date = @doc('date'),
    season = @doc('season')
  ),
  audits (
    not_null(columns := (game_id, event_id, event_key, appearance_start_event_id, sequence_id, reason, prior_raw_pitch_sequence, current_raw_pitch_sequence)),
    unique_grain(columns := (event_key, sequence_id)),
    accepted_values(column := reason, is_in := ('TokenMismatch', 'CatcherPickoffConflict', 'AmbiguousPickoffReplay')),
    issue_events_unresolved(),
    relationships(column := game_id, to_column := game_id, to_model := main_models.stg_games),
    relationships(column := event_key, to_column := event_key, to_model := main_models.stg_events)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_event_pitch_sequence_issues.parquet'
  ),
);







WITH source AS (
    SELECT * FROM event.event_pitch_sequence_issues
),

renamed AS (
    SELECT
        game_id,
        event_id,
        event_key,
        appearance_start_event_id,
        sequence_id,
        reason,
        prior_event_id,
        prior_raw_pitch_sequence,
        current_raw_pitch_sequence,
        STRPTIME(SUBSTRING(game_id, 4, 8), '%Y%m%d')::DATE AS date,
        SUBSTRING(game_id, 4, 4)::INT2 AS season,

    FROM source
)

SELECT * FROM renamed
