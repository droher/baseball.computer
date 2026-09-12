MODEL (
  name main_models.event_observation_pitch,
  kind FULL,
  description 'Per (event_key, dimension) observation ledger for pitch-level covariates. Driven by main_models.event_states_full (every event in 1910-2025), LEFT JOINed to a per-event aggregate of main_models.stg_event_pitch_sequences and to main_models.stg_event_pitch_sequence_status, whose pitch_sequence_resolution_status is carried on every row. When the appearance is Unresolved (the cumulative pitch-sequence history conflicts), the four sequence dimensions carry raw_value NULL, observed_status = contradicted, source_acquisition_status = contradicted, and model_input_eligible = FALSE, so they are distinguishable from ordinary missing coverage. Unavailable appearances have no parsed sequence items and stay missing/null. The exact Retrosheet text stays in the staging status model and never enters raw_value. Atomic dimensions: count_balls, count_strikes, pitch_sequence, pitch_results, strike_types, pitch_count_total. raw_value sources: stg_events.count_balls / count_strikes for the count dims (event_states_full carries them through); STRING_AGG / COUNT over stg_event_pitch_sequences for the sequence dims, filtered through main_seeds.seed_pitch_types (is_pitch flag for pitch_results, category = Strike for strike_types). v1 does NOT emit default_code for the count dims — no upstream signal distinguishes a recorded 0-0 from a defaulted 0-0; documented as a future enhancement. strike_types emits unknown_code when any strike row in the event is the StrikeUnknownType sentinel. source_acquisition_status joins source_acquisition_ledger: count dims map to dimension=event, sequence dims map to dimension=pitch_sequence (both game-wide, team_id IS NULL); the per-event contradicted override for Unresolved sequence dimensions wins over the ledger value. data_error_risk joins on (game_id, field_name=dimension); no current field_name maps to a pitch dimension, so no-op in v1.',
  grain (event_key, dimension),
  columns (
    event_key UINTEGER,
    dimension VARCHAR,
    observed_status VARCHAR,
    sentinel_type VARCHAR,
    raw_value VARCHAR,
    deduced_value VARCHAR,
    source_acquisition_status VARCHAR,
    data_error_risk VARCHAR,
    model_input_eligible BOOLEAN,
    pitch_sequence_resolution_status VARCHAR
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    dimension = 'Atomic pitch dimension: count_balls, count_strikes, pitch_sequence, pitch_results, strike_types, pitch_count_total.',
    observed_status = 'FK to seed_observed_status. observed = raw value present and meaningful; unknown_code = strike_types row contains a StrikeUnknownType sentinel; missing = raw NULL or zero pitch rows for the dimension; contradicted = sequence dimension of an Unresolved appearance. derived not used in v1 (no upstream deduction path for pitch dimensions).',
    sentinel_type = 'Raw-value character: valid_value, null, empty_sequence (kept in enum for parity; not emitted in v1). unknown when strike_types is observed only as StrikeUnknownType. null for the contradicted sequence dimensions of an Unresolved appearance.',
    raw_value = 'Source value serialized as text. count dims: stg_events.count_balls / count_strikes cast to VARCHAR. pitch_sequence: STRING_AGG of sequence_item ORDER BY sequence_id. pitch_results: same agg filtered by seed_pitch_types.is_pitch. strike_types: same agg filtered by seed_pitch_types.category = Strike. pitch_count_total: COUNT(*) of pitch_sequences rows. NULL for every sequence dimension of an Unresolved appearance; the raw Retrosheet text is never serialized here.',
    deduced_value = 'Always NULL in v1 — no upstream deduction path.',
    source_acquisition_status = 'source_acquisition_ledger.source_availability_status; count dims use dimension=event, sequence dims use dimension=pitch_sequence (game-wide, team_id IS NULL). contradicted for the sequence dimensions of an Unresolved appearance regardless of the ledger value.',
    data_error_risk = 'source_data_error_risk_ledger.data_error_class joined on (game_id, field_name=dimension), keeping the most severe class per seed_data_error_class.severity_rank; COALESCE none. No-op in v1.',
    model_input_eligible = 'TRUE when seed_observed_status.is_training_eligible for the row''s observed_status AND source_acquisition_status != not_acquired. Always FALSE for the sequence dimensions of an Unresolved appearance.',
    pitch_sequence_resolution_status = 'stg_event_pitch_sequence_status.pitch_sequence_resolution_status for the event: Resolved, Unavailable, or Unresolved. Carried on every dimension row, including the count dimensions, whose status logic it does not affect.'
  ),
  audits (
    not_null(columns := (event_key, dimension, observed_status, sentinel_type, source_acquisition_status, data_error_risk, model_input_eligible, pitch_sequence_resolution_status)),
    unique_grain(columns := (event_key, dimension)),
    accepted_values(column := dimension, is_in := (
      'count_balls', 'count_strikes', 'pitch_sequence', 'pitch_results',
      'strike_types', 'pitch_count_total'
    )),
    accepted_values(column := sentinel_type, is_in := (
      'null', 'unknown', 'default', 'zero', 'empty_sequence', 'valid_value', 'not_applicable'
    )),
    accepted_values(column := pitch_sequence_resolution_status, is_in := ('Resolved', 'Unavailable', 'Unresolved')),
    unresolved_pitch_dimensions_contradicted(),
    relationships(
      column := observed_status,
      to_model := main_seeds.seed_observed_status,
      to_column := observed_status
    ),
    sentinel_status_consistent(),
    derived_requires_deduced(),
    model_input_eligible_matches_seed()
  )
);

WITH events_in_scope AS (
    SELECT
        e.event_key,
        e.game_id,
        e.season,
        e.count_balls,
        e.count_strikes,
        st.pitch_sequence_resolution_status,
        COALESCE(st.pitch_sequence_resolution_status = 'Unresolved', FALSE) AS is_unresolved
    FROM main_models.event_states_full AS e
    LEFT JOIN main_models.stg_event_pitch_sequence_status AS st USING (event_key)
    WHERE e.season BETWEEN @start_season AND @end_season
),

pitch_agg AS (
    SELECT
        ps.event_key,
        COUNT(*) AS pitch_seq_total,
        STRING_AGG(CAST(ps.sequence_item AS VARCHAR), '' ORDER BY ps.sequence_id) AS pitch_sequence_str,
        STRING_AGG(CAST(ps.sequence_item AS VARCHAR), '' ORDER BY ps.sequence_id)
            FILTER (WHERE spt.is_pitch) AS pitch_results_str,
        STRING_AGG(CAST(ps.sequence_item AS VARCHAR), '' ORDER BY ps.sequence_id)
            FILTER (WHERE spt.category = 'Strike') AS strike_types_str,
        COUNT(*) FILTER (WHERE spt.is_pitch) AS pitches_count,
        COUNT(*) FILTER (WHERE spt.category = 'Strike') AS strike_count,
        BOOL_OR(CAST(ps.sequence_item AS VARCHAR) = 'StrikeUnknownType') AS has_strike_unknown
    FROM main_models.stg_event_pitch_sequences AS ps
    INNER JOIN main_seeds.seed_pitch_types AS spt
        ON CAST(spt.sequence_item AS VARCHAR) = CAST(ps.sequence_item AS VARCHAR)
    GROUP BY ps.event_key
),

acq AS (
    SELECT game_id, dimension AS source_dimension, source_availability_status
    FROM main_models.source_acquisition_ledger
    WHERE dimension IN ('event', 'pitch_sequence') AND team_id IS NULL
),

risk_per_game_field AS (
    SELECT game_id, field_name, data_error_class
    FROM (
        SELECT
            r.game_id,
            r.field_name,
            r.data_error_class,
            ROW_NUMBER() OVER (
                PARTITION BY r.game_id, r.field_name
                ORDER BY s.severity_rank, r.data_error_class
            ) AS severity_row
        FROM main_models.source_data_error_risk_ledger AS r
        INNER JOIN main_seeds.seed_data_error_class AS s USING (data_error_class)
    )
    WHERE severity_row = 1
),

count_balls_dim AS (
    SELECT
        event_key,
        game_id,
        'count_balls' AS dimension,
        'event' AS source_dimension,
        pitch_sequence_resolution_status,
        FALSE AS is_unresolved,
        CAST(count_balls AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE WHEN count_balls IS NULL THEN 'null' ELSE 'valid_value' END AS sentinel_type,
        CASE WHEN count_balls IS NULL THEN 'missing' ELSE 'observed' END AS observed_status
    FROM events_in_scope
),

count_strikes_dim AS (
    SELECT
        event_key,
        game_id,
        'count_strikes' AS dimension,
        'event' AS source_dimension,
        pitch_sequence_resolution_status,
        FALSE AS is_unresolved,
        CAST(count_strikes AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE WHEN count_strikes IS NULL THEN 'null' ELSE 'valid_value' END AS sentinel_type,
        CASE WHEN count_strikes IS NULL THEN 'missing' ELSE 'observed' END AS observed_status
    FROM events_in_scope
),

pitch_sequence_dim AS (
    SELECT
        e.event_key,
        e.game_id,
        'pitch_sequence' AS dimension,
        'pitch_sequence' AS source_dimension,
        e.pitch_sequence_resolution_status,
        e.is_unresolved,
        CASE WHEN e.is_unresolved THEN NULL ELSE pa.pitch_sequence_str END AS raw_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE
            WHEN e.is_unresolved THEN 'null'
            WHEN pa.pitch_seq_total IS NULL THEN 'null'
            WHEN pa.pitch_sequence_str IS NULL OR pa.pitch_sequence_str = '' THEN 'empty_sequence'
            ELSE 'valid_value'
        END AS sentinel_type,
        CASE
            WHEN e.is_unresolved THEN 'contradicted'
            WHEN pa.pitch_seq_total IS NULL THEN 'missing'
            WHEN pa.pitch_sequence_str IS NULL OR pa.pitch_sequence_str = '' THEN 'missing'
            ELSE 'observed'
        END AS observed_status
    FROM events_in_scope AS e
    LEFT JOIN pitch_agg AS pa USING (event_key)
),

pitch_results_dim AS (
    SELECT
        e.event_key,
        e.game_id,
        'pitch_results' AS dimension,
        'pitch_sequence' AS source_dimension,
        e.pitch_sequence_resolution_status,
        e.is_unresolved,
        CASE WHEN e.is_unresolved THEN NULL ELSE pa.pitch_results_str END AS raw_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE
            WHEN e.is_unresolved THEN 'null'
            WHEN COALESCE(pa.pitches_count, 0) = 0 THEN 'null'
            ELSE 'valid_value'
        END AS sentinel_type,
        CASE
            WHEN e.is_unresolved THEN 'contradicted'
            WHEN COALESCE(pa.pitches_count, 0) = 0 THEN 'missing'
            ELSE 'observed'
        END AS observed_status
    FROM events_in_scope AS e
    LEFT JOIN pitch_agg AS pa USING (event_key)
),

strike_types_dim AS (
    SELECT
        e.event_key,
        e.game_id,
        'strike_types' AS dimension,
        'pitch_sequence' AS source_dimension,
        e.pitch_sequence_resolution_status,
        e.is_unresolved,
        CASE WHEN e.is_unresolved THEN NULL ELSE pa.strike_types_str END AS raw_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE
            WHEN e.is_unresolved THEN 'null'
            WHEN COALESCE(pa.strike_count, 0) = 0 THEN 'null'
            WHEN pa.has_strike_unknown THEN 'unknown'
            ELSE 'valid_value'
        END AS sentinel_type,
        CASE
            WHEN e.is_unresolved THEN 'contradicted'
            WHEN COALESCE(pa.strike_count, 0) = 0 THEN 'missing'
            WHEN pa.has_strike_unknown THEN 'unknown_code'
            ELSE 'observed'
        END AS observed_status
    FROM events_in_scope AS e
    LEFT JOIN pitch_agg AS pa USING (event_key)
),

pitch_count_total_dim AS (
    SELECT
        e.event_key,
        e.game_id,
        'pitch_count_total' AS dimension,
        'pitch_sequence' AS source_dimension,
        e.pitch_sequence_resolution_status,
        e.is_unresolved,
        CASE WHEN e.is_unresolved THEN NULL ELSE CAST(pa.pitch_seq_total AS VARCHAR) END AS raw_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE
            WHEN e.is_unresolved THEN 'null'
            WHEN pa.pitch_seq_total IS NULL THEN 'null'
            ELSE 'valid_value'
        END AS sentinel_type,
        CASE
            WHEN e.is_unresolved THEN 'contradicted'
            WHEN pa.pitch_seq_total IS NULL THEN 'missing'
            ELSE 'observed'
        END AS observed_status
    FROM events_in_scope AS e
    LEFT JOIN pitch_agg AS pa USING (event_key)
),

all_dims AS (
    SELECT * FROM count_balls_dim
    UNION ALL BY NAME SELECT * FROM count_strikes_dim
    UNION ALL BY NAME SELECT * FROM pitch_sequence_dim
    UNION ALL BY NAME SELECT * FROM pitch_results_dim
    UNION ALL BY NAME SELECT * FROM strike_types_dim
    UNION ALL BY NAME SELECT * FROM pitch_count_total_dim
),

resolved_acquisition AS (
    SELECT
        d.*,
        CASE
            WHEN d.is_unresolved THEN 'contradicted'
            ELSE COALESCE(acq.source_availability_status, 'not_acquired')
        END AS source_acquisition_status
    FROM all_dims AS d
    LEFT JOIN acq ON acq.game_id = d.game_id AND acq.source_dimension = d.source_dimension
)

SELECT
    d.event_key,
    d.dimension,
    d.observed_status,
    d.sentinel_type,
    d.raw_value,
    d.deduced_value,
    d.source_acquisition_status,
    COALESCE(r.data_error_class, 'none') AS data_error_risk,
    (
        st.is_training_eligible
        AND d.source_acquisition_status != 'not_acquired'
    ) AS model_input_eligible,
    d.pitch_sequence_resolution_status
FROM resolved_acquisition AS d
LEFT JOIN main_seeds.seed_observed_status AS st ON st.observed_status = d.observed_status
LEFT JOIN risk_per_game_field AS r ON r.game_id = d.game_id AND r.field_name = d.dimension
