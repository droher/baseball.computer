MODEL (
  name main_models.event_observation_geometry,
  kind FULL,
  description 'Per (event_key, dimension) observation ledger for batted-ball geometry covariates. Location-side provenance keeps the recorded general-location token in raw_value, its seed-taxonomy global-side class in mapped_value, and any fielder fallback in deduced_value. The separate location_angle dimension preserves the recorded within-zone modifier; its ambiguous Default sentinel is not training truth.',
  grain (event_key, dimension),
  columns (
    event_key UINTEGER,
    dimension VARCHAR,
    observed_status VARCHAR,
    sentinel_type VARCHAR,
    raw_value VARCHAR,
    mapped_value VARCHAR,
    deduced_value VARCHAR,
    value_origin VARCHAR,
    source_acquisition_status VARCHAR,
    data_error_risk VARCHAR,
    model_input_eligible BOOLEAN
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    dimension = 'Atomic geometry dimension: trajectory, location_side, location_angle, location_depth, location_edge, general_location, ball_handler_position, pulled_opposite.',
    observed_status = 'FK to seed_observed_status. Direct source truth is observed, deterministic fallback is derived, and null/Unknown/Default source states retain distinct non-training statuses.',
    sentinel_type = 'Raw-value character: valid_value, unknown, default, null, or zero. location_angle emits default for its ambiguous Default modifier.',
    raw_value = 'Source value serialized as text. For location_side this is the recorded general-location token, not its mapped side class.',
    mapped_value = 'Deterministic seed-taxonomy class derived only from raw_value. Populated for observed location_side as the global category_side; All is the broad Catcher region and does not assert a directional side. NULL for other dimensions.',
    deduced_value = 'Deterministic inference serialized as text; non-NULL only when observed_status = derived. pulled_opposite domain is exactly pulled / opposite / middle.',
    value_origin = 'Provenance of the usable or sentinel value: source_raw, source_mapped, deterministic_deduction, source_sentinel, or source_missing.',
    source_acquisition_status = 'source_acquisition_ledger.source_availability_status for (game_id, dimension=batted_ball, team_id IS NULL). COALESCE not_acquired when no row exists.',
    data_error_risk = 'source_data_error_risk_ledger.data_error_class joined on (game_id, field_name=dimension), keeping the most severe class per seed_data_error_class.severity_rank; COALESCE none. No-op in v1 (no current field_name maps to a geometry dimension).',
    model_input_eligible = 'TRUE when the (event, dimension) row is admissible to a fitted-model training set: seed_observed_status.is_training_eligible for the row''s observed_status AND source_acquisition_status != not_acquired.'
  ),
  audits (
    not_null(columns := (event_key, dimension, observed_status, sentinel_type, source_acquisition_status, data_error_risk, model_input_eligible)),
    unique_grain(columns := (event_key, dimension)),
    accepted_values(column := dimension, is_in := (
      'trajectory', 'location_side', 'location_angle', 'location_depth', 'location_edge',
      'general_location', 'ball_handler_position', 'pulled_opposite'
    )),
    accepted_values(column := sentinel_type, is_in := (
      'null', 'unknown', 'default', 'zero', 'empty_sequence', 'valid_value', 'not_applicable'
    )),
    accepted_values(column := value_origin, is_in := (
      'source_raw', 'source_mapped', 'deterministic_deduction', 'source_sentinel', 'source_missing'
    )),
    relationships(
      column := observed_status,
      to_model := main_seeds.seed_observed_status,
      to_column := observed_status
    ),
    sentinel_status_consistent(),
    derived_requires_deduced(),
    deduced_value_in_domain(dimension := 'pulled_opposite', allowed := ('pulled', 'opposite', 'middle')),
    model_input_eligible_matches_seed()
  )
);

WITH events_in_scope AS (
    SELECT
        b.event_key,
        b.game_id,
        es.season,
        b.recorded_trajectory,
        b.trajectory,
        b.is_trajectory_deduced,
        b.recorded_location_angle,
        b.location_side AS inferred_location_side,
        b.recorded_location_depth,
        b.location_depth,
        b.location_edge,
        b.recorded_location,
        direct_location.category_side AS recorded_location_side,
        es.batter_hand,
        se.batted_to_fielder AS raw_batted_to_fielder
    FROM main_models.calc_batted_ball_type AS b
    INNER JOIN main_models.event_states_full AS es USING (event_key)
    INNER JOIN main_models.stg_events AS se USING (event_key)
    LEFT JOIN main_seeds.seed_hit_location_categories AS direct_location
        ON direct_location.batted_location_general = b.recorded_location
    WHERE es.season BETWEEN @start_season AND @end_season
),

acq_batted_ball AS (
    SELECT game_id, source_availability_status
    FROM main_models.source_acquisition_ledger
    WHERE dimension = 'batted_ball' AND team_id IS NULL
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

trajectory AS (
    SELECT
        event_key,
        game_id,
        'trajectory' AS dimension,
        CAST(recorded_trajectory AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS mapped_value,
        CASE
            WHEN is_trajectory_deduced AND trajectory IS NOT NULL AND CAST(trajectory AS VARCHAR) != 'Unknown'
            THEN CAST(trajectory AS VARCHAR)
            ELSE NULL
        END AS deduced_value,
        CASE
            WHEN recorded_trajectory IS NULL THEN 'null'
            WHEN CAST(recorded_trajectory AS VARCHAR) = 'Unknown' THEN 'unknown'
            ELSE 'valid_value'
        END AS sentinel_type,
        CASE
            WHEN recorded_trajectory IS NOT NULL AND CAST(recorded_trajectory AS VARCHAR) != 'Unknown' THEN 'observed'
            WHEN is_trajectory_deduced AND trajectory IS NOT NULL AND CAST(trajectory AS VARCHAR) != 'Unknown' THEN 'derived'
            WHEN CAST(recorded_trajectory AS VARCHAR) = 'Unknown' THEN 'unknown_code'
            ELSE 'missing'
        END AS observed_status
    FROM events_in_scope
),

location_side AS (
    SELECT
        event_key,
        game_id,
        'location_side' AS dimension,
        CAST(recorded_location AS VARCHAR) AS raw_value,
        recorded_location_side AS mapped_value,
        CASE
            WHEN (recorded_location IS NULL OR CAST(recorded_location AS VARCHAR) = 'Unknown')
                 AND inferred_location_side IS NOT NULL AND inferred_location_side != 'Unknown'
            THEN inferred_location_side
            ELSE NULL
        END AS deduced_value,
        CASE
            WHEN recorded_location IS NULL THEN 'null'
            WHEN CAST(recorded_location AS VARCHAR) = 'Unknown' THEN 'unknown'
            ELSE 'valid_value'
        END AS sentinel_type,
        CASE
            WHEN recorded_location_side IS NOT NULL THEN 'observed'
            WHEN (recorded_location IS NULL OR CAST(recorded_location AS VARCHAR) = 'Unknown')
                 AND inferred_location_side IS NOT NULL AND inferred_location_side != 'Unknown' THEN 'derived'
            WHEN CAST(recorded_location AS VARCHAR) = 'Unknown' THEN 'unknown_code'
            ELSE 'missing'
        END AS observed_status
    FROM events_in_scope
),

location_angle AS (
    SELECT
        event_key,
        game_id,
        'location_angle' AS dimension,
        CAST(recorded_location_angle AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS mapped_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE
            WHEN recorded_location_angle IS NULL THEN 'null'
            WHEN CAST(recorded_location_angle AS VARCHAR) = 'Unknown' THEN 'unknown'
            WHEN CAST(recorded_location_angle AS VARCHAR) = 'Default' THEN 'default'
            ELSE 'valid_value'
        END AS sentinel_type,
        CASE
            WHEN recorded_location_angle IS NULL THEN 'missing'
            WHEN CAST(recorded_location_angle AS VARCHAR) = 'Unknown' THEN 'unknown_code'
            WHEN CAST(recorded_location_angle AS VARCHAR) = 'Default' THEN 'default_code'
            ELSE 'observed'
        END AS observed_status
    FROM events_in_scope
),

location_depth AS (
    SELECT
        event_key,
        game_id,
        'location_depth' AS dimension,
        CAST(recorded_location_depth AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS mapped_value,
        CASE
            WHEN (recorded_location_depth IS NULL OR CAST(recorded_location_depth AS VARCHAR) = 'Unknown')
                 AND location_depth IS NOT NULL AND location_depth != 'Unknown'
            THEN location_depth
            ELSE NULL
        END AS deduced_value,
        CASE
            WHEN recorded_location_depth IS NULL THEN 'null'
            WHEN CAST(recorded_location_depth AS VARCHAR) = 'Unknown' THEN 'unknown'
            ELSE 'valid_value'
        END AS sentinel_type,
        CASE
            WHEN recorded_location_depth IS NOT NULL AND CAST(recorded_location_depth AS VARCHAR) != 'Unknown' THEN 'observed'
            WHEN location_depth IS NOT NULL AND location_depth != 'Unknown' THEN 'derived'
            WHEN CAST(recorded_location_depth AS VARCHAR) = 'Unknown' THEN 'unknown_code'
            ELSE 'missing'
        END AS observed_status
    FROM events_in_scope
),

location_edge AS (
    SELECT
        event_key,
        game_id,
        'location_edge' AS dimension,
        location_edge AS raw_value,
        CAST(NULL AS VARCHAR) AS mapped_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE
            WHEN location_edge IS NULL THEN 'null'
            WHEN location_edge = 'Unknown' THEN 'unknown'
            ELSE 'valid_value'
        END AS sentinel_type,
        CASE
            WHEN location_edge IS NOT NULL AND location_edge != 'Unknown' THEN 'observed'
            WHEN location_edge = 'Unknown' THEN 'unknown_code'
            ELSE 'missing'
        END AS observed_status
    FROM events_in_scope
),

general_location AS (
    SELECT
        event_key,
        game_id,
        'general_location' AS dimension,
        CAST(recorded_location AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS mapped_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE
            WHEN recorded_location IS NULL THEN 'null'
            WHEN CAST(recorded_location AS VARCHAR) = 'Unknown' THEN 'unknown'
            ELSE 'valid_value'
        END AS sentinel_type,
        CASE
            WHEN recorded_location IS NOT NULL AND CAST(recorded_location AS VARCHAR) != 'Unknown' THEN 'observed'
            WHEN CAST(recorded_location AS VARCHAR) = 'Unknown' THEN 'unknown_code'
            ELSE 'missing'
        END AS observed_status
    FROM events_in_scope
),

ball_handler_position AS (
    SELECT
        event_key,
        game_id,
        'ball_handler_position' AS dimension,
        CAST(raw_batted_to_fielder AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS mapped_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE
            WHEN raw_batted_to_fielder IS NULL THEN 'null'
            WHEN raw_batted_to_fielder = 0 THEN 'zero'
            ELSE 'valid_value'
        END AS sentinel_type,
        CASE
            WHEN raw_batted_to_fielder BETWEEN 1 AND 9 THEN 'observed'
            WHEN raw_batted_to_fielder = 0 THEN 'unknown_code'
            ELSE 'missing'
        END AS observed_status
    FROM events_in_scope
),

pulled_opposite AS (
    SELECT
        event_key,
        game_id,
        'pulled_opposite' AS dimension,
        CAST(NULL AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS mapped_value,
        deduced_value,
        'null' AS sentinel_type,
        CASE WHEN deduced_value IS NOT NULL THEN 'derived' ELSE 'missing' END AS observed_status
    FROM (
        SELECT
            event_key,
            game_id,
            CASE
                WHEN inferred_location_side = 'Middle' THEN 'middle'
                WHEN (CAST(batter_hand AS VARCHAR) = 'L' AND inferred_location_side = 'Right')
                     OR (CAST(batter_hand AS VARCHAR) = 'R' AND inferred_location_side = 'Left') THEN 'pulled'
                WHEN (CAST(batter_hand AS VARCHAR) = 'L' AND inferred_location_side = 'Left')
                     OR (CAST(batter_hand AS VARCHAR) = 'R' AND inferred_location_side = 'Right') THEN 'opposite'
                ELSE NULL
            END AS deduced_value
        FROM events_in_scope
    )
),

all_dims AS (
    SELECT * FROM trajectory
    UNION ALL BY NAME SELECT * FROM location_side
    UNION ALL BY NAME SELECT * FROM location_angle
    UNION ALL BY NAME SELECT * FROM location_depth
    UNION ALL BY NAME SELECT * FROM location_edge
    UNION ALL BY NAME SELECT * FROM general_location
    UNION ALL BY NAME SELECT * FROM ball_handler_position
    UNION ALL BY NAME SELECT * FROM pulled_opposite
)

SELECT
    d.event_key,
    d.dimension,
    d.observed_status,
    d.sentinel_type,
    d.raw_value,
    d.mapped_value,
    d.deduced_value,
    CASE
        WHEN d.observed_status = 'derived' THEN 'deterministic_deduction'
        WHEN d.mapped_value IS NOT NULL THEN 'source_mapped'
        WHEN d.observed_status = 'observed' THEN 'source_raw'
        WHEN d.raw_value IS NULL THEN 'source_missing'
        ELSE 'source_sentinel'
    END AS value_origin,
    COALESCE(acq.source_availability_status, 'not_acquired') AS source_acquisition_status,
    COALESCE(r.data_error_class, 'none') AS data_error_risk,
    (
        st.is_training_eligible
        AND COALESCE(acq.source_availability_status, 'not_acquired') != 'not_acquired'
    ) AS model_input_eligible
FROM all_dims AS d
LEFT JOIN main_seeds.seed_observed_status AS st ON st.observed_status = d.observed_status
LEFT JOIN acq_batted_ball AS acq ON acq.game_id = d.game_id
LEFT JOIN risk_per_game_field AS r ON r.game_id = d.game_id AND r.field_name = d.dimension
