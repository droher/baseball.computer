MODEL (
  name main_models.event_observation_geometry,
  kind FULL,
  description 'Per (event_key, dimension) observation ledger for batted-ball geometry covariates. Driven by main_models.calc_batted_ball_type, which is itself filtered to batted_trajectory IS NOT NULL; the ledger is sparse by design (no row = dimension does not apply to this event). Atomic dimensions: trajectory, location_side, location_depth, location_edge, general_location, ball_handler_position, pulled_opposite. observed_status drawn from seed_observed_status. raw_value reflects the source value verbatim (recorded_* columns from calc_batted_ball_type for the first five dims, raw batted_to_fielder from stg_events for ball_handler_position, NULL for the purely-derived pulled_opposite). deduced_value populated only when observed_status = derived: trajectory uses calc.is_trajectory_deduced; location_side and location_depth populate when calc inferred from the fielder/inference path; pulled_opposite derives from (deduced location_side × resolved batter_hand). sentinel_type encodes the raw value character (null / unknown / valid_value / zero — zero is the batted_to_fielder 0 sentinel for unknown fielder; default / empty_sequence / not_applicable kept in the enum for sibling-ledger parity but not emitted here). source_acquisition_status joins source_acquisition_ledger (dimension = batted_ball, game-wide). data_error_risk joins source_data_error_risk_ledger on (game_id, field_name); no current field_name maps to a geometry dimension, so the JOIN is a no-op in v1 — kept for forward-compat. model_input_eligible = observed_status NOT IN (not_applicable, data_error_prone) AND source_acquisition_status != not_acquired.',
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
    model_input_eligible BOOLEAN
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    dimension = 'Atomic geometry dimension: trajectory, location_side, location_depth, location_edge, general_location, ball_handler_position, pulled_opposite.',
    observed_status = 'FK to seed_observed_status. observed = raw recorded value is meaningful (non-null, non-Unknown, non-zero-fielder); derived = raw missing/Unknown but a deterministic inference is available (only fires for trajectory/location_side/location_depth/pulled_opposite); unknown_code = raw is the Unknown sentinel and no inference is available; missing = raw is NULL and no inference is available.',
    sentinel_type = 'Raw-value character: valid_value, unknown (literal Unknown enum value), null (raw is NULL), zero (batted_to_fielder = 0 sentinel for unknown fielder). default/empty_sequence/not_applicable are sibling-ledger enum values, kept here for cross-ledger parity but not emitted.',
    raw_value = 'Source value serialized as text. recorded_* columns from calc_batted_ball_type for the first five dims, stg_events.batted_to_fielder for ball_handler_position (pre-nullification, so HR/GRD events keep the original 0/NULL marker), NULL for pulled_opposite.',
    deduced_value = 'Deterministic inference serialized as text; non-NULL only when observed_status = derived.',
    source_acquisition_status = 'source_acquisition_ledger.source_availability_status for (game_id, dimension=batted_ball, team_id IS NULL). COALESCE not_acquired when no row exists.',
    data_error_risk = 'source_data_error_risk_ledger.data_error_class joined on (game_id, field_name=dimension); COALESCE none. No-op in v1 (no current field_name maps to a geometry dimension).',
    model_input_eligible = 'TRUE when the (event, dimension) row is admissible to a fitted-model training set: observed_status NOT IN (not_applicable, data_error_prone) AND source_acquisition_status != not_acquired.'
  ),
  audits (
    not_null(columns := (event_key, dimension, observed_status, sentinel_type, source_acquisition_status, data_error_risk, model_input_eligible)),
    unique_grain(columns := (event_key, dimension)),
    accepted_values(column := dimension, is_in := (
      'trajectory', 'location_side', 'location_depth', 'location_edge',
      'general_location', 'ball_handler_position', 'pulled_opposite'
    )),
    accepted_values(column := sentinel_type, is_in := (
      'null', 'unknown', 'default', 'zero', 'empty_sequence', 'valid_value', 'not_applicable'
    )),
    relationships(
      column := observed_status,
      to_model := main_seeds.seed_observed_status,
      to_column := observed_status
    )
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
        b.location_side,
        b.recorded_location_depth,
        b.location_depth,
        b.location_edge,
        b.recorded_location,
        es.batter_hand,
        se.batted_to_fielder AS raw_batted_to_fielder
    FROM main_models.calc_batted_ball_type AS b
    INNER JOIN main_models.event_states_full AS es USING (event_key)
    INNER JOIN main_models.stg_events AS se USING (event_key)
    WHERE es.season BETWEEN @start_season AND @end_season
),

acq_batted_ball AS (
    SELECT game_id, source_availability_status
    FROM main_models.source_acquisition_ledger
    WHERE dimension = 'batted_ball' AND team_id IS NULL
),

risk_per_game_field AS (
    SELECT
        game_id,
        field_name,
        MIN(data_error_class) AS data_error_class
    FROM main_models.source_data_error_risk_ledger
    GROUP BY game_id, field_name
),

trajectory AS (
    SELECT
        event_key,
        game_id,
        'trajectory' AS dimension,
        CAST(recorded_trajectory AS VARCHAR) AS raw_value,
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
        CAST(recorded_location_angle AS VARCHAR) AS raw_value,
        CASE
            WHEN (recorded_location_angle IS NULL OR CAST(recorded_location_angle AS VARCHAR) = 'Unknown')
                 AND location_side IS NOT NULL AND location_side != 'Unknown'
            THEN location_side
            ELSE NULL
        END AS deduced_value,
        CASE
            WHEN recorded_location_angle IS NULL THEN 'null'
            WHEN CAST(recorded_location_angle AS VARCHAR) = 'Unknown' THEN 'unknown'
            ELSE 'valid_value'
        END AS sentinel_type,
        CASE
            WHEN recorded_location_angle IS NOT NULL AND CAST(recorded_location_angle AS VARCHAR) != 'Unknown' THEN 'observed'
            WHEN location_side IS NOT NULL AND location_side != 'Unknown' THEN 'derived'
            WHEN CAST(recorded_location_angle AS VARCHAR) = 'Unknown' THEN 'unknown_code'
            ELSE 'missing'
        END AS observed_status
    FROM events_in_scope
),

location_depth AS (
    SELECT
        event_key,
        game_id,
        'location_depth' AS dimension,
        CAST(recorded_location_depth AS VARCHAR) AS raw_value,
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
        CASE
            WHEN location_side IS NULL OR location_side = 'Unknown' THEN NULL
            WHEN batter_hand IS NULL THEN NULL
            WHEN location_side = 'Center' THEN 'straightaway'
            WHEN (CAST(batter_hand AS VARCHAR) = 'L' AND location_side = 'Right')
                 OR (CAST(batter_hand AS VARCHAR) = 'R' AND location_side = 'Left') THEN 'pulled'
            WHEN (CAST(batter_hand AS VARCHAR) = 'L' AND location_side = 'Left')
                 OR (CAST(batter_hand AS VARCHAR) = 'R' AND location_side = 'Right') THEN 'opposite'
            ELSE NULL
        END AS deduced_value,
        CASE
            WHEN location_side IS NOT NULL AND location_side != 'Unknown' AND batter_hand IS NOT NULL THEN 'valid_value'
            ELSE 'null'
        END AS sentinel_type,
        CASE
            WHEN location_side IS NOT NULL AND location_side != 'Unknown' AND batter_hand IS NOT NULL THEN 'derived'
            ELSE 'missing'
        END AS observed_status
    FROM events_in_scope
),

all_dims AS (
    SELECT * FROM trajectory
    UNION ALL BY NAME SELECT * FROM location_side
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
    d.deduced_value,
    COALESCE(acq.source_availability_status, 'not_acquired') AS source_acquisition_status,
    COALESCE(r.data_error_class, 'none') AS data_error_risk,
    (
        d.observed_status NOT IN ('not_applicable', 'data_error_prone')
        AND COALESCE(acq.source_availability_status, 'not_acquired') != 'not_acquired'
    ) AS model_input_eligible
FROM all_dims AS d
LEFT JOIN acq_batted_ball AS acq ON acq.game_id = d.game_id
LEFT JOIN risk_per_game_field AS r ON r.game_id = d.game_id AND r.field_name = d.dimension
