MODEL (
  name main_models.event_observation_credit,
  kind FULL,
  description 'Per (event_key, dimension) observation ledger for fielding-credit covariates. Driven by main_models.event_states_full (every event in 1910-2025). Atomic dimensions: putout_credit, assist_credit, error_credit, double_play_credit, triple_play_credit, passed_ball_credit. Putout/assist/error rows aggregate main_models.calc_fielding_play_agg, splitting known (fielding_position != 0) from unknown (fielding_position = 0) credit-bearer rows; unknown_code fires when any unknown-fielder credit row is present for that dimension. double_play / triple_play join main_models.event_double_plays (event-grain); passed_ball aggregates main_models.stg_event_baserunners filtered to baserunning_play_type = PassedBall. source_acquisition_status joins source_acquisition_ledger on (game_id, fielding_team_id, dimension=box_fielding) — credit data are side-dependent and live in the box_fielding row. data_error_risk joins on (game_id, field_name=dimension); the current source_data_error_risk_ledger field_names (fielding_putouts, fielding_putouts_assists, fielding_putouts_assists_errors) are composites that do not literally match the credit dimension keys, so this is a no-op in v1 — kept for a follow-up enrichment PR that maps composite field_names to per-credit dimensions.',
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
    dimension = 'Atomic fielding-credit dimension: putout_credit, assist_credit, error_credit, double_play_credit, triple_play_credit, passed_ball_credit.',
    observed_status = 'FK to seed_observed_status. observed = credit recorded with known fielder; unknown_code = credit recorded with fielding_position=0 (unknown fielder marker); not_applicable = no credit on this play for the dimension. derived not used in v1.',
    sentinel_type = 'Raw-value character: valid_value, unknown, not_applicable. null / default / zero / empty_sequence kept in enum for sibling-ledger parity but not emitted.',
    raw_value = 'Source value serialized as text. Credit dims: total credit count (known + unknown). DP/TP dims: boolean cast to text. passed_ball: boolean cast to text.',
    deduced_value = 'Always NULL in v1.',
    source_acquisition_status = 'source_acquisition_ledger.source_availability_status for (game_id, fielding_team_id, dimension=box_fielding).',
    data_error_risk = 'source_data_error_risk_ledger.data_error_class joined on (game_id, field_name=dimension); COALESCE none. No-op in v1.',
    model_input_eligible = 'TRUE when observed_status NOT IN (not_applicable, data_error_prone) AND source_acquisition_status != not_acquired.'
  ),
  audits (
    not_null(columns := (event_key, dimension, observed_status, sentinel_type, source_acquisition_status, data_error_risk, model_input_eligible)),
    unique_grain(columns := (event_key, dimension)),
    accepted_values(column := dimension, is_in := (
      'putout_credit', 'assist_credit', 'error_credit',
      'double_play_credit', 'triple_play_credit', 'passed_ball_credit'
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
        event_key,
        game_id,
        season,
        fielding_team_id
    FROM main_models.event_states_full
    WHERE season BETWEEN @start_season AND @end_season
),

fielding_agg AS (
    SELECT
        event_key,
        SUM(CASE WHEN fielding_position != 0 THEN putouts ELSE 0 END) AS known_putouts,
        SUM(CASE WHEN fielding_position = 0 THEN putouts ELSE 0 END) AS unknown_putouts_sum,
        SUM(CASE WHEN fielding_position != 0 THEN assists ELSE 0 END) AS known_assists,
        SUM(CASE WHEN fielding_position = 0 THEN assists ELSE 0 END) AS unknown_assists,
        SUM(CASE WHEN fielding_position != 0 THEN errors ELSE 0 END) AS known_errors,
        SUM(CASE WHEN fielding_position = 0 THEN errors ELSE 0 END) AS unknown_errors
    FROM main_models.calc_fielding_play_agg
    GROUP BY event_key
),

passed_ball_agg AS (
    SELECT
        event_key,
        TRUE AS has_passed_ball
    FROM main_models.stg_event_baserunners
    WHERE CAST(baserunning_play_type AS VARCHAR) = 'PassedBall'
    GROUP BY event_key
),

acq_box_fielding AS (
    SELECT game_id, team_id, source_availability_status
    FROM main_models.source_acquisition_ledger
    WHERE dimension = 'box_fielding'
),

risk_per_game_field AS (
    SELECT
        game_id,
        field_name,
        MIN(data_error_class) AS data_error_class
    FROM main_models.source_data_error_risk_ledger
    GROUP BY game_id, field_name
),

events_with_aggs AS (
    SELECT
        e.event_key,
        e.game_id,
        e.fielding_team_id,
        COALESCE(fa.known_putouts, 0) AS known_putouts,
        COALESCE(fa.unknown_putouts_sum, 0) AS unknown_putouts_sum,
        COALESCE(fa.known_assists, 0) AS known_assists,
        COALESCE(fa.unknown_assists, 0) AS unknown_assists,
        COALESCE(fa.known_errors, 0) AS known_errors,
        COALESCE(fa.unknown_errors, 0) AS unknown_errors,
        COALESCE(dp.is_double_play, FALSE) AS is_double_play,
        COALESCE(dp.is_triple_play, FALSE) AS is_triple_play,
        COALESCE(pb.has_passed_ball, FALSE) AS has_passed_ball
    FROM events_in_scope AS e
    LEFT JOIN fielding_agg AS fa USING (event_key)
    LEFT JOIN main_models.event_double_plays AS dp USING (event_key)
    LEFT JOIN passed_ball_agg AS pb USING (event_key)
),

putout_credit AS (
    SELECT
        event_key,
        game_id,
        fielding_team_id,
        'putout_credit' AS dimension,
        CAST(known_putouts + unknown_putouts_sum AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE
            WHEN unknown_putouts_sum > 0 THEN 'unknown'
            WHEN known_putouts > 0 THEN 'valid_value'
            ELSE 'not_applicable'
        END AS sentinel_type,
        CASE
            WHEN unknown_putouts_sum > 0 THEN 'unknown_code'
            WHEN known_putouts > 0 THEN 'observed'
            ELSE 'not_applicable'
        END AS observed_status
    FROM events_with_aggs
),

assist_credit AS (
    SELECT
        event_key,
        game_id,
        fielding_team_id,
        'assist_credit' AS dimension,
        CAST(known_assists + unknown_assists AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE
            WHEN unknown_assists > 0 THEN 'unknown'
            WHEN known_assists > 0 THEN 'valid_value'
            ELSE 'not_applicable'
        END AS sentinel_type,
        CASE
            WHEN unknown_assists > 0 THEN 'unknown_code'
            WHEN known_assists > 0 THEN 'observed'
            ELSE 'not_applicable'
        END AS observed_status
    FROM events_with_aggs
),

error_credit AS (
    SELECT
        event_key,
        game_id,
        fielding_team_id,
        'error_credit' AS dimension,
        CAST(known_errors + unknown_errors AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE
            WHEN unknown_errors > 0 THEN 'unknown'
            WHEN known_errors > 0 THEN 'valid_value'
            ELSE 'not_applicable'
        END AS sentinel_type,
        CASE
            WHEN unknown_errors > 0 THEN 'unknown_code'
            WHEN known_errors > 0 THEN 'observed'
            ELSE 'not_applicable'
        END AS observed_status
    FROM events_with_aggs
),

double_play_credit AS (
    SELECT
        event_key,
        game_id,
        fielding_team_id,
        'double_play_credit' AS dimension,
        CAST(is_double_play AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE WHEN is_double_play THEN 'valid_value' ELSE 'not_applicable' END AS sentinel_type,
        CASE WHEN is_double_play THEN 'observed' ELSE 'not_applicable' END AS observed_status
    FROM events_with_aggs
),

triple_play_credit AS (
    SELECT
        event_key,
        game_id,
        fielding_team_id,
        'triple_play_credit' AS dimension,
        CAST(is_triple_play AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE WHEN is_triple_play THEN 'valid_value' ELSE 'not_applicable' END AS sentinel_type,
        CASE WHEN is_triple_play THEN 'observed' ELSE 'not_applicable' END AS observed_status
    FROM events_with_aggs
),

passed_ball_credit AS (
    SELECT
        event_key,
        game_id,
        fielding_team_id,
        'passed_ball_credit' AS dimension,
        CAST(has_passed_ball AS VARCHAR) AS raw_value,
        CAST(NULL AS VARCHAR) AS deduced_value,
        CASE WHEN has_passed_ball THEN 'valid_value' ELSE 'not_applicable' END AS sentinel_type,
        CASE WHEN has_passed_ball THEN 'observed' ELSE 'not_applicable' END AS observed_status
    FROM events_with_aggs
),

all_dims AS (
    SELECT * FROM putout_credit
    UNION ALL BY NAME SELECT * FROM assist_credit
    UNION ALL BY NAME SELECT * FROM error_credit
    UNION ALL BY NAME SELECT * FROM double_play_credit
    UNION ALL BY NAME SELECT * FROM triple_play_credit
    UNION ALL BY NAME SELECT * FROM passed_ball_credit
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
LEFT JOIN acq_box_fielding AS acq
    ON acq.game_id = d.game_id AND acq.team_id = d.fielding_team_id
LEFT JOIN risk_per_game_field AS r ON r.game_id = d.game_id AND r.field_name = d.dimension
