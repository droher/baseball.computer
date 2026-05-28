MODEL (
  name main_models.model_input_responsibility,
  kind VIEW,
  description 'Modeling dataset for fielding responsibility (analytical opportunity model, not official credit). One row per event_key restricted to clean range plays: batted balls whose ball_handler_position is observed and in 3..9 (pitcher and catcher excluded). The recorded handler position is the responsibility proxy (label). Carries observed batted-ball geometry classes (trajectory, location_side, location_depth, location_edge) as covariates and alignment_normal_prior (era-normal alignment basis; alignment_actual_post stays NULL until Model K shift propensity is identified). Filtered to target_population_status = event_level.',
  grain (event_key),
  columns (
    event_key UINTEGER,
    ball_handler_position UTINYINT,
    trajectory_class VARCHAR,
    location_side_class VARCHAR,
    location_depth_class VARCHAR,
    location_edge_class VARCHAR,
    game_id VARCHAR,
    season SMALLINT,
    league VARCHAR,
    game_type GAME_TYPE,
    source_type VARCHAR,
    source_family VARCHAR,
    target_population_status VARCHAR,
    park_id PARK_ID,
    park_episode_status VARCHAR,
    scorer VARCHAR,
    inning_start UTINYINT,
    frame_start FRAME,
    base_state_start UTINYINT,
    outs_start UTINYINT,
    score_margin TINYINT,
    leverage_bucket VARCHAR,
    batter_hand HAND,
    pitcher_hand HAND,
    batting_team_id TEAM_ID,
    fielding_team_id TEAM_ID,
    personnel_confidence VARCHAR,
    context_confidence VARCHAR,
    exposure_status VARCHAR,
    result_family VARCHAR,
    alignment_regime VARCHAR,
    alignment_normal_prior VARCHAR,
    alignment_actual_post VARCHAR,
    primary_fold VARCHAR,
    time_forward_fold VARCHAR,
    training_weight DOUBLE,
    source_snapshot_id VARCHAR
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    ball_handler_position = 'Recorded fielder position (3..9) that handled the batted ball, from event_observation_geometry dimension = ball_handler_position with observed_status = observed. The responsibility-model label. Pitcher (1) and catcher (2) are excluded from this range-style model.',
    trajectory_class = 'raw_value for event_observation_geometry where dimension = trajectory and observed_status = observed. NULL when not recorded.',
    location_side_class = 'raw_value for event_observation_geometry where dimension = location_side and observed_status = observed. NULL when not recorded.',
    location_depth_class = 'raw_value for event_observation_geometry where dimension = location_depth and observed_status = observed. NULL when not recorded.',
    location_edge_class = 'raw_value for event_observation_geometry where dimension = location_edge and observed_status = observed. NULL when not recorded.',
    alignment_normal_prior = 'Era-normal alignment basis (the alignment_regime value). The fallback alignment input the responsibility model conditions on while Model K shift propensity is unavailable.',
    alignment_actual_post = 'NULL — reserved for the Model K shift-propensity posterior alignment input, which has no source data yet.',
    primary_fold = 'Default game-hash split. HASH(game_id) mod 100 -> [0,69]=TRAIN, [70,84]=VALIDATE, [85,99]=TEST.',
    time_forward_fold = 'Eval-only time-forward split. season <= 2022 -> TRAIN, season = 2023 -> VALIDATE, season >= 2024 -> TEST.',
    training_weight = '1.0 universally.',
    source_snapshot_id = 'Stamp from the source_snapshot_id var.'
  ),
  audits (
    not_null(columns := (event_key, ball_handler_position, game_id, season, primary_fold, source_snapshot_id)),
    unique_grain(columns := (event_key)),
    accepted_values(column := ball_handler_position, is_in := (3, 4, 5, 6, 7, 8, 9)),
    accepted_values(column := primary_fold, is_in := ('TRAIN', 'VALIDATE', 'TEST')),
    accepted_values(column := time_forward_fold, is_in := ('TRAIN', 'VALIDATE', 'TEST')),
    accepted_values(column := alignment_regime, is_in := (
      'pre_shift_era', 'shift_growth_era', 'full_shift_era', 'post_restriction'
    )),
    relationships(column := event_key, to_model := main_models.event_observation_context, to_column := event_key)
  )
);

WITH handler AS (
    SELECT event_key, raw_value
    FROM main_models.event_observation_geometry
    WHERE dimension = 'ball_handler_position'
      AND observed_status = 'observed'
      AND TRY_CAST(raw_value AS INTEGER) BETWEEN 3 AND 9
),

trj AS (
    SELECT event_key, raw_value
    FROM main_models.event_observation_geometry
    WHERE dimension = 'trajectory' AND observed_status = 'observed'
),

side AS (
    SELECT event_key, raw_value
    FROM main_models.event_observation_geometry
    WHERE dimension = 'location_side' AND observed_status = 'observed'
),

dep AS (
    SELECT event_key, raw_value
    FROM main_models.event_observation_geometry
    WHERE dimension = 'location_depth' AND observed_status = 'observed'
),

edge AS (
    SELECT event_key, raw_value
    FROM main_models.event_observation_geometry
    WHERE dimension = 'location_edge' AND observed_status = 'observed'
)

SELECT
    handler.event_key,
    TRY_CAST(handler.raw_value AS INTEGER)::UTINYINT AS ball_handler_position,
    trj.raw_value AS trajectory_class,
    side.raw_value AS location_side_class,
    dep.raw_value AS location_depth_class,
    edge.raw_value AS location_edge_class,
    c.game_id,
    c.season,
    c.league,
    c.game_type,
    c.source_type,
    c.source_family,
    c.target_population_status,
    c.park_id,
    c.park_episode_status,
    c.scorer,
    c.inning_start,
    c.frame_start,
    c.base_state_start,
    c.outs_start,
    c.score_margin,
    c.leverage_bucket,
    c.batter_hand,
    c.pitcher_hand,
    c.batting_team_id,
    c.fielding_team_id,
    c.personnel_confidence,
    c.context_confidence,
    c.exposure_status,
    c.result_family,
    c.alignment_regime,
    c.alignment_regime AS alignment_normal_prior,
    NULL::VARCHAR AS alignment_actual_post,
    CASE
        WHEN (HASH(c.game_id)::HUGEINT % 100) < 70 THEN 'TRAIN'
        WHEN (HASH(c.game_id)::HUGEINT % 100) < 85 THEN 'VALIDATE'
        ELSE 'TEST'
    END AS primary_fold,
    CASE
        WHEN c.season <= 2022 THEN 'TRAIN'
        WHEN c.season = 2023 THEN 'VALIDATE'
        ELSE 'TEST'
    END AS time_forward_fold,
    1.0 AS training_weight,
    @VAR('source_snapshot_id', 'dev') AS source_snapshot_id
FROM handler
INNER JOIN main_models.event_observation_context AS c USING (event_key)
LEFT JOIN trj USING (event_key)
LEFT JOIN side USING (event_key)
LEFT JOIN dep USING (event_key)
LEFT JOIN edge USING (event_key)
WHERE c.target_population_status = 'event_level'
