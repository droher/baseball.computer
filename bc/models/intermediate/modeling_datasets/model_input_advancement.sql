MODEL (
  name main_models.model_input_advancement,
  kind VIEW,
  description 'Modeling dataset for runner advancement. One row per (event_key, baserunner) from stg_event_baserunners. base_start derived from the baserunner enum (Batter=0, First=1, Second=2, Third=3). Carries observed-class columns for trajectory, location_depth, ball_handler_position from event_observation_geometry. Does NOT pull sacrifice_flies or post-advancement labels — those condition on the target. geometry_posterior_artifact_id and responsibility_artifact_id are reserved for downstream posterior artifacts. Filtered to target_population_status = event_level.',
  grain (event_key, baserunner),
  columns (
    event_key UINTEGER,
    baserunner VARCHAR,
    base_start UTINYINT,
    runner_id VARCHAR,
    trajectory_class VARCHAR,
    trajectory_is_observed BOOLEAN,
    location_depth_class VARCHAR,
    location_depth_is_observed BOOLEAN,
    ball_handler_position_class VARCHAR,
    ball_handler_position_is_observed BOOLEAN,
    geometry_posterior_artifact_id VARCHAR,
    responsibility_artifact_id VARCHAR,
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
    inputter VARCHAR,
    translator VARCHAR,
    affiliated_team TEAM_ID,
    inning_start UTINYINT,
    frame_start FRAME,
    base_state_start UTINYINT,
    outs_start UTINYINT,
    score_margin TINYINT,
    leverage_index DOUBLE,
    leverage_bucket VARCHAR,
    runs_on_play UTINYINT,
    hit_or_out BOOLEAN,
    batter_id VARCHAR,
    pitcher_id VARCHAR,
    batter_hand HAND,
    pitcher_hand HAND,
    batting_team_id TEAM_ID,
    fielding_team_id TEAM_ID,
    personnel_confidence VARCHAR,
    context_confidence VARCHAR,
    exposure_status VARCHAR,
    result_family VARCHAR,
    alignment_regime VARCHAR,
    dl_artifact_id VARCHAR,
    dl_p_class DOUBLE[],
    holdout_flags STRUCT(
      is_heldout_scorer BOOLEAN,
      is_heldout_park BOOLEAN,
      is_heldout_alignment_regime BOOLEAN,
      is_heldout_source_acquisition_block BOOLEAN,
      is_heldout_season_block BOOLEAN,
      is_heldout_aggregate_total BOOLEAN,
      is_heldout_player_group BOOLEAN
    ),
    primary_fold VARCHAR,
    training_weight DOUBLE,
    source_snapshot_id VARCHAR
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    baserunner = 'Baserunner enum from stg_event_baserunners: Batter, First, Second, Third.',
    base_start = 'Numeric base the runner occupies at event start. Batter=0, First=1, Second=2, Third=3.',
    runner_id = 'stg_event_baserunners.runner_id.',
    trajectory_class = 'raw_value for event_observation_geometry where dimension = trajectory. NULL when not recorded; heuristic deductions excluded.',
    trajectory_is_observed = 'observed_status = observed for trajectory. Heuristic deductions do not count.',
    location_depth_class = 'raw_value for event_observation_geometry where dimension = location_depth. NULL when not recorded; heuristic deductions excluded.',
    location_depth_is_observed = 'observed_status = observed for location_depth. Heuristic deductions do not count.',
    ball_handler_position_class = 'raw_value for event_observation_geometry where dimension = ball_handler_position. NULL when not recorded; heuristic deductions excluded.',
    ball_handler_position_is_observed = 'observed_status = observed for ball_handler_position. Heuristic deductions do not count.',
    geometry_posterior_artifact_id = 'NULL — geometry-posterior artifact pointer reserved for downstream.',
    responsibility_artifact_id = 'NULL — fielder-responsibility artifact pointer reserved for downstream.',
    primary_fold = 'Default game-hash split. HASH(game_id) mod 100 -> [0,69]=TRAIN, [70,84]=VALIDATE, [85,99]=TEST.',
    training_weight = '1.0 universally.',
    source_snapshot_id = 'Stamp from the source_snapshot_id var.',
    holdout_flags = 'STRUCT of 7 stress-test holdout BOOLEANs, NULL until stress_holdout_registry materializes the split policy.',
    dl_artifact_id = 'dl_proposal_manifest.dl_artifact_id, NULL until DL supplements land.',
    dl_p_class = 'dl_proposal_manifest.dl_p_class, NULL until DL supplements land.'
  ),
  audits (
    not_null(columns := (event_key, baserunner, base_start, game_id, season, primary_fold, source_snapshot_id)),
    unique_grain(columns := (event_key, baserunner)),
    accepted_values(column := baserunner, is_in := ('Batter', 'First', 'Second', 'Third')),
    accepted_values(column := primary_fold, is_in := ('TRAIN', 'VALIDATE', 'TEST')),
    accepted_values(column := alignment_regime, is_in := (
      'pre_shift_era', 'shift_growth_era', 'full_shift_era', 'post_restriction'
    )),
    accepted_values(column := leverage_bucket, is_in := ('low', 'medium', 'high')),
    relationships(column := event_key, to_model := main_models.event_observation_context, to_column := event_key)
  )
);

WITH br AS (
    SELECT
        event_key,
        baserunner,
        runner_id
    FROM main_models.stg_event_baserunners
),

trj AS (
    SELECT event_key, raw_value, observed_status
    FROM main_models.event_observation_geometry
    WHERE dimension = 'trajectory'
),

dep AS (
    SELECT event_key, raw_value, observed_status
    FROM main_models.event_observation_geometry
    WHERE dimension = 'location_depth'
),

bhp AS (
    SELECT event_key, raw_value, observed_status
    FROM main_models.event_observation_geometry
    WHERE dimension = 'ball_handler_position'
)

SELECT
    br.event_key,
    br.baserunner,
    CASE br.baserunner
        WHEN 'Batter' THEN 0
        WHEN 'First'  THEN 1
        WHEN 'Second' THEN 2
        WHEN 'Third'  THEN 3
    END::UTINYINT AS base_start,
    br.runner_id,
    trj.raw_value AS trajectory_class,
    (trj.observed_status = 'observed') AS trajectory_is_observed,
    dep.raw_value AS location_depth_class,
    (dep.observed_status = 'observed') AS location_depth_is_observed,
    bhp.raw_value AS ball_handler_position_class,
    (bhp.observed_status = 'observed') AS ball_handler_position_is_observed,
    NULL::VARCHAR AS geometry_posterior_artifact_id,
    NULL::VARCHAR AS responsibility_artifact_id,
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
    c.inputter,
    c.translator,
    c.affiliated_team,
    c.inning_start,
    c.frame_start,
    c.base_state_start,
    c.outs_start,
    c.score_margin,
    c.leverage_index,
    c.leverage_bucket,
    c.runs_on_play,
    c.hit_or_out,
    c.batter_id,
    c.pitcher_id,
    c.batter_hand,
    c.pitcher_hand,
    c.batting_team_id,
    c.fielding_team_id,
    c.personnel_confidence,
    c.context_confidence,
    c.exposure_status,
    c.result_family,
    c.alignment_regime,
    p.dl_artifact_id,
    p.dl_p_class,
    STRUCT_PACK(
        is_heldout_scorer := s.is_heldout_scorer,
        is_heldout_park := s.is_heldout_park,
        is_heldout_alignment_regime := s.is_heldout_alignment_regime,
        is_heldout_source_acquisition_block := s.is_heldout_source_acquisition_block,
        is_heldout_season_block := s.is_heldout_season_block,
        is_heldout_aggregate_total := s.is_heldout_aggregate_total,
        is_heldout_player_group := s.is_heldout_player_group
    ) AS holdout_flags,
    CASE
        WHEN (HASH(c.game_id)::HUGEINT % 100) < 70 THEN 'TRAIN'
        WHEN (HASH(c.game_id)::HUGEINT % 100) < 85 THEN 'VALIDATE'
        ELSE 'TEST'
    END AS primary_fold,
    1.0 AS training_weight,
    @VAR('source_snapshot_id', 'dev') AS source_snapshot_id
FROM br
INNER JOIN main_models.event_observation_context AS c USING (event_key)
LEFT JOIN trj USING (event_key)
LEFT JOIN dep USING (event_key)
LEFT JOIN bhp USING (event_key)
LEFT JOIN main_models.dl_advancement_proposal_manifest AS p
    ON p.event_key = br.event_key
    AND p.baserunner = br.baserunner
LEFT JOIN main_models.stress_holdout_registry AS s ON s.event_key = br.event_key
WHERE c.target_population_status = 'event_level'
