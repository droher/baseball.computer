MODEL (
  name main_models.model_input_event_universe,
  kind VIEW,
  description 'Event-universe modeling dataset for shared entity-embedding pretraining (v2). One row per event_key. Driver = event_observation_context filtered to target_population_status = event_level. Inputs: high-card player slots (batter, pitcher, 8 fielder positions, 3 runner slots), park, scorer; low-card categoricals incl. weather/game-context from stg_games; numerics incl. count + score-margin + temperature + wind + day_of_year. Pretext targets: pa_result, result_family, hit_or_out, outs_on_play_capped, runs_on_play_capped, trajectory_remapped, r1/r2/r3_advancement (7-class baserunner advancement), batted_location_general/depth/edge, batted_to_fielder_class. NULL-when-unobserved; per-head NULL-masking handles the rest.',
  grain (event_key),
  columns (
    event_key UINTEGER,
    game_id VARCHAR,
    season SMALLINT,
    league VARCHAR,
    game_type GAME_TYPE,
    source_family VARCHAR,
    park_id PARK_ID,
    scorer VARCHAR,
    batter_id VARCHAR,
    pitcher_id VARCHAR,
    fielder_pos_2 VARCHAR,
    fielder_pos_3 VARCHAR,
    fielder_pos_4 VARCHAR,
    fielder_pos_5 VARCHAR,
    fielder_pos_6 VARCHAR,
    fielder_pos_7 VARCHAR,
    fielder_pos_8 VARCHAR,
    fielder_pos_9 VARCHAR,
    runner_on_1b_id VARCHAR,
    runner_on_2b_id VARCHAR,
    runner_on_3b_id VARCHAR,
    inning_start UTINYINT,
    frame_start FRAME,
    base_state_start UTINYINT,
    outs_start UTINYINT,
    score_margin TINYINT,
    count_balls UTINYINT,
    count_strikes UTINYINT,
    alignment_regime VARCHAR,
    personnel_confidence VARCHAR,
    context_confidence VARCHAR,
    time_of_day VARCHAR,
    doubleheader_status VARCHAR,
    precipitation VARCHAR,
    sky VARCHAR,
    wind_direction VARCHAR,
    field_condition VARCHAR,
    temperature_fahrenheit TINYINT,
    wind_speed_mph UTINYINT,
    day_of_year SMALLINT,
    result_family VARCHAR,
    pa_result PLATE_APPEARANCE_RESULT,
    hit_or_out BOOLEAN,
    outs_on_play_capped UTINYINT,
    runs_on_play_capped UTINYINT,
    trajectory_remapped VARCHAR,
    r1_advancement VARCHAR,
    r2_advancement VARCHAR,
    r3_advancement VARCHAR,
    batted_location_general VARCHAR,
    batted_location_depth VARCHAR,
    batted_location_edge VARCHAR,
    batted_to_fielder_class VARCHAR,
    primary_fold VARCHAR,
    time_forward_fold VARCHAR,
    training_weight DOUBLE,
    source_snapshot_id VARCHAR
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    primary_fold = 'Default game-hash split. HASH(game_id) mod 100 -> [0,69]=TRAIN, [70,84]=VALIDATE, [85,99]=TEST.',
    time_forward_fold = 'Eval-only time-forward split. season <= 2022 -> TRAIN, season = 2023 -> VALIDATE, season >= 2024 -> TEST. Used to measure embedding generalization to unseen seasons.',
    training_weight = 'Always 1.0 for pretrain — per-head NULL masking applied at fit time (NULL pretext target -> sample_weight=0 for that head only).',
    source_snapshot_id = 'Stamp from the source_snapshot_id var.',
    pa_result = 'stg_events.plate_appearance_result. Present for plate-appearance events.',
    result_family = 'event_observation_context.result_family (9-class enum).',
    hit_or_out = 'event_observation_context.hit_or_out.',
    outs_on_play_capped = 'event_states_full.outs_on_play clamped to [0,3].',
    runs_on_play_capped = 'event_observation_context.runs_on_play clamped to [0,4].',
    trajectory_remapped = '5-class trajectory {Fly, GroundBall, LineDrive, PopUp, Bunt}; NULL when trajectory was not directly recorded. Heuristically-deduced trajectories (HR->Fly, OF putout->AirBall, etc.) are excluded from this column so pretrain heads train only on truly-observed labels.',
    r1_advancement = '7-class baserunner advancement for runner on 1B: {Stayed, Advanced1, Advanced2, Scored, OutAdvancing, OutCaughtStealing, OutPickoff}. NULL when 1B unoccupied at event start.',
    r2_advancement = 'Same 7-class advancement for runner on 2B.',
    r3_advancement = 'Same 7-class advancement for runner on 3B.',
    batted_location_general = 'event_observation_geometry.dimension=general_location raw_value when observed_status=observed; NULL otherwise. Heuristic deductions excluded.',
    batted_location_depth = 'event_observation_geometry.dimension=location_depth raw_value when observed_status=observed; NULL otherwise. Heuristic deductions excluded.',
    batted_location_edge = 'event_observation_geometry.dimension=location_edge raw_value when observed_status=observed; NULL otherwise. Heuristic deductions excluded.',
    batted_to_fielder_class = 'event_observation_geometry.dimension=ball_handler_position raw_value when observed_status=observed; NULL otherwise. Heuristic deductions excluded.'
  ),
  audits (
    not_null(columns := (event_key, game_id, season, primary_fold, time_forward_fold, source_snapshot_id, source_family)),
    unique_grain(columns := (event_key)),
    accepted_values(column := primary_fold, is_in := ('TRAIN', 'VALIDATE', 'TEST')),
    accepted_values(column := time_forward_fold, is_in := ('TRAIN', 'VALIDATE', 'TEST')),
    accepted_values(column := result_family, is_in := (
      'hit', 'out_in_play', 'strikeout', 'walk', 'hbp',
      'sacrifice', 'reached_on_error', 'fielders_choice', 'interference'
    )),
    accepted_values(column := alignment_regime, is_in := (
      'pre_shift_era', 'shift_growth_era', 'full_shift_era', 'post_restriction'
    )),
    accepted_values(column := trajectory_remapped, is_in := (
      'Fly', 'GroundBall', 'LineDrive', 'PopUp', 'Bunt'
    )),
    accepted_values(column := r1_advancement, is_in := (
      'Stayed', 'Advanced1', 'Advanced2', 'Scored', 'OutAdvancing', 'OutCaughtStealing', 'OutPickoff'
    )),
    accepted_values(column := r2_advancement, is_in := (
      'Stayed', 'Advanced1', 'Advanced2', 'Scored', 'OutAdvancing', 'OutCaughtStealing', 'OutPickoff'
    )),
    accepted_values(column := r3_advancement, is_in := (
      'Stayed', 'Advanced1', 'Advanced2', 'Scored', 'OutAdvancing', 'OutCaughtStealing', 'OutPickoff'
    )),
    relationships(column := event_key, to_model := main_models.event_observation_context, to_column := event_key)
  )
);

WITH traj AS (
    SELECT
        event_key,
        raw_value AS traj_raw
    FROM main_models.event_observation_geometry
    WHERE dimension = 'trajectory'
      AND observed_status = 'observed'
),
loc_general AS (
    SELECT
        event_key,
        raw_value AS class_raw
    FROM main_models.event_observation_geometry
    WHERE dimension = 'general_location'
      AND observed_status = 'observed'
),
loc_depth AS (
    SELECT
        event_key,
        raw_value AS class_raw
    FROM main_models.event_observation_geometry
    WHERE dimension = 'location_depth'
      AND observed_status = 'observed'
),
loc_edge AS (
    SELECT
        event_key,
        raw_value AS class_raw
    FROM main_models.event_observation_geometry
    WHERE dimension = 'location_edge'
      AND observed_status = 'observed'
),
ball_handler AS (
    SELECT
        event_key,
        raw_value AS class_raw
    FROM main_models.event_observation_geometry
    WHERE dimension = 'ball_handler_position'
      AND observed_status = 'observed'
),
runners_pivot AS (
    SELECT
        event_key,
        ANY_VALUE(runner_id) FILTER (WHERE baserunner = 'First') AS runner_on_1b_id,
        ANY_VALUE(runner_id) FILTER (WHERE baserunner = 'Second') AS runner_on_2b_id,
        ANY_VALUE(runner_id) FILTER (WHERE baserunner = 'Third') AS runner_on_3b_id,
        ANY_VALUE(
            CASE
                WHEN baserunner = 'First' THEN
                    CASE
                        WHEN baserunning_play_type = 'CaughtStealing' AND is_out THEN 'OutCaughtStealing'
                        WHEN baserunning_play_type = 'PickedOff' AND is_out THEN 'OutPickoff'
                        WHEN baserunning_play_type IN ('PickedOffCaughtStealing') AND is_out THEN 'OutCaughtStealing'
                        WHEN is_out THEN 'OutAdvancing'
                        WHEN run_scored_flag THEN 'Scored'
                        WHEN base_end = 'Third' THEN 'Advanced2'
                        WHEN base_end = 'Second' THEN 'Advanced1'
                        WHEN base_end = 'First' THEN 'Stayed'
                        ELSE NULL
                    END
                ELSE NULL
            END
        ) AS r1_advancement,
        ANY_VALUE(
            CASE
                WHEN baserunner = 'Second' THEN
                    CASE
                        WHEN baserunning_play_type = 'CaughtStealing' AND is_out THEN 'OutCaughtStealing'
                        WHEN baserunning_play_type = 'PickedOff' AND is_out THEN 'OutPickoff'
                        WHEN baserunning_play_type IN ('PickedOffCaughtStealing') AND is_out THEN 'OutCaughtStealing'
                        WHEN is_out THEN 'OutAdvancing'
                        WHEN run_scored_flag THEN 'Scored'
                        WHEN base_end = 'Third' THEN 'Advanced1'
                        WHEN base_end = 'Second' THEN 'Stayed'
                        ELSE NULL
                    END
                ELSE NULL
            END
        ) AS r2_advancement,
        ANY_VALUE(
            CASE
                WHEN baserunner = 'Third' THEN
                    CASE
                        WHEN baserunning_play_type = 'CaughtStealing' AND is_out THEN 'OutCaughtStealing'
                        WHEN baserunning_play_type = 'PickedOff' AND is_out THEN 'OutPickoff'
                        WHEN baserunning_play_type IN ('PickedOffCaughtStealing') AND is_out THEN 'OutCaughtStealing'
                        WHEN is_out THEN 'OutAdvancing'
                        WHEN run_scored_flag THEN 'Scored'
                        WHEN base_end = 'Third' THEN 'Stayed'
                        ELSE NULL
                    END
                ELSE NULL
            END
        ) AS r3_advancement
    FROM main_models.stg_event_baserunners
    WHERE baserunner IN ('First', 'Second', 'Third')
    GROUP BY event_key
)

SELECT
    c.event_key,
    c.game_id,
    c.season,
    c.league,
    c.game_type,
    c.source_family,
    c.park_id,
    c.scorer,
    c.batter_id,
    c.pitcher_id,
    ef.catcher_id AS fielder_pos_2,
    ef.first_base_id AS fielder_pos_3,
    ef.second_base_id AS fielder_pos_4,
    ef.third_base_id AS fielder_pos_5,
    ef.shortstop_id AS fielder_pos_6,
    ef.left_field_id AS fielder_pos_7,
    ef.center_field_id AS fielder_pos_8,
    ef.right_field_id AS fielder_pos_9,
    rp.runner_on_1b_id,
    rp.runner_on_2b_id,
    rp.runner_on_3b_id,
    c.inning_start,
    c.frame_start,
    c.base_state_start,
    c.outs_start,
    c.score_margin,
    e.count_balls,
    e.count_strikes,
    c.alignment_regime,
    c.personnel_confidence,
    c.context_confidence,
    CAST(g.time_of_day AS VARCHAR) AS time_of_day,
    CAST(g.doubleheader_status AS VARCHAR) AS doubleheader_status,
    CAST(g.precipitation AS VARCHAR) AS precipitation,
    CAST(g.sky AS VARCHAR) AS sky,
    CAST(g.wind_direction AS VARCHAR) AS wind_direction,
    CAST(g.field_condition AS VARCHAR) AS field_condition,
    g.temperature_fahrenheit,
    g.wind_speed_mph,
    CAST(EXTRACT(DOY FROM g.date) AS SMALLINT) AS day_of_year,
    c.result_family,
    evp.plate_appearance_result AS pa_result,
    c.hit_or_out,
    CAST(LEAST(GREATEST(e.outs_on_play, 0), 3) AS UTINYINT) AS outs_on_play_capped,
    CAST(LEAST(GREATEST(c.runs_on_play, 0), 4) AS UTINYINT) AS runs_on_play_capped,
    CASE
        WHEN traj.traj_raw IS NULL THEN NULL
        WHEN traj.traj_raw IN ('FoulBunt', 'GroundBallBunt', 'LineDriveBunt', 'PopUpBunt', 'UnspecifiedBunt') THEN 'Bunt'
        WHEN traj.traj_raw IN ('Fly', 'GroundBall', 'LineDrive', 'PopUp') THEN traj.traj_raw
        ELSE NULL
    END AS trajectory_remapped,
    rp.r1_advancement,
    rp.r2_advancement,
    rp.r3_advancement,
    loc_general.class_raw AS batted_location_general,
    loc_depth.class_raw AS batted_location_depth,
    loc_edge.class_raw AS batted_location_edge,
    ball_handler.class_raw AS batted_to_fielder_class,
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
FROM main_models.event_observation_context AS c
LEFT JOIN main_models.event_states_full AS e USING (event_key)
LEFT JOIN main_models.stg_events AS evp USING (event_key)
LEFT JOIN main_models.event_fielders_flat AS ef USING (event_key)
LEFT JOIN runners_pivot AS rp USING (event_key)
LEFT JOIN main_models.stg_games AS g ON c.game_id = g.game_id
LEFT JOIN traj USING (event_key)
LEFT JOIN loc_general USING (event_key)
LEFT JOIN loc_depth USING (event_key)
LEFT JOIN loc_edge USING (event_key)
LEFT JOIN ball_handler USING (event_key)
WHERE c.target_population_status = 'event_level'
