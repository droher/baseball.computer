MODEL (
  name main_models.model_input_fielding_credit,
  kind VIEW,
  description 'Modeling dataset for fielding-credit multinomial allocation: full opportunity universe = events x 9 fielders x 3 credit_types (~440M rows on event_level). Drives from event_personnel_lookup -> personnel_fielding_states (positions 1-9) CROSS JOIN VALUES putout / assist / error. known_credit is the observed putouts/assists/errors from event_player_fielding_stats. unknown_credit_need is the per-credit-type unknown allocation target from fielding_credit_gaps (only unknown_putouts is reachable today). aggregate_residual is the per-credit-type box-vs-pbp residual. Zero-credit rows are kept by design — they are the multinomial model negative examples and the conservation-constraint denominator. Filtered to target_population_status = event_level.',
  grain (event_key, player_id, fielding_position, credit_type),
  columns (
    event_key UINTEGER,
    player_id VARCHAR,
    fielding_position UTINYINT,
    credit_type VARCHAR,
    known_credit DOUBLE,
    unknown_credit_need DOUBLE,
    aggregate_residual DOUBLE,
    fielding_evidence_status VARCHAR,
    gap_class VARCHAR,
    personnel_hard_mask_available BOOLEAN,
    eligible_for_allocation BOOLEAN,
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
    player_id = 'Fielder occupying the position at the event (personnel_fielding_states.player_id).',
    fielding_position = 'Fielder position 1-9 from personnel_fielding_states.',
    credit_type = 'putout, assist, or error. CROSS JOIN with the per-event personnel fielding state.',
    known_credit = 'Observed credit count for the (event, player, position, credit_type) from event_player_fielding_stats (COALESCE 0). Zero when the fielder did not record this credit on the play.',
    unknown_credit_need = 'Per-credit-type unknown-allocation target from fielding_credit_gaps. For putout: unknown_putouts (the play had a putout credited to fielding_position = 0). No upstream signal for unknown_assist or unknown_error today, so 0 for both.',
    aggregate_residual = 'Per-credit-type box-vs-PBP residual at the team-game level from fielding_credit_gaps (aggregate_residual_putouts / _assists / _errors). Useful as a conservation-constraint anchor.',
    fielding_evidence_status = 'fielding_credit_gaps.fielding_evidence_status, copied through per row.',
    gap_class = 'fielding_credit_gaps.gap_class, copied through per row.',
    personnel_hard_mask_available = 'fielding_credit_gaps.personnel_hard_mask_available. TRUE iff PSR hard-zero mask is consistent across the event personnel.',
    eligible_for_allocation = 'fielding_credit_gaps.eligible_for_allocation. TRUE iff the row sits in the subset the allocation model is predicting (unknown_putout / no_box_unknown / box_residual_positive AND hard mask available).',
    primary_fold = 'Default game-hash split. HASH(game_id) mod 100 -> [0,69]=TRAIN, [70,84]=VALIDATE, [85,99]=TEST.',
    training_weight = 'DOUBLE. 1.0 universally (no data_error flag is reachable on the fielding ledger surface). Kept for API parity with other datasets.',
    source_snapshot_id = 'Stamp from the source_snapshot_id var.',
    holdout_flags = 'STRUCT of 7 stress-test holdout BOOLEANs, NULL until stress_holdout_registry materializes the split policy.',
    dl_artifact_id = 'dl_proposal_manifest.dl_artifact_id, NULL until DL supplements land. Manifest joined on (event_key, dimension = credit_type) for forward-compat.',
    dl_p_class = 'dl_proposal_manifest.dl_p_class, NULL until DL supplements land.'
  ),
  audits (
    not_null(columns := (event_key, player_id, fielding_position, credit_type, game_id, season, batting_team_id, fielding_team_id, primary_fold, source_snapshot_id)),
    unique_grain(columns := (event_key, player_id, fielding_position, credit_type)),
    accepted_values(column := credit_type, is_in := ('putout', 'assist', 'error')),
    accepted_values(column := primary_fold, is_in := ('TRAIN', 'VALIDATE', 'TEST')),
    accepted_values(column := fielding_evidence_status, is_in := (
      'complete_with_zero_unknowns', 'complete_with_known_unknowns',
      'no_fielding_row', 'incomplete_event_flag'
    )),
    accepted_values(column := gap_class, is_in := (
      'complete', 'unknown_putout', 'unknown_assist_risk',
      'box_residual_positive', 'box_residual_negative',
      'no_box_unknown', 'data_error_flagged', 'not_applicable'
    )),
    accepted_values(column := target_population_status, is_in := ('event_level', 'aggregate_only', 'gamelog_only', 'structural_absence', 'out_of_scope', 'coverage_within_source_sparse')),
    accepted_values(column := source_family, is_in := ('play_by_play', 'box_score', 'gamelog', 'derived', 'absent')),
    accepted_values(column := alignment_regime, is_in := (
      'pre_shift_era', 'shift_growth_era', 'full_shift_era', 'post_restriction'
    )),
    accepted_values(column := leverage_bucket, is_in := ('low', 'medium', 'high')),
    relationships(column := event_key, to_model := main_models.event_observation_context, to_column := event_key)
  )
);

WITH opportunities AS (
    SELECT
        epl.event_key,
        pfs.player_id,
        pfs.fielding_position,
        pfs.fielding_team_id,
        ct.credit_type
    FROM main_models.event_personnel_lookup AS epl
    INNER JOIN main_models.personnel_fielding_states AS pfs
        ON pfs.game_id = epl.game_id
        AND pfs.personnel_fielding_key = epl.personnel_fielding_key
    CROSS JOIN (VALUES ('putout'), ('assist'), ('error')) AS ct(credit_type)
    WHERE pfs.fielding_position BETWEEN 1 AND 9
),

known AS (
    SELECT
        event_key,
        player_id,
        fielding_position,
        putouts,
        assists,
        errors
    FROM main_models.event_player_fielding_stats
)

SELECT
    o.event_key,
    o.player_id,
    o.fielding_position,
    o.credit_type,
    CASE o.credit_type
        WHEN 'putout' THEN COALESCE(k.putouts::DOUBLE, 0)
        WHEN 'assist' THEN COALESCE(k.assists::DOUBLE, 0)
        WHEN 'error'  THEN COALESCE(k.errors::DOUBLE, 0)
    END AS known_credit,
    CASE o.credit_type
        WHEN 'putout' THEN g.unknown_putouts
        ELSE 0.0
    END AS unknown_credit_need,
    CASE o.credit_type
        WHEN 'putout' THEN g.aggregate_residual_putouts
        WHEN 'assist' THEN g.aggregate_residual_assists
        WHEN 'error'  THEN g.aggregate_residual_errors
    END AS aggregate_residual,
    g.fielding_evidence_status,
    g.gap_class,
    g.personnel_hard_mask_available,
    g.eligible_for_allocation,
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
FROM opportunities AS o
INNER JOIN main_models.event_observation_context AS c USING (event_key)
LEFT JOIN known AS k
    ON k.event_key = o.event_key
    AND k.player_id = o.player_id
    AND k.fielding_position = o.fielding_position
LEFT JOIN main_models.fielding_credit_gaps AS g
    ON g.event_key = o.event_key
    AND g.fielding_team_id = o.fielding_team_id
LEFT JOIN main_models.dl_credit_proposal_manifest AS p
    ON p.event_key = o.event_key
    AND p.player_id = o.player_id
    AND p.fielding_position = o.fielding_position
    AND p.credit_type = o.credit_type
LEFT JOIN main_models.stress_holdout_registry AS s USING (event_key)
WHERE c.target_population_status = 'event_level'
