MODEL (
  name main_models.stress_holdout_registry,
  kind FULL,
  description 'Stress-test holdout registry per doc-02 §"Stress-test holdouts". One row per event_key with seven BOOLEAN flags marking the event for each independent stress-test holdout. Flags are not intersected with the primary fold; generalization to a stress dimension is measured by sampling the posterior predictive on rows where the flag is TRUE. Policy (v1, deterministic from event_observation_context covariates): is_heldout_scorer = scorer IS NOT NULL AND HASH(scorer) % 10 = 0 (10% of distinct scorers). is_heldout_park = park_id IS NOT NULL AND HASH(park_id || season) % 10 = 0 (10% of park-seasons; park-episode unit deferred until entity_link_reliability gains park-episode enrichment). is_heldout_alignment_regime = alignment_regime = ''post_restriction'' (newest of four regimes; most-distinct-from-training OOS test). is_heldout_source_acquisition_block = source_type IS NOT NULL AND HASH(source_type || decade) % 20 = 0 where decade = (season / 10) * 10 (5% of source_type x decade blocks; file-family unit deferred until acquisition ledger exposes file_family). is_heldout_season_block = season IN (2024, 2025) (most recent two seasons; forecasting-forward stress). is_heldout_aggregate_total = HASH(game_id || fielding_team_id) % 20 = 0 (5% of game x fielding-team aggregate combos; player-position aggregate unit deferred until fielding-credit aggregate target lands). is_heldout_player_group = (batter_id IS NOT NULL AND HASH(batter_id) % 20 = 0) OR (pitcher_id IS NOT NULL AND HASH(pitcher_id) % 20 = 0) (5% of batters union 5% of pitchers; player-career unit). All flags COALESCE to FALSE when source covariate is NULL. DuckDB HASH() is deterministic across reruns within a DuckDB version; rotating the holdout slate requires changing the policy literal here and bumping the snapshot.',
  grain (event_key),
  columns (
    event_key UINTEGER,
    is_heldout_scorer BOOLEAN,
    is_heldout_park BOOLEAN,
    is_heldout_alignment_regime BOOLEAN,
    is_heldout_source_acquisition_block BOOLEAN,
    is_heldout_season_block BOOLEAN,
    is_heldout_aggregate_total BOOLEAN,
    is_heldout_player_group BOOLEAN
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    is_heldout_scorer = 'TRUE iff scorer IS NOT NULL AND HASH(scorer) % 10 = 0. ~10% of distinct scorers held out; events with NULL scorer (no scorekeeping coverage) are not held out.',
    is_heldout_park = 'TRUE iff park_id IS NOT NULL AND HASH(park_id || season) % 10 = 0. ~10% of park-seasons held out.',
    is_heldout_alignment_regime = 'TRUE iff alignment_regime = ''post_restriction'' (season >= 2023). Holds out the newest regime entirely so the model is tested on the regime most-distinct from training.',
    is_heldout_source_acquisition_block = 'TRUE iff source_type IS NOT NULL AND HASH(source_type || decade) % 20 = 0, where decade = (season / 10) * 10. ~5% of source_type x decade blocks held out.',
    is_heldout_season_block = 'TRUE iff season IN (2024, 2025). Most recent two seasons held out for forecasting-forward stress.',
    is_heldout_aggregate_total = 'TRUE iff HASH(game_id || fielding_team_id) % 20 = 0. ~5% of game x fielding-team aggregate combos held out; used to test reconstructing official aggregates from event-level allocations.',
    is_heldout_player_group = 'TRUE iff HASH(batter_id) % 20 = 0 OR HASH(pitcher_id) % 20 = 0 (NULL ids treated as FALSE). ~5% of batters union ~5% of pitchers held out by career.'
  ),
  audits (
    not_null(columns := (event_key, is_heldout_scorer, is_heldout_park, is_heldout_alignment_regime, is_heldout_source_acquisition_block, is_heldout_season_block, is_heldout_aggregate_total, is_heldout_player_group)),
    unique_grain(columns := (event_key)),
    relationships(column := event_key, to_model := main_models.event_observation_context, to_column := event_key)
  )
);

SELECT
    ctx.event_key,
    CASE
        WHEN ctx.scorer IS NULL THEN FALSE
        ELSE (HASH(ctx.scorer) % 10) = 0
    END AS is_heldout_scorer,
    CASE
        WHEN ctx.park_id IS NULL THEN FALSE
        ELSE (HASH(CONCAT_WS('|', ctx.park_id::VARCHAR, ctx.season::VARCHAR)) % 10) = 0
    END AS is_heldout_park,
    (ctx.alignment_regime = 'post_restriction') AS is_heldout_alignment_regime,
    CASE
        WHEN ctx.source_type IS NULL THEN FALSE
        ELSE (HASH(CONCAT_WS('|', ctx.source_type, ((ctx.season / 10) * 10)::VARCHAR)) % 20) = 0
    END AS is_heldout_source_acquisition_block,
    (ctx.season IN (2024, 2025)) AS is_heldout_season_block,
    (HASH(CONCAT_WS('|', ctx.game_id, ctx.fielding_team_id::VARCHAR)) % 20) = 0 AS is_heldout_aggregate_total,
    (
        (ctx.batter_id IS NOT NULL AND (HASH(ctx.batter_id) % 20) = 0)
        OR (ctx.pitcher_id IS NOT NULL AND (HASH(ctx.pitcher_id) % 20) = 0)
    ) AS is_heldout_player_group
FROM main_models.event_observation_context AS ctx
