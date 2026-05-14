MODEL (
  name main_models.stress_holdout_registry,
  kind VIEW,
  description 'Stress-test holdout registry. Zero-row stub — LEFT-JOIN-safe placeholder until the split-registry policy SQL lands. Consumers COALESCE flags to FALSE.',
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
    is_heldout_scorer = 'TRUE iff the event belongs to a scorer-holdout split. NULL in stub; COALESCE FALSE downstream.',
    is_heldout_park = 'TRUE iff the event belongs to a park-season or park-episode holdout. NULL in stub; COALESCE FALSE downstream.',
    is_heldout_alignment_regime = 'TRUE iff the event belongs to an alignment-regime holdout. NULL in stub; COALESCE FALSE downstream.',
    is_heldout_source_acquisition_block = 'TRUE iff the event belongs to a source-family or file-family holdout. NULL in stub; COALESCE FALSE downstream.',
    is_heldout_season_block = 'TRUE iff the event belongs to a season or era block holdout. NULL in stub; COALESCE FALSE downstream.',
    is_heldout_aggregate_total = 'TRUE iff the event participates in an aggregate-total holdout for fielding. NULL in stub; COALESCE FALSE downstream.',
    is_heldout_player_group = 'TRUE iff the event belongs to a player-group holdout for embeddings or player effects. NULL in stub; COALESCE FALSE downstream.'
  )
);

SELECT
    NULL::UINTEGER AS event_key,
    NULL::BOOLEAN  AS is_heldout_scorer,
    NULL::BOOLEAN  AS is_heldout_park,
    NULL::BOOLEAN  AS is_heldout_alignment_regime,
    NULL::BOOLEAN  AS is_heldout_source_acquisition_block,
    NULL::BOOLEAN  AS is_heldout_season_block,
    NULL::BOOLEAN  AS is_heldout_aggregate_total,
    NULL::BOOLEAN  AS is_heldout_player_group
WHERE FALSE
