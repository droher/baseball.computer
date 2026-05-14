MODEL (
  name main_models.dl_proposal_manifest,
  kind VIEW,
  description 'Deep-learning proposal manifest. Zero-row stub — LEFT-JOIN-safe placeholder until DL supplements land. Real implementation materializes one row per (event_key, dimension) carrying the DL proposal artifact id and a serialized class-probability vector + logit.',
  grain (event_key, dimension),
  columns (
    event_key UINTEGER,
    dimension VARCHAR,
    dl_artifact_id VARCHAR,
    dl_p_class VARCHAR,
    dl_logit_class DOUBLE
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    dimension = 'Modeling dimension the DL proposal targets (e.g., geometry trajectory, advancement). Schema reserved; stub emits zero rows.',
    dl_artifact_id = 'Identifier of the trained DL model that produced this proposal. NULL until DL supplements land.',
    dl_p_class = 'Serialized per-class probability vector for the proposal. NULL until DL supplements land.',
    dl_logit_class = 'Logit / log-odds for the proposed class. NULL until DL supplements land.'
  )
);

SELECT
    NULL::UINTEGER AS event_key,
    NULL::VARCHAR  AS dimension,
    NULL::VARCHAR  AS dl_artifact_id,
    NULL::VARCHAR  AS dl_p_class,
    NULL::DOUBLE   AS dl_logit_class
WHERE FALSE
