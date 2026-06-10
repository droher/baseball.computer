AUDIT (
  name model_input_eligible_matches_seed
);

SELECT
  t.event_key,
  t.dimension,
  t.observed_status,
  t.source_acquisition_status,
  t.model_input_eligible
FROM @this_model AS t
INNER JOIN main_seeds.seed_observed_status AS st
  ON st.observed_status = t.observed_status
WHERE t.model_input_eligible IS DISTINCT FROM (
  st.is_training_eligible AND t.source_acquisition_status != 'not_acquired'
)
