# table_inventory

Row counts and provenance-column cardinality for all 12 published `main_models.*` estimated tables; the geometry table alone carries 253M rows, dwarfing the other eleven combined, and every populated table's `confidence_status` is still `exploratory` at `model_version 0.3.0`.

| table_name | n_rows | n_artifacts | model_versions | confidence_statuses |
| --- | ---: | ---: | --- | --- |
| assist_count_distribution | 596 | 1 | 0.3.0 | exploratory |
| imputed_advancement_probabilities | 0 | 0 | NULL | NULL |
| imputed_ball_handler_probabilities | 11,712,096 | 1 | 0.3.0 | exploratory |
| imputed_batted_ball_geometry | 253,013,169 | 5 | 0.3.0 | exploratory |
| imputed_fielding_credit | 4,257,414 | 2 | 0.3.0 | exploratory |
| linear_weights_estimated | 5,099 | 1 | 0.3.0 | exploratory |
| park_factor_summary | 2,636 | 1 | 0.3.0 | exploratory |
| pitch_count_coverage | 0 | 0 | NULL | NULL |
| pitch_summary_distribution | 18,156 | 1 | 0.3.0 | exploratory |
| run_expectancy_summary | 5,891 | 1 | 0.3.0 | exploratory |
| scorer_observation_propensities | 67,397,468 | 1 | 0.3.0 | exploratory |
| state_transition_summary | 152,925 | 1 | 0.3.0 | exploratory |

Note: `imputed_advancement_probabilities` (Model H) and `pitch_count_coverage` (Model J coverage arm) are the two deferred/empty tables documented in `docs/estimated-models.md` — 0 rows and NULL provenance aggregates for both is expected behavior (typed zero-row frames), not a query error. `imputed_batted_ball_geometry` has 5 distinct `artifact_id`s because its geometry dimensions (trajectory, location side/depth/edge, general_location) are fit and published as separate per-dimension artifacts; `imputed_fielding_credit` has 2, one per `credit_type` (putout, assist).
