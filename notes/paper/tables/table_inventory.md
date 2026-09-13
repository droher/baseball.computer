> Historical September 4, 2026 snapshot under gate version 2. Its `passed` labels are not current validation claims. See [current evidence](../EVIDENCE.md) and the revised manuscript.

# table_inventory

Row counts and provenance-column cardinality for all 12 published `main_models.*` estimated tables, read from prod `bc.db` on 2026-09-04 after the refit restate. The geometry table alone carries 253M rows, 74% of the 340.6M published rows, and every populated table's `confidence_status` is `passed` under validation gate version 2 at `model_version 0.3.0`.

| table_name | n_rows | n_artifacts | model_versions | confidence_statuses |
| --- | ---: | ---: | --- | --- |
| assist_count_distribution | 596 | 1 | 0.3.0 | passed |
| imputed_advancement_probabilities | 0 | 0 | NULL | NULL |
| imputed_ball_handler_probabilities | 11,712,096 | 1 | 0.3.0 | passed |
| imputed_batted_ball_geometry | 253,013,169 | 5 | 0.3.0 | passed |
| imputed_fielding_credit | 8,335,674 | 2 | 0.3.0 | passed |
| linear_weights_estimated | 5,036 | 1 | 0.3.0 | passed |
| park_factor_summary | 2,636 | 1 | 0.3.0 | passed |
| pitch_count_coverage | 0 | 0 | NULL | NULL |
| pitch_summary_distribution | 18,156 | 1 | 0.3.0 | passed |
| run_expectancy_summary | 5,940 | 1 | 0.3.0 | passed |
| scorer_observation_propensities | 67,397,468 | 1 | 0.3.0 | passed |
| state_transition_summary | 148,500 | 1 | 0.3.0 | passed |

Note: `imputed_advancement_probabilities` (Model H) and `pitch_count_coverage` (Model J coverage arm) are the two deferred/empty tables documented in `docs/estimated-models.md`; 0 rows and NULL provenance aggregates for both is expected behavior (typed zero-row frames), not a query error. `imputed_batted_ball_geometry` has 5 distinct `artifact_id`s because its geometry dimensions (trajectory, location side/depth/edge, general_location) are fit and published as separate per-dimension artifacts; `imputed_fielding_credit` has 2, one per `credit_type` (putout, assist), and since the 2026-09-04 restate both credit types score the same production slice, 4,167,837 rows each. Artifact ids behind the counts: `state-transition-v5`, `re-full-eraregime-v4` (run expectancy and linear weights), `pf-full-ar1-v4`, `ps-cut1-full-v7`, `full-10k-v16-production-export` (putout), `full-10k-v3-cut1-prod` (assist), `e-v12-noprop-{location_side,location_depth,location_edge,general_location}-zero` and `e-v12-noprop-trajectory-shrunk` (geometry), `10k-v6-unseen-fix` (all six observation dimensions), `d-noprop-10k-v2` (ball handler), `assist-count-v1`.
