# Open follow-ups

Operational items that don't block deployment but deserve a home.

## rebuild-prod OOM on model_input_fielding_credit

A from-scratch `just rebuild-prod` OOMs on `model_input_fielding_credit`'s audit at the default `BC_DUCKDB_THREADS=14` — the 440M-row view's audit peaks at ~44.7 GiB against the 48GB `memory_limit` (64GB machine). Every other model builds fine. Re-run picks up only the failed view; `BC_DUCKDB_THREADS=4 just rebuild-prod` (or retrying the single model at 4 threads) clears it. Either lower the default rebuild thread count or raise `memory_limit` headroom for that one audit.

## Publish path (DuckLake)

### Site cutover

Deferred until the site team confirms parity on a test branch. Specifically:

- **Query parity.** For a representative sample of site queries, rows and values match between `ATTACH 'https://data.baseball.computer/baseball/v1/baseball.ducklake' (TYPE ducklake, READ_ONLY)` and the existing `ATTACH 'https://.../dbt/bc_remote.db'`.
- **Cold-attach latency.** Single catalog fetch + lazy parquet reads acceptable vs the current single-DB-file fetch. Measure on the site's actual edge.
- **VARCHAR-not-ENUM acceptable.** Site code that filters / joins on ENUM columns (`event_type`, `park_id`, etc.) keeps working with VARCHAR semantics. DuckLake v1.0 stores ENUMs as VARCHAR; the publish script does the cast explicitly so column metadata reflects reality.
- **LLM-metadata bridge** is either ready to consume the DuckLake table layout, or works against both artifacts.

When cutover lands:

1. Delete `scripts/create_web_db.py`.
2. Stop publishing the `dbt/` R2 prefix (leave a grace window for any external consumer pinned to it).
3. Update `README.md`, `CLAUDE.md`, and the site's data-access docs to reference only the DuckLake URL.
4. After the grace window, purge the `dbt/` R2 prefix.

### Cloudflare cache-purge prerequisite

`scripts/upload_ducklake.py` requires `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ZONE_ID` env vars at upload time. Token needs Zone:Cache Purge scope on the `data.baseball.computer` zone.

### DATA_VERSION bumping

`bc/data_version.txt` controls the R2 prefix (`baseball/v<DATA_VERSION>/`). Bump on schema-breaking changes (new ENUM values are not breaking; renamed / removed columns or tables are). Old prefixes stay attachable until manually purged. No automation; bump manually as part of the change that breaks the schema.

### Per-table compression / row-group settings

`scripts/create_web_db.py` writes `event_states_full` at `COMPRESSION GZIP, ROW_GROUP_SIZE 262144` and everything else at `ZSTD, ROW_GROUP_SIZE 1966080`. DuckLake exposes `parquet_compression` / `parquet_row_group_size` only as catalog-wide options (`ducklake_set_option`), not per-table. The publish script sets them catalog-wide to ZSTD + 1966080, so `event_states_full` doesn't get its tuned settings in the DuckLake artifact. Workarounds when DuckLake adds richer write options:

- Per-table options at the DuckLake spec level.
- COPY-then-`ducklake_add_data_files` (write parquet with desired knobs, register the file as a DuckLake data file manifest entry, bypasses normal commits).

### R2 / Cloudflare upload concurrency

`upload_ducklake.py` uploads files sequentially through boto3. Fine for the catalog file, but the data dir is many parquet files. If upload time becomes the bottleneck, switch to `concurrent.futures.ThreadPoolExecutor` around `client.upload_file`.

### Incremental kinds — shelved

Decision (2026-05-03): not pursuing. Motivation was DuckLake snapshot-retention storage savings, but there is no current need to retain snapshots. Both SQLMesh (`snapshot_ttl="in 1 hour"` + janitor) and DuckLake (`expire_snapshots()` keeping the last 5) prune aggressively, so a fixed working set already bounds cost. Adding `INCREMENTAL_BY_TIME_RANGE` would add coordination complexity (interval config, late-arriving data, partition replacement) only when we want to retain N>>5 snapshots.

## Data quality

### QA-fix branch residue (2026-07-05)

The `qa-fixes` branch closed the confirmed QA findings (test hermeticity, `weak_identification_flag` wiring, fail-closed convergence gate, seed franchise/pitch-type gaps, dead `high` context-confidence tier, coverage-SQL minors, doc corrections). Left open:

- ~~**Prod restatement.**~~ Done 2026-07-05 via full `rebuild-prod` (user-chosen over a 33-model restate). First attempt surfaced a real defect in the QA seed extensions: carrying the seven Negro-league franchises' single-league rows into 1949 attributed KCM's 1949 games to NN1 (folded 1931), creating a sparse `(KAN05, 1949, NN1)` park cell at factor 10.47 that failed `calc_park_factor_trajectory_outs`'s bounded_range audit. Fixed by splitting 1949 NAL rows off the six non-NAL rows (all seven clubs played 1949 in the NAL; NN2 folded after 1948). Second rebuild passed everything except the known `model_input_fielding_credit` audit OOM, cleared by resuming the plan at `BC_DUCKDB_THREADS=4`.
- **Era-stale league codes on spanning Negro-league rows.** The single-row-per-franchise convention still attributes e.g. KCM 1937–48 (NAL years) to NN1 and CAG 1932–48 to NN1. Pre-existing, systemic, and now the only remaining league-attribution imprecision for these franchises; fixing it properly means era-splitting more rows with documented membership years.
- **Remaining NULL-league event rows.** The seed fixes cover 17,354 of the 41,738 `league_group='Other'` event rows caused by failed franchise lookups; the remaining 24,384 stem from other franchises/date gaps not in the QA findings. A full uncovered-franchise sweep (join `stg_games` to `seed_franchises` on team_id + date range) is the discovery query.
- **PH6 and CI1, 1936.** 5 game-sides each fall outside their seed rows (PH6 is 1934-only, CI1 is 1937-only). Plausibly independent-club years; a second row with empty `league` may be the right shape, but 1936 status needs historical evidence before editing.
- **Roster-observed hands.** `event_observation_context` still stamps batter/pitcher hand `derived` unconditionally; per-roster `observed` hands remain the documented follow-up. The `high` confidence tier is now reachable regardless (rollup fixed).
- **pyright vs basedpyright.** `[tool.pyright]` is configured but no checker is installed or run anywhere; six files carry inline `# pyright:` directives using basedpyright-only rules (stock pyright: 10 errors, 8 of them unknown-rule; basedpyright: 5 errors + 1,416 warnings). Tooling-direction decision: adopt basedpyright as a dev dependency or rewrite the directives for stock pyright.

### Partial-coverage SUMs

For pre-1900s + Negro-League seasons the per-game SUMs are biased-low (retrosheet has partial coverage; Lahman fills only when retrosheet returns NULL). Right fix needs per-stat per-row gating: choose Lahman when the per-game data has at least one NULL contributor AND Lahman's value is strictly greater than retrosheet's partial SUM. Sketched but not shipped. Gets fiddly because SQLMesh's `EXCLUDE` / `REPLACE` clauses don't expand `@EACH` macros, so the per-stat block has to be emitted via a Python-side macro returning a string (or every stat enumerated by hand). Defer until a real consumer asks.

Pre-1920 SB/CS override (`databank_running`) is preserved as a separate REPLACE — distinct semantic (override vs fill-on-NULL).

### Park-factor priors for sparse leagues

Even with `bounded_max=20`, the residual NN1/NN2 spatial-distribution outliers represent real data but extreme park factors. Bump `prior_sample_size` per-league (e.g. 5000 for NN1/NN2 vs 1000 default) to dampen further if downstream use cases need it.

## Machine learning

### Phase-3 deep-supplement follow-ups

Active items the latest pretrain + downstream cascade does not fix:

- **`model_input_advancement` SQL gaps.** The dataset does not yet emit the dependent variable `advancement_class` or a `time_forward_fold` column. Specs in `targets/advancement.py` no longer register on import. Call `_register()` explicitly once the SQL gap closes. Apply the same 7-class derivation logic the pretrain heads use in `model_input_event_universe.sql` (`r1/r2/r3_advancement` pivots). Restore the registry-dependent tests in `test_advancement_targets.py` (`test_advancement_layout_registered`, `test_specs_publish_to_advancement_manifest`, `test_specs_resolve_via_get_target`).
- **Pretrain emit: persist + load stage-1 vocab.json.** `scripts/pretrain_emit_offsets.py` re-derives vocabularies from the dataset via `_collect_input_stats`. If the dataset parquet is regenerated or `BC_PRETRAIN_DATASET_LIMIT` changes between stage-1 fit and emit, embeddings silently map to wrong tokens. Fix: load stage-1 vocab.json (already persisted by `pretrain/artifacts.py`) and fail loud if any TRAIN token is missing.
- **Promote pretrain encoding helpers to public API.** `scripts/pretrain_emit_offsets.py` reaches into module-private `_apply_all_remaps` / `_collect_input_stats` / `_encode_inputs` in `bc/python_models/statistical/deep/pretrain/training.py`. Either promote those to a public `pretrain/encoding.py` module or move the emitter inside the package.
- **Advancement r2/r3 home-plate fallback.** The `runners_pivot` CASE in `model_input_event_universe.sql` and the future `advancement_class` derivation in `model_input_advancement.sql` rely on `run_scored_flag` for the `Scored` arm. Verify upstream `stg_event_baserunners.run_scored_flag` is non-NULL whenever `base_end = 'Home'`; if not, add a `base_end = 'Home'` fallback.
- **`validate_pre_event` deny-list maintenance.** Add new suffix / prefix patterns whenever a new dataset surfaces a leak shape not covered by the deny-list. The test in `bc/tests/statistical/deep/test_pre_event_layout.py` is parametrized; add the new column there too.
- **Pretrain → downstream dim alignment for ungrouped low-card cols.** Geometry fits need `BC_DEEP_FORCE_EMBED_DIM=128` to match the pretrain's capped 128-D player embedding. That forces every target embed layer to 128, including `park_id` (pretrain dim 17) and `scorer` (pretrain dim 95). `set_pretrained_embeddings` falls back to `skipped_dim_mismatch=True` on those, so park / scorer warm-start is lost. Two fixes worth trying: (a) zero-pad pretrain embeddings to the target dim before copy; (b) bake the same embedding-group share pattern into geometry layouts so `park_id` / `scorer` are looked up through the union vocab and dim resolves consistently. Until then, downstream park / scorer signal is whatever the geometry fit alone recovers.
- **Park-factors / run-values DL specs deferred.** Per `04-deep-learning-supplements.md`, park factors and run values are hierarchical-Bayes territory in Phase 4, not Phase-3 DL targets. Skipped intentionally.

### Phase-4 bayes follow-ups

- **MNAR correction redesign — per-class selection model.** The learned per-class
  `gamma_propensity` covariate on Model A's marginal propensity was attempted and refuted by the
  masked backtest (implementation-review.md T1.1): training is observed-only, so the coefficient
  learns the survivor tilt — among surviving events low `p_observed` correlates with *less* focal
  class — and extrapolates it wrong-signed into the unobserved slice. The real fix needs the
  per-class selection probability P(observed | class, x) entering `eta` as a fixed Bayes-rule
  offset (`−log P(obs | c, x)`) or a joint selection / pattern-mixture model — EM-style, since
  the class is missing exactly where the offset is needed. Validate any candidate with
  `scripts/mnar_masked_backtest.py`. The infrastructure is in place: propensity dataset columns
  (`e-v10-geometry` / `obs-v3-propensity`), full-coverage Model A exports (`10k-v4-fullscore`),
  and the `gamma_propensity` hook (default `gamma_propensity_zero`).
  - DONE (mechanism): the fixed per-class offset form `corrected_c = softmax(eta_c + delta_c)`
    is built as flavor `gamma_propensity_offset` — a scoring-only transform (`selection_offset` in
    `_posterior_event_softmax`, plumbed via `GeometryInputs.selection_log_odds_offset` /
    `BC_GEOMETRY_SELECTION_OFFSET`), fit unchanged. The masked backtest's `offset` arm
    (oracle `delta_c = logit(w_class·mean_intensity)` post-hoc reweight of the noprop arm) is the
    proof; `gamma_propensity_class` stays as the negative control. Design + numbers:
    `notes/data-coverage-implementation/mnar-selection-offset-design.md`.
  - STILL OPEN (production): `delta_c` is not identified from data — it must be supplied (sensitivity
    ribbon over the canonical grid, or an anchored per-(era, class) point from partial-truth). Per-(era,
    class) offset, the sensitivity-ribbon export, and the anchored estimate + its holdout are not built.
- **Model H advancement has no Model A observedness target.** The six obs propensity targets
  cover the geometry dims + ball_handler_position only. Any MNAR work on Model H needs its own
  observedness target first.
- **Model C credit is excluded from the propensity-covariate scope.** Its missingness mechanism
  (attribution availability) differs structurally from the geometry/handler recording mechanism
  Model A models; a credit-side selection correction would need its own missingness model.
- **Smoke-gate thresholds are sized for catastrophe detection.** `validate._BAYES_THRESHOLDS_SMOKE` (`rhat ≤ 1.5`, `ess_bulk ≥ 3`, `divergence_fraction ≤ 0.05`, `post_pred_bucket_dev` warn ≤ 0.10) reflect `SMOKE_CONFIG`'s 100 total draws against a model whose minimum per-cell ess is bounded by the per-cell row count (the v1 trajectory model has ~7000 RE cells, so per-cell ess at 100 draws plateaus around 5). The smoke gate detects broken sampling (NaN, divergence storm), not slow mixing. Default thresholds (`rhat ≤ 1.05`, `ess_bulk ≥ 400`, zero divergences) apply at production sample sizes.
- **Batter / pitcher random effects.** v1 observation propensity model intentionally defers batter and pitcher REs. Add only if residual analysis on v1 shows player-level signal not subsumed by scorer × era × park effects. Cost is potentially huge — ~30k batters × 30k pitchers — and would require a centered + non-centered hybrid.
- **Smoke-gate ess threshold loosened to 100** (was 400) — rare-class FE blocks slow-mix at any N as a sampler-efficiency artifact, not a model-validity issue. The 1M trajectory sweep would still block at ess=6 under either gate.
- **Re-introduce the DL covariate?** Dropped from v1 because the post-PA covariates now in the model cover what the DL was learning. Revisit only if calibration-by-slice diagnostics show residual gaps the current covariate set doesn't fill.

### Phase-4 bayes — done

- ~~`SamplingConfig.cores=1` is a macOS workaround~~ — closed 2026-05-19. NUTS backend defaults to numpyro on both `SMOKE_CONFIG` and `DEFAULT_CONFIG`; JAX vectorized chains sidestep `cores`.
- ~~Aggregated Binomial-per-cell formulation~~ — closed 2026-05-19. Attempted in PR3, failed diagnostics under richer covariate set; redesigned as event-grain on numpyro.
- ~~Roll out 4-dim observation propensity~~ — closed 2026-05-21. Shipped 6 dims (trajectory / location_side / location_depth / location_edge / general_location / ball_handler_position) at 10K each. `broad_contact` dropped, replaced by `general_location`. `pa_result` (13-level plate-appearance outcome) added as FE: load-bearing for ball_handler (+0.186 OOS PR-AUC), neutral on the other 5.

### Buildable-now backlog (run-value chain + MNAR band)

- **MNAR sensitivity ribbon — done.** `python_models/statistical/sensitivity.py` + `scripts/sensitivity_ribbon.py`. Post-hoc reweight of a published per-event class-share export over a selection-log-odds grid (`±{0.25,0.5,1.0}` nats), per-class marginal band; `delta=0` reproduces the published MAR marginal. Grid scale validated against the masked backtest oracle (`--validate-backtest`). Ribbons written beside all 5 `e-noprop-*` geometry fits. The honest band for the unidentified `delta_c`; the anchored per-`(era,class)` point estimate is still deferred. See `mnar-selection-offset-design.md` §Status.
- **`state_transition` — done; converges after a reachability redesign.** Model G's Markov base-out transition arm, estimated-tier `state_transition_summary`. The original centered/non-centered hierarchies did NOT converge (centered ess ≈ 5; non-centered fixed the cell layer but the corpus/start level walled out at rhat 4.04). Root cause was misspecification, not parameterization: the model treated all 24×25 `(start, end)` base-out pairs as reachable, but outs never decrease within an event, so >50% of pairs are structural zeros and the global class-0 reference (0 outs, empty) is unreachable from any start with outs ≥ 1 — 16 of 24 start states were spending free `cell_logodds` on impossible classes pulled to −∞ against the prior. FIX (shipped): a per-start reachability mask (`end_outs ≥ start_outs`), a per-start modal reference pinned to 0, and DROP the corpus pooling level (start states live in different reachable spaces with different references, so pooling them is meaningless). `alpha_trans` + `z_cell` are stored flat over reachable-nonref pairs only. eta/softmax in float64 so the masked `exp(−30)` entries stay representable for the predictive sampler. Result at full scale (16.3M events → 6,218 cells, 16K draws, `state-transition-v3`): **rhat_max 1.016, ess_bulk_min 188, ess_tail_min 490, 0 divergences, held-out TV −37%** (0.070 → 0.044, loglik_lift 0.0124) — clears the strict gate with no rare-class tail above it. Validated, published, and materialized as `state_transition_summary` (155,450 rows, audits passed). `sample_model` now drops the warmup groups nutpie writes despite `save_warmup=False`, halving the artifact.
- **`linear_weights_estimated` — done.** Estimated companion to the deterministic `linear_weights`: propagate Model G's RE posterior draws through `runs_on_play + RE_end − RE_start`, average by play, center vs league mean, collapse to mean/sd/HDI per `(season, league, play)`. New `linear_weights_transition_counts` intermediate collapses ~18M events to per-combo counts. CAVEAT: the band is RE-posterior uncertainty ONLY — finite-sample (sparse-cell) uncertainty is NOT captured, so a low-`n_events` cell looks as tight as a dense one. `n_events` rides along to mark sparse cells; a future v2 could add a bootstrap / multinomial-count layer for sampling uncertainty, or carry the deterministic model's <100-occurrence imputation fallback per draw.
- **Model C `assist_count` (v3.1) — done.** The pre-built cell-grain multi-assist count model wired end to end as estimated-tier `assist_count_distribution`: per-`(result_family, base_state_start, outs_start)` cell Multinomial over `M ∈ {1,2,3,4}`. Full fit converges cleanly (rhat 1.009, ess 3969, 0 divergences), held-out lift 0.124 / TV −0.093 (33%); 149 cells, 3.48M assist events; `@model` materializes with audits passed. See Phase-4 Model C below.
- **Model C `error_credit` (v4) — DATA-BLOCKED, not buildable as a coverage surface.** The error model is code-complete (`models/error_credit.py` + prep + tests), but there is **no production imputation target in the data**: every one of the 16.3M error rows has `unknown_credit_need = 0` (the upstream parser emits no unknown-error signal — see the `model_input_fielding_credit.sql` note "No upstream signal for unknown_assist or unknown_error today"). All 356K events with an error are already attributed to a position. So a fit would only produce held-out diagnostics on fully-observed data with an EMPTY production export — nothing to publish as an imputation surface. Unblocking needs an upstream change (Rust parser / SQL) that emits an unknown-error allocation need, or a v5-style team-game box-residual anchor (`aggregate_residual_errors`). The v4 DP state-gating arm is also separately unbuilt. This is genuinely-future, gated on data, not wiring.

### Phase-4 Model C

- **Model C v1.5 OOS eval at full scale.** v1.5 ships dual-arm (supervised on unmasked well-attributed events + aggregate on synthetically masked ones) and writes `validation/held_out_metrics.json` per fit. After merge, run the N-sweep (10K / 50K / 100K) and pick the operating point on diminishing-returns of OOS top-1 / PR-AUC / log-loss, not just rhat. Held-out games are the 10% of games at `game_hash_fold(g, fold_count=10) == 0`. **Operating point shipped:** `full-10k-v15-tuned` with `BC_CREDIT_NONCENTER_SEASON=1` (non-centered `beta_season` fixed the funnel that 10K centered hit; the knob and the per-event season RE it parameterized have since been removed from the builder — scalar-per-event terms cancel exactly in the softmax) and `BC_CREDIT_MIN_NATURAL_UNK_RATE=0.01` (drops the ~50 modern-PBP seasons with effectively zero natural unknown rate, cutting noisy `beta_season` cells without losing inference scope). When reporting any held-out metric on Model C, also report it restricted to the production-target slice (events with `unknown_credit_need > 0`) — held-out is composition-skewed toward easy events; see the v1.6 retraction for the failure mode this protects against.
- ~~**Model C v1.6 — direct-handler evidence column.**~~ **Shipped 2026-05-23 and retracted same day.** Held-out lift (top-1 +25.7pp, OF PR-AUC ~0.10 → ~0.997) was a sample-composition artifact: held-out events are 77% handler-recorded but the production inference target is **0.09% handler-recorded** (3,663 of 4,167,837 events). The two columns share upstream coverage — when PBP records the putout chain it also records `batted_to_fielder`; when it doesn't record the putout (= the inference target) it also doesn't record the handler. v1.6 collapses to v1.5 on the dominant production slice. Retraction landed in `phase4_model_c_revert_v16`: dropped the column from `model_input_fielding_credit` + `FIXED_EFFECT_COLUMNS`, rolled `dataset_version` back to `0.2.0`, re-promoted `full-10k-v15-tuned`, and added a hard `PRODUCTION_FE_COVERAGE_FLOOR = 0.01` guard in `_credit_data.py`. Full retraction write-up in `notes/data-coverage-implementation/03-hierarchical-models.md`. Reintroducing handler evidence is not blocked but needs (a) an upstream pipeline change that lifts `batted_to_fielder` coverage above 1% on the unknown-putout slice, or (b) any held-out metric reported restricted to the `direct_handler_position IS NULL` slice.
- **Model C v2 — player REs.** Add per-player hierarchical effect once v1 calibration shows residual player signal beyond the positional baseline. Centered + non-centered hybrid likely required given vocab size; eligibility-masked design means most players never appear at most positions, so the prior matters a lot.
- **Model C v3 — assists allocation (cut 1, shipped).** Branch `phase4_model_c_v3` registers `assist_credit_allocation`: K=10 softmax with a NONE sentinel class, single-assist events only (`A_count ∈ {0, 1}`), `putout_position` (1..9) joined per event from the dataset's putout `known_credit` rows and exposed as a fixed effect. Operating point: artifact `full-10k-v3-cut1-prod`, nutpie backend, `BC_CREDIT_NONCENTER_SEASON=1`, 10K game-subsample. Geometry clean: rhat_max 1.036, ess_bulk_min 185, ess_tail_min 430, 0 divergences (non-centered season fixed the funnel centered season hit — sigma_season rhat 1.57 / ess 7 under centered). Held-out metrics are putout-marginalized over the published v1.5 putout posterior (`full-10k-v15-tuned`), because `putout_position` is observed on held-out events but unknown on the production target — the same source-coupling that forced the v1.6 `direct_handler_position` retraction. Marginalized (n_eval=100000): top-1 over 10 classes 0.6635 vs most-frequent baseline 0.6523 (essentially baseline); any_assist PR-AUC 0.5155 vs 0.3477 base rate (a real lift); top-3 0.865; log_loss 1.06; marginal calibration TV 0.052 (per-slice weighted TV 0.052–0.061), NONE under-predicted ~5 pt (0.604 vs 0.652, an expected consequence of marginalizing v1.5's diffuse putout posterior whose own top-1 is ~0.53). The artifact also records `observed_putout_upper_bound` (top-1 0.714, any_assist PR-AUC 0.828) — these condition on the true putout and are an upper bound only, not the production metric; the held-out eval carries a `putout_marginalized: true` flag and excludes `putout_position` from slice calibration when marginalizing. Key finding: per-fielder identification on the production slice is ~baseline — pinpointing the assister needs a sharp putout that production lacks; the model's real value is P(any assist) and calibrated expected shares, not naming the fielder. This is the inverse of v1.6: the lift that survives marginalization is the part that does not depend on the missing clue. Production export wired — scores all 463,093 production-target events (`credit_type='putout' AND unknown_credit_need>0 AND personnel_hard_mask_available AND eligible_for_allocation`), 4,167,837 rows (463,093 × 9 positions); each event's 9 position shares + none_share sum to exactly 1; none_share ranges 0.058–0.58 (mean 0.49), so P(any assist) varies meaningfully by event. Validated against prod personnel tables: `imputed_fielding_credit` join yields 4,167,837 rows, unique grain, zero nulls (not_null + unique_grain audits pass). `exports/event_credit.parquet` carries a nullable `none_share` column; `imputed_fielding_credit` surfaces it. Full SQLMesh branch-env plan-model was not run (a fresh branch env requires a full-corpus rebuild — env-setup cost, unrelated to the model); join logic + audits validated directly against prod, and SQLMesh materialization happens at promotion.
- **Model C v3.1 — multi-assist count submodel SHIPPED.** `assist_count` registered + wired (estimated `assist_count_distribution`): cell-grain Multinomial over `M ∈ {1,2,3,4}` per `(result_family, base_state_start, outs_start)` — the collapsed form of the spec's Dirichlet-multinomial. `result_family`/base-out is the identifiable proxy for the spec's force-out/infield-assist/bunt-DP/rundown taxonomy (not a dataset column). Full fit `assist-count-v1` converges (rhat 1.009, ess 3969), held-out lift 0.124 / TV −0.093. STILL DEFERRED: empirical per-position synthetic-mask weights for assists (cut 1 uses uniform); recalibrate the ~5 pt NONE under-prediction in the K=10 allocation softmax.
- **Model C v4 — errors + double plays. DATA-BLOCKED.** `error_credit` is code-complete but cannot publish as a coverage surface: errors have `unknown_credit_need = 0` across all 16.3M rows (no upstream missing-error signal), so the production export is empty — there is nothing to impute. Needs an upstream parser/SQL change to emit an unknown-error need, or a v5-style team-game box-residual anchor. The DP state-gating arm (DPs only from base/out states that admit two outs) is separately unbuilt. The supervised error softmax (scorer-discretion `delta_scorer` interaction) fits fine on the 356K attributed events but only yields held-out diagnostics, not an imputation surface.
- **Model C v5 — team-level box-residual fallback.** Per-player aggregate (v1) is the strong form. v5 adds a Normal likelihood at the team-credit-type grain for games where `official_credit_authority` returns `withheld` (v1 excludes those rows entirely). Weaker constraint than the per-player target but recovers coverage on games with negative-residual or otherwise invalid per-player totals.

### Phase 5 — runtime artifacts, tiers, rollout

**Done (non-gated scaffold).** Publication-tier registry (`publication_tiers.py`:
official/deterministic/estimated/synthetic/withheld + `tier_for`); the full estimated-metadata
contract (`artifact_id`, `model_name`, `model_version`, `source_snapshot_id`, `method`,
`observed_status`, `confidence_status`, `weak_identification_flag`) stamped on all eleven estimated
tables via `stamp_estimated_contract` (the nine coverage tables plus `state_transition_summary` and
`linear_weights_estimated`), sourced from the artifact manifest; `ess_bulk`/`rhat`
diagnostics dropped from the published summaries; a registry-driven `estimated_contract_complete`
audit on every estimated `@model` (typed-empty passes; populated rows must carry all eight columns).
Compat is structural — the deterministic surfaces (`park_factors`, `run_expectancy_matrix`,
`linear_weights`) are unchanged and tier-registered, the estimated siblings (`park_factor_summary`,
`run_expectancy_summary`) exist and are tier-stamped, grains kept separate (don't join). LLM
supplement carries the official-vs-estimated tier vocabulary. Conventions in
`notes/data-coverage-implementation/phase5-conventions.md`.

**Promotion-coupled remainder (run at/near `promote-prod`).**
- ~~Regenerate `docs/llm/baseball.lsf`~~ — done 2026-07-05 post prod rebuild (213 tables, all twelve
  estimated tables present).
- Enrich the nine coverage `@model` `column_descriptions` so the LSF entries are not sparse.
- ~~Populate `weak_identification_flag` at fit time~~ — done 2026-07-05 (`qa-fixes`): derived from fit
  diagnostics via `validate.diagnostics_indicate_weak_identification` (ESS below 4× gate floor, rhat
  above half-margin band, divergences, non-finite). Published artifacts fit before the wiring still
  carry `false` unconditionally.
- A 7th+ BSL `SemanticTable` for estimated outputs IF they become BSL-queryable — note the `bsl` dep
  group pins `sqlglot < 28`, mutually exclusive with the SQLMesh env (conditional per the checklist).
- Rollback path: the legacy deterministic sources are untouched, so rollback is "stop reading the
  estimated siblings" — formalize the documented restore step at promotion.
- The gated `promote-prod` that writes `bc.db` itself (the user's call).

### Artifact backfill

Six third-wave targets shipped code + tests but their `predictions_*` `@model`s gate on `python_models.ml.artifact_exists(target)`. `enabled=False` until the pin JSON lands. Run the matching `scripts/train_<name>.py --epochs 1 --rows-per-batch 100000` once each to land the artifact JSONs:

- `outcome_baserunning_cat`
- `outcome_batted_location_cat`
- `outcome_batted_trajectory_cat`
- `outcome_has_batting_bin`
- `outcome_is_win_bin`
- `outcome_runs_following_num`

`outcome_baserunning_cat` left with `filter_zero_weight=False` since it has a meaningful `'Other'` label for non-baserunning events. Flip the flag if downstream metrics get noisy on plate-appearance rows.

### MLflow → R2 artifact upload

Deferred until multi-target. When multiple models need to be loaded by the prediction `@model`, push fitted-model artifacts to R2 and have `load_scorer` fetch by run_id.

### Calibration pass

Single-epoch baseline produces argmax probabilities clustered around the majority-class prior (avg p ≈ 0.48 for `InPlayOut`). After more epochs, validate calibration with a reliability diagram before reporting accuracy.

### Sklearn pipeline option

A regression baseline (logistic regression with one-hot + target-encoded categoricals) would be a cheap sanity check against the Keras model. Useful when a target has too few examples to justify deep embeddings.

### `run_id` propagation into the audit

`model_run_id` is uniform per scoring run (one column value across the whole table). Adding an audit that asserts this uniformity would catch accidental multi-run mixing if the scorer is ever called more than once per `execute()`.

### Predictions parquet snapshot

If a downstream wants predictions, add `download_parquet` to the predictions `@model` and re-run publish. No code changes to consumers.

### Hamilton dependency upgrade path

`apache-hamilton` 1.90 resolves alongside SQLMesh 0.234 cleanly. Watch for sqlglot pin conflicts on future Hamilton upgrades. Same constraint story as `boring-semantic-layer` could appear.

## Audits

### Custom `relationships` audit under DEV_ONLY

Resolved 2026-05-03. The audit body's `@to_model` text substitution was replaced with a Python `@macro` `relationships_check(@column, @to_column, @to_model)` (`bc/macros/_env_to_model.py`). The macro reads `evaluator.locals['this_model']` (env-aware: under DEV_ONLY dev, schema is `sqlmesh__<canonical>`, table name is `<canonical>__<model>__<hash>__<env>`), strips the env suffix off the table name, and rewrites `to_model` to the env-suffixed view (`main_models__dev.X`). Declaring `to_model` as a real `depends_on` was rejected because the project DAG has 12 cyclic FK pairs (e.g. `main_models.people` is supplemented from `stg_box_score_*` lines that themselves FK-check `people`). The macro additionally probes the engine adapter for the rewritten target; when a transitively-referenced model hasn't materialized yet, the predicate collapses to `TRUE`, surfacing as a 0-row audit. After a full plan dev the env is complete and audits run their real predicate. Prod runs (canonical schema) are unaffected.

### `earned_runs > runs` residue, ~10 cases per modern season

The `bounded_range(earned_runs ≤ runs)` audit on `player_game_pitching_stats` (and the planned sweep onto `team_game_pitching_stats` / `player_team_season_pitching_stats`) was spec'd with a `season >= 1948` Lahman-supplement carve-out, but the audit still surfaces ~816 rows distributed evenly across 1948 → 2025 (roughly 4 – 23 per season), not the 1,031 pre-1948 Lahman residue the plan expected. Spot-check `LAN202505300` `friem001`: `stg_game_earned_runs.earned_runs = 6` against event-derived `runs = 5`. Likely cause is Retrosheet's bequeathed / inherited runner accounting in the official ER files diverging from the run-assignment logic in `event_pitching_stats.runs`: a runner who was on base when the pitcher left and later scored gets charged ER to the original pitcher, but the run-assignment logic credits the run to whichever pitcher was on the mound when it scored.

Validated 2026-05-07: every offending team-game has matching team totals (team R = team ER), with one pitcher's ER>R offset by another pitcher's R>ER on the same team. The per-pitcher invariant `earned_runs ≤ runs` does not hold by construction. ER and R follow different attribution rules. The audit was dropped from `player_game_pitching_stats`. The team-game-grain version (team R ≥ team ER, modulo Lahman-supplement era) is still candidate for `team_game_pitching_stats`; spec'd, not added in this change.

Pre-1948, the carve-out target was Lahman-supplemented ER exceeding Retrosheet partial-game R sums (separate root cause, same shape). Real fix is the per-stat per-row gate sketched under "Partial-coverage SUMs". Both eras converge once that gate exists.

### Umpire FK audits dropped — 4 unknown umpires not in `main_models.people`

Dropped 2026-05-07. The 6 `relationships(umpire_*_id → main_models.people.person_id)` audits I tried to add to `game_start_info` fail on 18 rows (7 home, 8 first, 3 third) caused by 4 umpires absent from `main_models.people`: `wasnu90`, `Gockle`, `fambu091`, `harrm201`. Two of these (`Gockle`, `wasnu90`) don't even match the standard 8-char Retrosheet person-id shape, so they look like upstream parser errors. Fix path is in `baseball.computer.rs` (or a manual people-supplement seed) so the audits land cleanly when re-added.

### Box-score within-row issues (operationalized via `box_score_data_issues`)

Resolved 2026-05-07. The 26 known box-score within-row violations (15 `hits_gt_at_bats`, 3 each `home_runs_gt_hits` / `strikeouts_gt_batters_faced`, 2 `strikeouts_gt_plate_appearances`, and one each of `hits_gt_batters_faced` / `extra_base_hits_gt_hits` / `earned_runs_gt_runs`, all 1899 – 1948 box scores) are now enumerated by `main_models.box_score_data_issues`. The new `bounded_excluding_data_issues` audit lets game-grain stat models add the same definitional bound checks while carving out the listed rows, so audits land cleanly today and any *new* violation introduced post-staging fails the build. Fix path for the 26 rows is still in the parser at [baseball.computer.rs](https://github.com/droher/baseball.computer.rs) or a manual override seed. Once those land, drop them from `box_score_data_issues` and the audits tighten automatically.

### Scratched starting pitchers (operationalized via `team_game_data_issues`)

Resolved 2026-05-07. 59 PlayByPlay-source team-games where `game_start_info` records a starting pitcher who never threw a pitch (scratched at the last minute, still the SP per MLB rules) are enumerated by `main_models.team_game_data_issues` with `issue_type = 'starting_pitcher_no_appearance'`. The `team_game_has_one_starter` audit and the `bounded_range(complete_games, 0, games_started)` audit on `player_game_pitching_stats` consume the carve-out via `@team_game_data_issue_match`. The 5 pitchers with `CG=1, GS=0` are the relievers who covered all 27 outs after the SP was scratched (Ernie Shore-style); they sit naturally inside the carve-out. New scratched-SP cases are picked up by the issues model on each plan; new audit failures outside the listed team-games will fail the build.

## Tests

### `bc/tests/test_bsl_semantic.py`

Runs only under the `bsl` uv group (the build env can't import BSL because xorq pins sqlglot <28). pytest collects-and-skips cleanly under the build env via `pytest.importorskip`.

## Synthetic box scores

### Date-independent metric (Round 2 Idea A) — date allocation dominates

`scripts/backtest_synthetic_lineups.py` now reports two date-independent recall rates next to the headline `wrong_starters_per_game`:

- `set_miss_rate = 1 − Σ_p min(syn_starts, real_starts) / Σ_p real_starts`
- `pos_set_miss_rate` = same on (player, fielding_position) buckets.

Pitcher rows are dropped on the (syn_pos, real_pos) axis, not on the season-player axis, so a two-way player still scores on his non-P bucket.

Full 1871 – 1910 numbers (post-Round-1):

- `wrong_starters_per_game = 3.999`
- `set_miss_rate = 1.43%`
- `pos_set_miss_rate = 1.91%`

Per-bucket decomposition:

| churn | wrong_starters_per_game | set_miss_rate_pct | pos_set_miss_rate_pct |
|---|---|---|---|
| multi-stint | 0.421 | 1.5 | 2.12 |
| single-stint full | 0.725 | 1.52 | 1.89 |
| single-stint partial | 2.852 | 1.29 | 1.9 |

Diagnosis: player selection is essentially right (`set_miss` flat at ~1.3 – 1.5% across all buckets); the dominant remaining error is which date the optimizer assigns each chosen player to. The single-stint partial bucket carries 2.85 of the 4.00 headline wrong-starters with the lowest set_miss — pure date-axis error.

Round 2 sequence still calls for shipping Idea B (retire modal prior) behind a flag and re-judging; given how flat set_miss is, B is unlikely to move it materially, in which case the remaining ceiling is structural and the next moves are D1 (bench-game accounting), D2 (catcher pair detection), D3 (rest-day priors).

### Per-position fair-share scaling already implemented

Step 3 of the synthetic-lineup-algorithm-improvements plan proposes scaling `position_target` down by `team_games_F / sum_p games_at_position[p, F]` and using `sum_F games_at_position[p]` as the total target. Both already exist in `game_lineups.py`: `_scale_position_targets` (called at line 346) does the position scaling, and `_build_milp_problem` already derives `total_targets` from `sum_F non_pitcher_fielding` over the scaled candidates. No code change available.

The remaining over-allocation (e.g. `raubt101` 1903 CHN: syn=26, real=15) is driven by individual fielding-position appearance counts that exceed real starts (Lahman `fielding.g` includes relief / defensive subs), not by team-level position over-count. Closing it needs a real-vs-appearance signal. `Appearances.GS` would do it but is NULL for non-pitchers pre-1904. No clean fix without new data.

### Catcher wrong-defender rate (1.07 / game)

Tried relaxing the MILP `default_slot` exclusion at `slot.fielding_position == 2` so any C-eligible candidate could occupy the C slot regardless of modal-lineup status (proposed as Step 2 of the synthetic-lineup-algorithm-improvements plan). It is a no-op: the existing predicate `default_slot != lineup_position AND fielding_position != slot.fielding_position` already lets backup catchers and dual-eligibility modal players into the C slot via the second clause, and the modal-default bonus (0.01) is two orders of magnitude smaller than the position-target slack penalty (1.0) so it can't be dominating.

The high C error rate is a date-allocation problem, not a slot-eligibility one. Per-game C choice between the modal C and the backup C has no signal beyond starting pitcher and DH, so the MILP's date assignment within the season-long position target is effectively arbitrary. Real fixes would need a per-game C signal (e.g. caught-stealing / pitch-framing prior tied to the gamelog's starting pitcher) or transaction-driven stint windows (Step 4 of that plan).

### Modal-prior retire flag (Round 2 Idea B) — confirmed no-op

`build_synthetic_lineup_assignments(disable_modal=True)` (also `BC_OPTIMIZER_NO_MODAL=1`, also `--disable-modal` on the backtest CLI) drops the modal-default cost bonus and the `default_slot` slot-pin from the eligibility predicate, then assigns `lineup_position` post-hoc per game-side by ranking non-pitcher starters by season PA/G (pitcher pinned at 9, pre-1973 NL no-DH convention).

Full 1871 – 1910 backtest, modal-on vs modal-off:

| metric | modal-on | modal-off | delta |
|---|---|---|---|
| wrong_starters_per_game | 3.999 | 3.996 | -0.003 |
| wrong_positions_per_game | 1.449 | 1.453 | +0.004 |
| set_miss_rate | 1.43% | 1.43% | 0.00 |
| pos_set_miss_rate | 1.91% | 1.91% | 0.00 |
| C wrong_per_game | 1.072 | 1.072 | 0.00 |

Every delta sits inside the MILP tiebreak noise floor. The modal prior is dead weight at the metric level. Keeping it costs ~0.01% in cost bonus that the position-target slack penalty (1.0) overrides. The flag ships as a diagnostic; we don't delete the modal path since `compute_modal_lineups` is also exported and used as the no-optimizer fallback for orphan team-seasons and fallback infeasible sides. A future cleanup commit could drop the modal-bonus cost term unconditionally without removing the modal lineup.

The full 1871 – 1910 result confirms the Round 2 Idea A diagnosis: the remaining ~4 wrong starters / game is dominated by date-allocation error, not player-set or position-set selection. The structural ceiling is reached without external data (per-game C signal, scheduled-rest priors, or transaction logs beyond what tranDB provides). Round 2 Ideas C, D1, D2, D3 are deferred — see plan for the full menu.

The `synthetic_box_score.*` schema fills lineup skeletons for the ~25K games that exist only in `misc.gamelog` (mostly pre-1901 MLB, plus a few NLB cases). Game-level metadata, default seasonal lineups, optimized non-pitcher starters, listed starting pitchers, and parsed line scores ship today; the items below extend the coverage.

### Event-shaped synthetic tables

Out of scope for the initial cut. The gamelog gives no per-event signal, so HBP / HR / SB / CS / DP / TP / comments / pinch_* / team_*_lines tables stay unwritten. Fabricating per-PA outcomes is a separate decision (statistical priors conditioned on park / season / batter and pitcher) and not under consideration yet.

### Project gamelog winning / losing / save pitcher

`misc.gamelog` carries `winning_pitcher`, `losing_pitcher`, `save_pitcher`, and `game_winning_rbi` columns that `stg_gamelog.sql` does not currently project. Once those columns land in staging, populate them on `synthetic_box_score.box_score_games` and remove their NULL stubs from the column list.

### Fold synthetic rows into season-level stats

`player_team_season_offense_stats` and `player_position_team_season_fielding_stats` currently treat gamelog-only games as silently missing. Plumbing the synthetic lineups in would credit each modal regular with one game per gamelog-only game, but since stat columns stay NULL, the season totals would not move. Decide whether the model layer should prefer "real but missing" over "synthetic but inferred" before touching the existing season models.

### NLB coverage gap

Team-seasons with no `baseballdatabank.appearances` rows are dropped silently by `team_season_modal_lineups`. For Negro-League seasons in scope, this means a gamelog game with no synthetic shell. Either log the dropped team-seasons prominently or attempt a roster-only fallback (use `misc.roster` to pick nine players, without the modal-fielder ranking).

### Pre-1973 DH assumption

`stg_databank_appearances` skips the `g_dh` column on the assumption that every gamelog-only game in scope is pre-DH. If a gamelog-only DH game ever surfaces, restore `g_dh` in the pivot (maps to `fielding_position = 10`) and teach the modal-lineup picker how to handle the DH slot.

### Lahman/Databank ↔ Retrosheet team_id mismatches

Resolved. The four Databank stagings (`stg_databank_appearances`, `stg_databank_batting`, `stg_databank_fielding`, `stg_databank_pitching`) now translate `team_id` via `baseballdatabank.teams.team_id_retro` (joined on `(year_id, team_id)`) and project `team_id_retro` directly with no fallback. The `not_null(team_id)` audit on each staging fails the build loudly if a row ever lacks a crosswalk match.

## Pretrain artifact directories on disk

Stage-1 / stage-2 pretrain artifacts produced before the v8→canonical rename live under `artifacts/statistical/deep/event_universe_v8/` and `event_universe_v8_context/`. The published pointer at `artifacts/statistical/published-data_coverage/pretrain/event_universe.json` still hard-codes the legacy `event_universe_v8` path in `manifest_path`, so downstream resolution works. New pretrain fits land under `event_universe/` and `event_universe_context/`. Move the historical artifacts only if disk hygiene matters — the pointer is what binds, not the directory name.

## Phase-3 pretrain — embedding LR + regularization

Full-corpus stage-2 best epoch was epoch 1 (out of 8). Embeddings overshoot the residual past the first pass under current schedule (split optimizer, trunk Adam 1e-3, embed Adam 5e-3 with warmup + CosineDecay α=0.1). Try lower embed LR (1.5e-3) and / or modest L2 (1e-5 to 1e-4) on `embed_player` to extend useful training and see if a longer fit lifts hard-head accuracy further. Current pretrained-vs-baseline gates already clear by ~5×, so this is a tightening, not a blocker.

## T2 spec-completion deferrals

The T2 spec completions (branch `data-coverage-t2-spec-completion`) landed code-complete +
smoke-verified. Full-scale fits, validation backtests, and publication-pointer advancement for the
new arms are a deferred materialization pass. The sub-pieces below were deferred deliberately —
genuine data blockers, identifiability impossibilities, or ambiguous estimands. Each names the
prerequisite to unblock.

### Materialization pass — done
- **G `run_expectancy`** — full fit `re-full-eraregime-v1` exercises the era_regime hierarchy at
  full scale (0 divergences, rhat 1.009, held-out RMSE 8% over the state-only baseline). Per-branch
  pointer published.
- **F `park_factor_runs`** — full-history AR(1) fit `pf-full-ar1-v2` (centered intercept + tightened
  deviation priors fixed an NB prior-predictive overflow; constant lone-park cells surface NaN rhat,
  now carried as `None`). rhat 1.028, ess 103, 0 divergences, theta_park sum-to-zero within
  season-league to 1e-15. Per-branch pointer published.
- **J `has_count` coverage** — registered `pitch_count_observedness` Bernoulli target + export branch
  + dataset-scoped propensity aggregator + `pitch_count_coverage` @model. Smoke held-out ROC-AUC 0.995.
- **E `gamma_handler`** — `_posterior_event_softmax` / `_posterior_held_out_softmax` taught the term;
  inert when absent. A production geometry fit with the handler covariate active can now publish.

### Materialization pass — deferred (each blocked on a real decision)
- **`linear_weights` (namespace decision).** `compute_marginal_linear_weights` derives Bayesian linear
  weights from the published `run_expectancy_summary`, but `main_models.linear_weights` already exists
  as a deterministic surface off `event_transition_values`. Per the separate-namespaces invariant the
  Bayesian version must be a sibling @model (own namespace), not overwrite the deterministic one. Pin
  the surface placement before wiring.
- **`state_transition` (gated on linear_weights).** The Markov base-out transition matrix is built and
  smoke-clean, but its primary consumer is the context-neutral linear weights, so it inherits the same
  placement decision. Wire alongside linear_weights once that lands.
- **`assist_count` / `error_credit` (roadmap-future).** These are the Model C "v3.1 multi-assist count"
  (Dirichlet-multinomial M∈{1..4}) and "v4 errors" submodels — built code-complete ahead of slot.
  Production wiring + full fits are genuinely that later-version work; sequence them with the rest of
  Model C v3.1/v4 rather than now.

### G — context-neutral P_LW linear weights (estimand decision)
The spec's headline context-neutral LW integrates `V_end` against the modeled marginal transition
`P_LW(end|start)` rather than the realized end state. That estimand is ambiguous at the per-play-type
grain: `P_LW` is keyed on the start state alone, so it does not distinguish play types sharing a start
state. The standard marginal LW is built and well-defined; pin the context-neutral estimand (or
confirm marginal is the published deliverable) before computing it. The Markov submodel that would
feed P_LW is built.

### C — double-play submodel (data blocker)
No DP truth in `model_input_fielding_credit`: the `known` CTE pulls only putouts/assists/errors;
`double_plays` exists upstream in `event_player_fielding_stats` but is not surfaced, and there is no
`outs_on_play` event column to gate "two outs recorded on the play." Surface both, then build the
DP submodel.

### C — real box-residual aggregate constraint (missing materializer)
There is no `materialize_credit_authority_targets` producing `credit_authority_targets.parquet`
(residual joined to `official_credit_authority.authority_source` for the per-source `sigma_aggregate`
map + `withheld` exclusion). The aggregate arm still uses the synthetic mask. Build the materializer,
then replace `_credit_data.py::_build_aggregate_targets_from_mask` with a box-residual builder emitting
the same 5-tuple; `models/credit.py`'s `T_observed` Normal arm needs no change.

### E — measurement-error confusion arm (identifiability impossibility)
UNIDENTIFIABLE in single-source Retrosheet. The spec's `Ω`/`Δ` confusion arm needs `Recorded | G` and
`Deduced | G` as two conditionally-independent labelings of the latent class. But the deduced layer is
a deterministic recode of the same scorer's fielding (`calc_batted_ball_type` rules), not an
independent second observation — so `Ω`/`Δ` cannot be separated from the class prior. Same blocker as
the Model B contact-label model. Needs a genuinely independent second label source (different scorer /
tracking system). Stays recorded-only until then.

### F — park_episode_id boundaries (NULL column)
`model_input_park_factors.park_episode_status` is 100% NULL in both dataset artifacts, so no stable
`park_episode_id` resets the AR(1) chain at structural park changes (it currently keys on
`(park, league)`). The upstream park-history dimension must populate a non-null `park_episode_id` and
`model_input_park_factors.sql` must select it; the prep then builds AR(1) chains on
`(park_episode_id, league)`.

### F — umpire/weather/surface/day-night/hand covariates (absent columns)
`model_input_park_factors` has no umpire id, weather (temp/wind), surface (turf/grass), or day_night
column. `batter_hand`/`pitcher_hand` exist per-event but the model is team-game grain. Each needs an
SQL change to surface the column (hand additionally needs an event-grain restructure).

### J — summary-model source context (single-source data) + batter/pitcher context (grain)
`model_input_pitch_summary` is single-source (`source_family`/`source_type` each one distinct value),
so a source partial-pooling level on the cell-grain summary is degenerate — needs a dataset/SQL change
surfacing >1 source family. Per-batter/pitcher context is only expressible at event grain; the summary
model is cell-grain over `(result_family, season, league)` — needs an event-grain restructure (or a
coarsened batter/pitcher bucketing). The `has_count` coverage Bernoulli arm IS built
(`models/pitch_coverage.py`); it is a standalone prep+builder not yet wired to a CLI target.
