---
title: Modeling review, 2026-09-03
type: review
status: open
audience: David, implementing agents
last-verified: 2026-09-03
---

# Modeling review, 2026-09-03

Scope: everything the data-coverage initiative shipped between May and July 2026 (Bayes models A, C, D, E, F, G, J, assist count, linear_weights_estimated; the deep supplements; the MNAR mechanism; validation and publish machinery; the paper), reviewed against the code, the published artifacts, prod `bc.db` (read-only), and the test suite. Eight independent read-only reviewers, one per slice, then cross-checked on the main thread. Every claim below was verified against the file and line cited or the prod query described; findings that could not be verified were dropped.

Test suite state at review: 784 passed, 0 failed, 0 skipped in 133 s (`bc/tests/statistical`).

## Verdict

The scaffolding is in good shape. The likelihoods match the documented formulas, the provenance contract is stamped from manifests and never hard-coded, every share and probability invariant holds to floating-point across 331M prod rows, the game-hash holdout is applied consistently before subsampling, the deep fold runner enforces out-of-fold discipline at write time, and the transition reachability fix was the right diagnosis. The problems are one layer up: what the published numbers mean, and whether the gates could have caught them.

Three published surfaces are wrong or misdescribed in ways the gates cannot see. Three of the five geometry dimensions apply a large constant per-class shift to every production event because production rows have no DL prediction. The MNAR "data anchor" is an algebraic identity of the observed slice and carries no information about the unrecorded slice. And the Model G surfaces fit substitution and no-play rows as events, so the run-expectancy variance is understated and the transition matrix carries spurious self-loops. Separately, the CI publish path produces empty estimated tables, and the paper's results section is stale against the database it says it reads.

## High severity

### H1. Location geometry imputations are driven by NULL handling, not the model

`bc/python_models/statistical/bayes/dl_covariate.py:62-65` maps a NULL `dl_p_class` row to a zero log-probability vector. `bc/python_models/statistical/models/_geometry_data.py:763` (production frame) then subtracts the training-slice class means, so a NULL row becomes `-mean_k`, and `bayes/training.py:1476` adds `gamma_dl * (-mean_k)` to every logit. The location DL specs score only `is_observed_class` rows (`deep/targets/geometry.py:139-142`), so on the frozen `e-v12` dataset 100% of production rows for `location_side`, `location_depth`, and `location_edge` are NULL. Trajectory is unaffected because its spec's filter includes derived and unknown rows.

Measured in prod `imputed_batted_ball_geometry` (6,989,832 events per dimension):

| dim | class | training share | published production mean share |
|---|---|---:|---:|
| location_edge | All | 0.009 | 0.173 |
| location_side | Default | 0.70 | 0.24 |
| location_side | Foul / Left / Right | 0.02 / 0.05 / 0.05 | 0.14 / 0.15 / 0.15 |
| location_depth | ExtraDeep | 0.06 | 0.22 |

The held-out gate cannot catch this: held-out rows are observed rows with real DL predictions. Two tests enshrine the halves separately (`bc/tests/statistical/bayes/test_geometry_prep.py:454`, `:622`) and none the combination. The propensity covariate path has a null-rate log and a 100%-NULL guard (`_geometry_data.py:503-511`); the DL path has neither.

Fix direction: either score the production slice with the DL model (extend the location specs' `filter_predicate` the way trajectory does) or zero the gamma term on NULL rows instead of centering them. Add the same NULL-rate guard the propensity path has. Then refit the three location dims.

### H2. The MNAR "data anchor" is `-ln(p_obs_GB)`, an identity of the observed slice

`mnar_anchor.py:224` computes `log(p_masked / p_obs)`. Every `derived` row in the trajectory-deduction slice is GroundBall (1,875,691 of 1,875,691 in prod), so `p_masked = 1` for GroundBall and 0 for every other class in every era. `artifacts/statistical/anchors/trajectory/real-run/anchor_offsets.parquet` confirms `delta_raw` = 1.2408 / 0.9171 / 0.8355 = `-ln(0.28916)` / `-ln(0.39968)` / `-ln(0.43367)` exactly. `test_mnar_anchor.py:119-131` asserts this identity as expected behavior. The headline pre-1950 anchored share 0.5818 is therefore `1/(2 - p_obs)` up to renormalization, a function of the observed slice alone.

The paper's own tables expose it: `groundball_mnar.md` says the 1988+ gap collapsed to 0.006, yet the 1988+ anchor is +0.835 nats and the ribbon publishes a 1988+ unrecorded GB share of 0.569 vs MAR 0.392. The "monotone shrinkage" the paper reads as scoring practice filling in is `p_obs` rising.

What the derived slice does support: it is a real partial truth. Joining the published trajectory export to `model_input_geometry`, pre-1950 the export scores 763,993 derived events (29% of the scored slice) with mean MAR GroundBall share 0.32 against a truth of 1.0. That gives a lower bound P(GB | unrecorded) >= n_derived / n_unrecorded = 0.292 (pre-1950), 0.339 (1950-87), 0.400 (1988+), and MAR sits below the bound in 1988+. It also gives a free validation set the pipeline never uses.

Fix direction: drop the anchored offset and the "anchored to data" language. Replace with the bound argument and a validation of the MAR shares on the derived slice. Keep the ribbon as an explicitly assumed band.

### H3. Model G fits non-plate-appearance rows as events

`_run_values_data.py:160-174` and `_state_transition_data.py:149-171` filter only on non-null keys and `runs_to_end_of_inning`; there is no filter on `plate_appearance_result` or `result_family`. For `2015_NL_0_0`, 9,428 of 32,407 rows (29%) have `plate_appearance_result IS NULL`, `runs_on_play = 0`, end state `0_0` (substitutions, no-play rows); genuine HR self-loops are 621. Corpus-wide `0_0 -> 0_0` has 1,085,517 events.

Consequences: each no-op row carries the identical `runs_to_end` as the next PA, so `n` in `NB(n*lambda, n*phi)` is inflated 15-40% by duplicates and the posterior sd is understated. `state_transition_summary` publishes `P(0_0 -> 0_0) = 0.309`, documented in `docs/estimated-models.md` as "bases empty, still 0 out"; that is not a plate-appearance transition law, and anyone iterating the chain gets 20-30% spurious self-loops on every state.

Related, same prep: the dataset includes innings >= 9 (2,271,023 rows, 12.5%, walk-off censored), ~230K non-regular-season rows, and 164K `denominator_policy = 'exclude'` rows. The deterministic `run_expectancy_matrix.sql:34-44` excludes all three. Across 5,739 matched cells the Bayes mean sits 1.1-1.4% below the deterministic value in every era (0 outs -0.021, 1 out -0.017, 2 outs -0.007). The per-play offsets in `linear_weights_estimated` vs `linear_weights` (IntentionalWalk -0.015, Walk -0.011) are this censoring plus shrinkage, not posterior uncertainty.

Fix direction: apply the deterministic matrix's population filter in both preps, refit `run_expectancy` and `state_transition`, re-propagate `linear_weights_estimated`.

### H4. The CI publish path produces empty estimated tables

`.gitignore:31` ignores `artifacts/`; zero pointer files and zero Bayes artifacts are tracked. `.github/workflows/publish.yml:35` runs a from-scratch `sqlmesh plan --auto-apply` on a bare checkout and then publishes `main_models` to R2. On that runner every pointer is missing, `manifest_ingest.py:298-303` logs INFO and continues, and every audit passes (`bc/audits/estimated_contract_complete.sql` is vacuous on zero rows; no coverage `@model` has a row-count audit). The published DuckLake catalog's twelve estimated tables are empty while local prod has 331M rows. Every pointer also stores an absolute `/Users/davidroher/...` `manifest_path`, so committing the pointers alone would not fix it.

Fix direction: upload artifacts (or at least the export parquets and manifests) to R2 and resolve pointers from there in CI, or exclude the estimated tier from the CI publish and publish it from the local prod build. Either way, add a minimum-row-count audit to every populated estimated table so an empty frame fails loudly.

### H5. `imputed_fielding_credit` putout rows are training data, not imputations

All 9,953 putout events (89,577 rows) have `known_credit > 0` and `unknown_credit_need = 0` in `model_input_fielding_credit`; 9,365 are not `eligible_for_allocation`. They are the training grain, which `statistical/CLAUDE.md` says ("the putout export covers BOTH the supervised and masked subsets of the training set"), but `docs/estimated-models.md` states the estimand as events "where the official record left a putout or assist unattributed". The 463,093-event production slice receives only assist rows; no putout shares exist for the events that need them. A consumer summing putout `expected_share` double-counts recorded putouts.

Fix direction: score the putout model over the production unknown slice the way the assist export already does, or drop putout rows from the published table and say so in the doc.

### H6. A smoke fit can be published with `confidence_status = 'passed'`

`cli.py:693-761` (`_run_publish_manifest`) checks only that the manifest's `artifact_id` matches; it never reads `is_smoke` or `validation_status`. `validate.py:761-762` then picks thresholds from the artifact's own `is_smoke` flag (rhat 1.5, ESS 3, 5% divergences, every held-out baseline downgraded to warn). `just fit-bayes --smoke` then `publish-manifest` then `validate-gates --write` yields `passed`, and `manifest_ingest.py:77-79` copies it into the table. All 19 published manifests have `is_smoke=False` today, so this is latent, not live.

Fix direction: refuse to publish a manifest with `is_smoke=True`, and refuse to stamp `passed` on one.

## Medium severity

### Identification and estimands

**M1. `weak_identification_flag` does not measure identification.** Four separate defects:
- `bayes/training.py:389-485` takes the minimum ESS over every element of every parameter (thousands of scorer, park, and cell elements), not group-level diagnostics as `statistical/CLAUDE.md:196` claims. `run_expectancy` is flagged True on every row (and `linear_weights_estimated` inherits it) because `phi` alone sits at ESS 396.4 against a 400 floor; every `re_value` is above 10,000.
- Park factor: `count_focus` at `training.py:429-446` omits `rho_park`, `sigma_park_innov`, `sigma_park_init`, `home_adv`. Read directly from `pf-full-ar1-v3/inference/posterior.nc`: `rho_park` mean 0.958, ESS 50, rhat 1.05; `sigma_park_innov` mean 0.019, ESS 17, rhat 1.17. The fit fails the strict rhat gate on the hyperparameter that controls all pooling, yet reports `passed` with rhat_max 1.010 and `weak_identification_flag = False` on all 2,636 rows.
- The flag is computed once at fit time and never recomputed. `assist_credit_allocation/full-10k-v3-cut1-prod` (2026-05-25, rhat 1.036, ESS 185) predates the flag logic (2026-07-05) and publishes False where the current thresholds give True.
- State transition's flag comes from the most data-dense pairs (`0_0 -> 0_1`, n = 977K, ESS 185) mixing poorly under the non-centered `z_cell * sigma_cell` parameterization (`state_transition.py:112-113`), which is the wrong choice for cells with thousands of events. Not from sparse cells.

**M2. `run_expectancy_summary.league` is `league_group`, not `league`.** `event_states_full.sql:207-210` maps anything outside AL/NL/FL to `'Other'`; RE keys off it. `state_transition_summary`, `linear_weights_estimated`, and `park_factor_summary` use real codes (NAL, NN2, ECL). 44 ST cells have no RE match, 99 RE cells have no ST match, and Negro-league linear weights borrow RE draws from the pooled `Other` bucket. `'Other'` does not exist in `event_states_full.league`, the documented join target.

**M3. Run-expectancy variance is misspecified.** One global `phi` (posterior 1.82) against empirical var/mean ratios of ~2.0 for `0_0` and two-out states and ~1.2-1.4 for `0_5..0_7`. Median `re_value_sd / sqrt(emp_var/n)` is 0.71-0.81 for bases-empty and two-out states, 1.09-1.11 for loaded zero-out. The validation report already records `predictive_coverage_out_of_band 0.8774` (warn) and it was not acted on. Compounds H3.

**M4. Pitch summary's pinned reference `b0_s0` is a data-error class for walks and strikeouts.** `pitch_summary.py:64-65`; `_pitch_summary_data.py:97-106` builds all 12 classes with no per-family mask. Walk `b0_s0` is 411 of 621K events; strikeout `b0_s0` is 511. The manifest's lowest-ESS rows are exactly the impossible walk and strikeout classes, which is what sets the table's flag True. Prod carries up to 0.06 mass on strikeouts ending with fewer than two strikes (1974 AL) while the doc says the constraint is recovered and prints 0.000.

**M5. State transition's outs-only mask leaves 115 of 408 "reachable" pairs impossible in one event** (e.g. `0_0 -> 0_7`), each with an `alpha_trans ~ N(0, 3)` plus a `z_cell` per cell: 115 corpus-level plus 29,683 cell-level pure-prior parameters. Published mass is tiny (max 2.4e-5) so this is a cost, not an error.

**M6. Park factor is runs per PA with a near-unit-root AR prior.** `park_factor.py:99,143` offsets by `log(PA)`; PA is itself inflated by scoring, so the factor attenuates (DEN02 2015: 1.264 vs `park_factors.basic_park_factor` 1.395 on runs per inning). With rho 0.958 and innovation sd 0.019, Coors moves 1.39 -> 1.21 -> 1.27 over 30 seasons and single-season events (2002 humidor) smear over 5+ seasons. AR steps are rank-within-chain, not seasons (`_park_factor_data.py:157-162`): MIL05|NL 1965 -> 1998 is one step. 81% of the 121 x 114 AR grid is pure prior.

**M7. Model A assigns unseen scorers the NULL-scorer effect.** `_event_data.py:213`: because 19% of rows have NULL `scorer`, `__unknown__` is always in the vocabulary, so scorers absent from the 10K sample (12.8% of scored events, 1.37M per dimension) take `beta_scorer[__unknown__]` (+0.146) rather than the documented -1 mask. Empirical observed rates on the trajectory frame: NULL-scorer 0.606, seen 0.441, unseen 0.281. The lowest-observation population receives the highest-observation effect.

**M8. Models D and E carry no era term before 2010.** `event_observation_context.sql:254` maps every season <= 2009 to one `alignment_regime` level; `season_league_idx` is built but never enters the geometry or ball-handler builders. Trajectory training-row season quantiles are [1941, 2001, 2021]; production [1919, 1955, 1983]. The cancellation argument in `docs/estimated-models.md:59` holds only for class-constant effects; per-class era effects would not cancel and are simply absent.

**M9. Model C aggregate arm drops zero-count cells and fixes sigma.** `_credit_data.py:627` filters `t_target > 0`, so the arm rewards mass on positive cells and never penalizes mass on positions that recorded nothing. `sigma_box_aggregate = 0.5` is a fixed constant (`:632`, `:905`); `BayesPriorConfig.sigma_box_aggregate` is dead config. The doc's "softmax over the personnel-eligible position set" does not exist: all nine positions always compete.

**M10. `linear_weights_estimated` publishes on n_events = 1 with no floor** (1924 NN1 Triple -0.774 vs 1.053) and substitutes RE = 0 for start keys with no posterior cell instead of dropping them (`linear_weights_estimated.py:71-115`; 448 events). The deterministic sibling floors at 100 occurrences with `is_imputed`, and drops missing cells.

### Deep supplements

**M11. Pretrained embeddings leak fold labels into "out-of-fold" DL predictions.** Every fold model warm-starts batter/pitcher embeddings from `event_universe` (`deep/targets/geometry.py:126,142`), which was pretrained on the whole DuckDB TRAIN partition with heads on the same geometry labels (`pretrain/training.py:536-544`, `pretrain/targets.py:95-140`). The Bayes holdout (BLAKE2s fold 0) is an unrelated partition, so 70.1% of Bayes held-out rows (432,301 of 616,513 for trajectory) sit inside the pretrain's labeled set; the held-out Bayes metric carries the same contamination. Which pretrain artifact produced v8/v9-cv is not recorded (`input_artifact_ids: []`). Magnitude unmeasured.

**M12. `gamma_dl` is a single scalar with an inert prior.** `models/geometry.py:79` has no class dim. Posterior sd 0.032-0.043 vs prior sd 0.5: the prior contributes under 1% of posterior precision. The paper writes `gamma_c` per class throughout and calls the flavor "shrunk".

**M13. No published DL artifact was gated on a held-out metric against a baseline.** `validate.py:144-161` compares only when `exports/baseline_predictions.parquet` exists; nothing writes it. Measured for this review on TEST, DL vs per-`result_family` class prior log-loss: trajectory 1.229 vs 1.258, side 0.996 vs 1.035, depth 1.094 vs 1.114, edge 0.899 vs 0.922. Real, modest, and previously unmeasured.

**M14. `calibration_method: "temperature"` in the v8 manifests is false.** `calibrators.py` was deleted in `d6404ee` with no call site ever in `training.py`. Trajectory v9-cv is a raw focal-loss softmax (systematically under-confident, consistent with `gamma_dl = 1.157` vs 0.84-0.90 for the CE-trained dims).

**M15. The gamma_dl ablation artifacts do not exist on disk.** Only the `-shrunk` geometry artifacts survive; no `e-v12-noprop-*-zero` dir and no `gamma_dl_shift.json` anywhere. `logs/gamma_dl_zero/*.log` confirm the fits ran 2026-07-14 and record top-1 (matching the table) but not log-loss or shift statistics. The paper's answer to referee M4 is unreproducible without ~1 h/dim of refitting.

### Validation and gates

**M16. The multinomial log-loss gate compares against the pooled held-out marginal only.** `training.py:1064-1082`, `validate.py:638-655`. The cell-grain families use nested baselines (`start_state_prob`, `mu_state`, park-removed model); the event-grain multinomials do not, so a Bayes layer that adds nothing over the DL proposal or a per-season class prior passes. `location_depth` passed at +0.040 nats (3.6% of baseline).

**M17. `validation_status` has no staleness or provenance.** `scripts/validate_gates.py:170-197` writes only the status string. Nothing forces a re-stamp when a gate is added, and the `--write` stamp is skipped when unchanged.

**M18. A full-scale fit with no held-out evidence passes** (`validate.py:548-560`, `bayes_held_out_metrics_absent` is warn). All calibration checks are warn-only: held-out ECE (`:591-604`), bucket-dev (`:853-880`), predictive coverage (`hdi_coverage.py:318-319`). Multinomial held-out ECE grades argmax confidence, the statistic the log-loss commit says no consumer reads; `distribution_calibration` TV is recorded but never thresholded.

**M19. The EDA publication gate is disconnected from publish.** `evaluate_publication_gate` is reached only by `check-publication-gate`; `_run_publish_manifest` and the sweep never call it. The 12 `model_configs/*.json` treatments never reach the artifact, and the published flag answers a different question than `06-rollout-and-validation.md` says it does.

**M20. The deep pointers never validate in the sweep.** `find_manifest` builds `root/dl_proposal_<dim>/<id>/` and never falls back, so all four rows are `missing` (already in `notes/followups.md`). Run directly, the location-depth artifact returns `deep_oof_fold_provenance_unverifiable` (v8 exports carry no `fold_id`).

**M21. The pitch_summary `method` column is wrong.** `manifest_ingest.py:696-697` stamps `hierarchical_bayes_nb` on a 12-class `pm.Multinomial` (`models/pitch_summary.py:70`). The ingest test never asserts `method`.

**M22. `source_snapshot_id` carries no snapshot identity.** 11 of 12 tables stamp `'dev'`, the `@VAR` default at `model_input_geometry.sql:224` and siblings.

**M23. Pointer present but export missing silently empties the prod table.** `manifest_ingest.py:309-316` and `linear_weights_estimated.py:135-141` degrade to the typed empty frame on a WARNING; `promote-prod` is a restate on a FULL model. A missing `manifest_path` hard-fails (correct); extend that to the export.

### MNAR

**M24. The "wrong sign" narrative contradicts the code's sign convention.** `z` is the standardized logit of `propensity_p_observed` (`_geometry_data.py:493-530`); masked events have low z. A negative `gamma_GB` times negative z raises GroundBall on the masked slice, which is the direction the correction needs. The survivor-tilt mechanism the paper describes predicts `gamma_GB > 0`. The -0.001 relative reduction is consistent with "inert", not "wrong-signed". The backtest `metrics.json` does not exist in the repo, so the coefficient cannot be re-checked.

**M25. The oracle-offset gate is an algebraic identity for three of four designs.** For class-only selection, `P(c | R=0) ∝ P(c | R=1) * odds_mask(c)` holds at the marginal level for any per-event shares, including a constant model. Only `covariate_joint` (0.537) carries information, and the paper does report it as partial.

**M26. Every robustness number is from smoke runs with all gates failing.** `mnar_backtest_robustness.md`: `SMOKE_BUDGET=1000`, 50 draws x 50 tune x 2 chains, ESS 20-47, `overall_pass` False for all four designs. The paper presents them without the caveat; its reproducibility command omits `--smoke`; no run artifacts exist under `artifacts/statistical/backtests/`.

**M27. The "joint ribbon" is the single-class marginal sweep.** `sensitivity.py:261`: the anchor vector is nonzero only on GroundBall, so other classes move only via renormalization. `JOINT_PERTURBATION_NATS = 0.25` has no derivation, replacing the ±1.0 grid that at least had a backtest calibration.

### Paper

**M28. §7, §9, §10 are stale against prod.** Every populated table now stamps `confidence_status = 'passed'` (restated 2026-07-30) and `linear_weights_estimated` already carries the Dirichlet bands, but the paper says every table is `exploratory` and the bands are "not yet promoted". The 2015 NL table (HomeRun HDI, OtherAdvanceOut) is wrong, and the claim that OtherAdvanceOut is the one row where the deterministic point sits outside the HDI is false (all 20 now fall inside). Internal contradictions: "18 of 23" vs "19 of 23"; "stamped at fit time" vs "at publish time" (code: materialization).

**M29. The transition diagnostics quoted are for `state-transition-v3`** (rhat 1.016, ESS 188); the published v4 has rhat 1.048, ESS 185, `weak_identification_flag = True`. The paper never says which tables carry the flag while presenting run expectancy, state transition, and linear weights as calibrated.

**M30. The split scheme described is neither partition.** The paper says `HASH(game_id) % 100` into TRAIN/VALIDATE/TEST and "every model reads the same partition"; every Bayes holdout is a separate BLAKE2s 10-fold partition, fold 0. The two hashes are unrelated and not nested. About 15% of Bayes held-out games are DuckDB-VALIDATE games whose DL logits came from the full fit that early-stopped on them.

**M31. Response-letter items asserted but absent from the text.** M9 (park-factor coverage "validated by the same machinery": `park_factor_runs` has no coverage hook); M4 (no ML-workflow QA labeling); Minor 1, 2, 5, 6, 8. Five cited memory files do not exist (`phase4_obs_propensity_findings`, `t2_spec_completions_landed`, `mnar_learned_gamma_propensity_refuted`, `pretrain_architecture`, `phase3_pretrain_proxy_misleads_downstream`). Model C is described as "collapsed Dirichlet-multinomial" (it is a plain multinomial) with `sigma_box` as a parameter (it is a constant). Model B's claim type flips between "formal identification limit" and "uninformative". The ablation is narrated as having guided the flavor choice; `gamma_dl_ablation.md` records that no zero flavor existed before 2026-07-14.

## Low severity

- DL clip at 1e-7 (log -16.1) makes near-zero DL probabilities unrecoverable through a scalar gamma; 3.9% of trajectory entries fall below 1e-4.
- `models/observation.py:6` says reference level pinned at 0; the builder uses `ZeroSumNormal`.
- Model A genuinely unseen levels (-1) plug in 0 rather than integrating over the RE scale; only park hits this path today.
- `hdi_cov` for state transition includes 29,019 near-empty (cell, class) points covered trivially by `[0, 0]`; restricting to `prob_mean >= 1e-3` gives 0.9666, still in band.
- `_check_grain_uniqueness_per_partition` treats every non-reserved column as grain; a future extra column silently loosens the check.
- Branch-root shadowing of `published/` holds only because `promote-prod` leaves `BC_STATS_PUBLISHED_ROOT` unset (`config.py:55-59`).
- `linear_weights_estimated` stamps `model_name='run_expectancy'`; the column no longer identifies the table.
- `gamma_dl_shift.py:84-86` divides by zero on a zero-variance cell, NaN compares False, and `share` is silently lowered.
- `publication.py:124-144` ignores severity, so an unlisted `warn` finding blocks.
- `validate_pre_event` deny-list misses `raw_value`, `deduced_value`, `plate_appearance_result`, `sentinel_type`, `observed_status`; `GEOMETRY_LAYOUT` is clean today.
- `leakage_probes.source_probe_held_out` has no callers outside tests despite the paper presenting the >= 0.75 AUC rule as a gate.
- `TRAJECTORY_BUNT_REMAP` and `_BUNT_VARIANTS` are duplicated constants with no test binding them.
- Paper: the 0_0-start transition example cannot exhibit the outs mask; `location_side` ranks third on log-loss lift, not second; "twenty-fold" is 23x; additive separability is stated as derived rather than assumed.

## Test suite

784 passed. The suite is hermetic and the log-loss baseline, reachability mask, selection-offset reweight, and linear-weights propagation are tested against independent hand computations. Gaps, ranked:

1. Per-grain sum-to-1 is untested at ingest for state transition, pitch summary, assist count, fielding credit (including `none_share`), and advancement; four ingest fixtures write un-normalized `rng.uniform` rows and nothing complains. Ball handler is tested at ingest but not after the personnel inner join.
2. Pointer present but unresolvable: only the geometry-dimension mismatch case is tested. Neither ingest module checks that the resolved manifest belongs to the requested spec.
3. Convergence gate fail sides (`bayes_high_rhat`, `bayes_low_ess`, `bayes_divergences`, `bayes_calibration_ece`, `bayes_post_pred_bucket_dev`) have zero test references.
4. Provenance columns: only `artifact_id` is value-compared through ingest.
5. "HDI contains mean" is not computed anywhere.
6. NB collapse parameterization is untested (names and dims only).
7. `_export_state_transition_summary`, `_evaluate_state_transition_held_out`, `_evaluate_run_expectancy_held_out`, `_evaluate_assist_count_held_out`, `_export_event_credit_shares`, `_posterior_event_means_pitch_coverage`, `_build_posterior_summary` have zero test references.
8. All 14 coverage `@model` `execute()` bodies are untested.

Quality: one pure snapshot test (`test_mnar_masked_backtest.py:459-492`, SHA digests and floats from one seeded run); several vacuous tests that assert on the test's own computation (`test_error_credit.py:145-161`, `test_linear_weights_estimated.py:169-173`, `test_splits.py:35-44`); global-state leaks (`test_validate_gates.py:41` leaves modules in `sys.modules`, `test_linear_weights_estimated.py:352` seeds global NumPy, two files set `KERAS_BACKEND` at import); the `slow` marker is never deselected so its docstrings are false; `test_pitch_summary_model.py:242-273` depends on a cwd-relative real artifact. Dead code with tests but no consumers: `models/error_credit.py`, `publication_tiers.py`, `deep/manifest_ingest.read_published_probabilities`, `leakage_probes.confound_probe`.

## Recommended order

1. H1 (location geometry NULL covariate): the fix is small, the refit is three dims, and three published surfaces are wrong today.
2. H3 + M2 + M3 (Model G population filter, league key, variance): one prep change, two refits, one re-propagation; fixes run expectancy, transitions, and linear weights together.
3. H4 (CI publish): decide where artifacts live for CI and add row-count audits.
4. H6 + M17 + M18 + M23 (publish guards): refuse smoke, refuse absent held-out on full fits, hard-fail missing exports, record gate version in the stamp.
5. M1 (flag): compute on group-level diagnostics, include every hyperparameter, recompute in the sweep.
6. H2 + M24-M27 (MNAR): rewrite around the bound and the derived-slice validation; rerun the backtests at full budget or drop the table.
7. H5, M4, M7, M21 (putout rows, pitch reference class, unseen scorers, method column).
8. Paper: M28-M31 after the refits, so the numbers are final once.
9. Tests: gap items 1-3 first.
