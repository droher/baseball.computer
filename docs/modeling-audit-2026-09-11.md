# Modeling audit — 2026-09-11

Reviewed code: `426b287b6db101dbfea8a88733b3a14bb719d5d3`. Local artifacts and `bc.db` inspected September 11. This is an audit and proposed work order; it does not change fitted models, publication statuses, database contents, or production.

Subsequent work: the [evidence contract](modeling-evidence-contract.md) and [shared-split reference comparison](geometry-reference-results-2026-09-11.md) are complete. The latter found an additional target-definition defect: observed `location_side` labels are local angle modifiers, including ambiguous `Default` values. Earlier location scores below measure that recorded field, not global field direction. Correcting this contract precedes further side fitting; trajectory's reference also failed the frozen predictive criteria and regressed on its historical observed subset.

The [corrected geometry refits](geometry-corrected-refits-2026-09-11.md) subsequently repaired that target and completed passing research comparisons for global side and an era-by-result trajectory interaction. Historical masking, calibration, and aggregate uncertainty remain the next evidence gaps; the audit findings below retain their original review scope.

## Judgment

Keep the modeling system, but change what earns confidence. The project has useful probability estimates, a sound separation between recorded facts and estimates, and substantial infrastructure for offline fitting. Its present evidence is strongest for prediction on held-out recorded games. That evidence does not establish accurate reconstruction of unrecorded historical events, identification of individual effects, or calibrated uncertainty for downstream quantities.

The next milestone should be one reproducible, end-to-end reconstruction benchmark and an enforceable publication contract. It should precede more model families, larger fits, or another round of deep pretraining. Start the benchmark with cheap conditional baselines and a model without pretrained inputs. Add complexity only when it improves the same target-population evaluation.

This conclusion is not a claim that all estimates are wrong. Four freshly recomputed posterior diagnostic checks passed, and several families have held-out improvements against their recorded comparators. The main problem is the distance between those results and the claims attached to the outputs.

## What was checked

- Read model builders and data preparation for observation, geometry, handler/fielding credit, park factors, run expectancy, transitions, and linear weights; deep pretraining/fold logic; validation, dataset snapshotting, and publication paths.
- Resolved all 23 top-level published pointers: 19 Bayesian and four deep artifacts. All 19 Bayesian artifacts pass the current validator using stored diagnostics. This was not a complete recomputation of all posterior diagnostics or coverage hooks.
- Recomputed diagnostics from the saved posteriors for location depth, trajectory, putout allocation, and park factors. All passed; maximum R-hat ranged from 1.0035 to 1.0052 and minimum bulk ESS from 1,534 to 2,391. No variables were excluded in these four checks.
- Inspected local production table metadata and sample rows, including `passed` confidence statuses. This was not a new live public-site verification or a full table-by-table conservation audit.
- Queried narrow columns of the frozen geometry frame after a 100,000-row smoke check; evaluated a simple location-side baseline; measured clustering effects in the held-out run-values frame.
- Ran 113 focused gate/publication tests and a separate 100-test split/deep/MNAR suite (one deselected). Both suites passed. Seven initial gate-suite failures came from an ArviZ cache-write permission error; rerunning with the required cache access resolved all seven.
- Exercised isolated synthetic counterexamples against the actual validator and dataset snapshot functions. These used temporary files and an in-memory database.

The accompanying [evidence bundle](modeling-audit-2026-09-11-evidence.json) records artifact metrics, queries, diagnostic results, test commands, and counterexamples. Historical-target proportions below come from the inspected saved bound artifact; predictive scores come from local published fit artifacts unless explicitly described as newly computed. No fits were retrained in this audit.

## Findings

### 1. High: there is no common untouched evaluation boundary across the composed model

The deep pipeline uses the frozen `primary_fold` TRAIN/VALIDATE/TEST partition. Bayesian geometry and observation preparation instead draw their own BLAKE2s game holdout from all eligible recorded rows. In the trajectory frame, 30,816 games are labeled primary TEST, but only 3,004 enter the Bayesian holdout; the remaining 27,812 are eligible for its training pool before subsampling.

A different split is not inherently leakage: the Bayesian holdout is disjoint from its own direct fit. The problem is composition. The primary TEST set is no longer a test set for the complete system, and Bayesian holdout rows can carry upstream predictions or embeddings whose training used their target labels.

The downstream deep fold runner correctly excludes each fold from that target fit. However, every fold can warm-start from a shared pretrain fitted on the entire primary TRAIN partition using the same geometry targets. Fold exclusion after target-supervised representation learning does not remove that information. The exact pretrain used by the published deep fits cannot be reconstructed from their manifests: their `input_artifact_ids` are empty. The published trajectory Bayesian artifact actively consumes the deep proposal. The mechanism is supported by the code and artifact configuration; its effect on reported accuracy has not been measured.

**Decision:** establish an outer split shared by every supervised stage, with inner folds for model selection and stacking. Initially disable pretrained inputs to obtain a clean reference. Only pay for fold-specific pretraining if it improves on that reference. Record training-game digests and every upstream artifact. The TEST slice inspected during this audit is development evidence now; do not present further tuning on it as a fresh confirmatory test.

Evidence: `models/_geometry_data.py:915-925,998-1005`; `models/_event_data.py:428-480`; `deep/training.py:338-390,803-824,864-967`; `deep/pretrain/training.py:536-548,650-672`; `deep/targets/geometry.py:103-143`, all under `bc/python_models/statistical/`. Split SQL: `bc/models/intermediate/modeling_datasets/model_input_geometry.sql:218-222`.

### 2. High: validation targets a different population from historical reconstruction

The saved trajectory bound artifact contains 5,867,709 unrecorded events; 5,732,739, or 97.70%, are before 1988. The temporal deep acceptance exercise trains through 2022 and validates on 2023. That is a legitimate temporal test, but it does not directly test transport into older recording regimes.

The geometry frame also contains only acquired play-by-play event rows. All 84,272,874 dimension rows have `target_population_status='event_level'` and `source_family='play_by_play'`. These models do not reconstruct missing games or create event-level truth from aggregate-only sources. Source/scorer-block masking inside acquired play-by-play is useful as a stress test, but it does not manufacture validation truth for entirely absent games.

The relevant distinction is between learning `P(class | context, recorded)` and using it as `P(class | context, unrecorded)`. High observation-model AUC or good recorded-game log loss does not identify that second distribution. The published trajectory fit uses `gamma_propensity_zero`; the current output is an uncorrected reference distribution, not an identified MNAR correction. Existing robustness results are smoke-budget fits with convergence failures, and oracle offsets in some designs largely verify the correction algebra.

**Decision:** use blocked era, scorer, and acquisition-pattern evaluations, including backward transport where recorded historical truth permits it. Apply masks that reproduce the joint absence of target and context fields. Report overlap/support and failure by slice. Use defensible deterministic deductions as bounds or auxiliary measurements, with explicit error assumptions; retain sensitivity ranges for the unidentified remainder. More training data alone cannot remove this identification problem. This follows the distinction between predictive evaluation under structured sampling and untestable missingness assumptions in [Roberts et al.](https://www.wsl.ch/lud/biodiversity_events/papers/Roberts_et_al-2017-Ecography.pdf) and [Molenberghs et al.](https://rss.onlinelibrary.wiley.com/doi/abs/10.1111/j.1467-9868.2007.00640.x).

Evidence: `scripts/permutation_importance_generic.py:202-230`; `notes/paper/tables/mnar_backtest_robustness.md`; `notes/paper/tables/trajectory_mnar_bound.md`; `artifacts/statistical/anchors/trajectory/bound-run/summary.json:105-139`; `bc/models/intermediate/modeling_datasets/model_input_geometry.sql:225-242`.

### 3. High: the gates can pass without the evidence they purport to require

Three isolated full-fit validator probes returned:

| Held-out payload | Result |
| --- | --- |
| Empty JSON object | `passed`, no findings |
| Non-finite `loglik_lift` | `passed`, no findings |
| Positive lift plus ECE 0.90 | `passed`, calibration warning |

The validator rejects a missing file, but it does not require an appropriate, finite predictive metric for each target kind. Multinomial detection partly depends on the optional presence of `top1_accuracy`. Coverage failures and even no matched coverage cells produce warnings. Missing coverage inputs can skip the check entirely.

The deep baseline comparator is a stub: it counts baseline rows and checks for a probability column. It does not compare predictive scores even if the file exists. Separately, the sweep resolves a deep pointer such as `dl_proposal_trajectory` as a model-directory name, while its artifact is stored under `geometry_trajectory`. All four published deep pointers therefore evade the normal sweep as “missing,” despite their manifests existing. Three location exports lack fold IDs, and none of the four has a baseline export.

Publication is not an enforcement boundary either. `publish-manifest` rejects smoke fits but does not invoke the validation or EDA gate. CI checks pointer-file existence; downstream audits check structural contracts and row counts. Those are valuable safeguards, but they do not imply successful scientific validation.

**Decision:** create one typed, target-specific validation result bound to the exact artifact and dependency hashes. Require mandatory finite metrics, minimum evaluable samples, common-split provenance, and a real baseline comparison. Reject failed dependencies for validated publication. Allow explicitly exploratory releases only through an explicit policy. Separate numerical, predictive, calibration, transport, and identification evidence rather than substituting one status for all of them.

Evidence: `bc/python_models/statistical/validate.py:110-134,137-190,400-416,750-945,1139-1155`; `scripts/validate_gates.py:149-179,320-340`; `bc/python_models/statistical/cli.py:701-778`; `.github/workflows/publish.yml:30-49`; `scripts/check_published_pointers.py:42-67`; `bc/audits/estimated_contract_complete.sql`.

### 4. High: provenance checks cannot distinguish important changes to the training data

The dataset query hash is computed from `SELECT * FROM <table>`, not the rendered view logic or source contents. Rerun verification compares that hash, the source-snapshot string, and column schema. Twenty-two of the 23 top-level published manifests say `source_snapshot_id='dev'`.

A synthetic reproduction changed a view's payload from 10 to 99 without changing its schema or snapshot string. `prepare_dataset` accepted the rerun and returned the frozen payload 10. Keeping a frozen artifact immutable is good; describing the current source as verified equivalent is not. This demonstrates a detection gap, not proof that every current artifact is stale.

The geometry metadata also reports all 84,272,874 rows as `observed_truth_count`, because its default predicate is `training_weight > 0`. The actual directly observed count is 37,105,511. The purely derived `pulled_opposite` dimension has zero directly observed rows but all 12,038,982 rows have positive weight. Actual target fit preparation applies stronger label filters; this metadata error does not by itself prove that unknown labels entered training.

**Decision:** bind datasets to source snapshot IDs, rendered transformation/code versions, schema and content digests, and upstream model artifacts. Distinguish eligible rows, directly observed labels, deterministic deductions, and inference rows in metadata. Preserve immutable snapshots while making reuse checks honest about what they verify.

Evidence: `bc/python_models/statistical/datasets.py:230-268,283-287,306-325,348-388`; `dataset_registry.py:33-39,100-123`; `bayes/training.py:3084-3107`, under the same package. Frozen-frame counts and the stale-reuse probe are in the evidence bundle.

### 5. High for interval claims: current coverage does not validate the fitted predictive distribution

Run-expectancy coverage adds Gaussian sampling noise using the held-out outcomes' own sample standard deviation, then combines it with a Gaussian approximation to the parameter posterior. It does not generate held-out outcomes from the fitted negative-binomial likelihood and its dispersion posterior.

This can be a useful studentized check of mean agreement. It cannot establish that the fitted model predicts dispersion, skew, or tails correctly. Calling it posterior-predictive validation overstates its scope. Likewise, adding finite-sample noise is the right conceptual response to comparing empirical frequencies with parameter intervals, but approximate marginal checks are not a substitute for validating the joint model.

Dependence matters too, although it should not be exaggerated: in 5,136 held-out run-value cells with at least 25 events, the ratio of inning-cluster standard error to independent-event standard error had median 1.014, 90th percentile 1.139, and maximum 1.792. The issue is modest in a typical cell and material in some tails.

Linear-weight intervals independently pair run-expectancy posterior draws with Dirichlet draws of transition frequencies even though both quantities are learned from overlapping event data. That factorization omits their sampling covariance. Its effect on interval width may have either sign and was not quantified here. Point estimates need not be discarded because their interval calibration is incomplete.

**Decision:** validate predictive intervals from the fitted posterior sampling law; use inning/game resampling as a dependence-aware comparator. For linear weights, recompute both ingredients in the same resamples or fit a coherent joint model. Check coverage and interval width by sample size and era before treating those HDIs as validated.

Evidence: `bc/python_models/statistical/hdi_coverage.py:469-529`; `models/run_values.py:81-93`; `linear_weights_estimated.py:141-176,357-367,418-420`, under the statistical package; `bc/models/intermediate/coverage/linear_weights_estimated.py:143-175`.

### 6. Medium: “weak identification” is currently a sampler-quality flag

`weak_identification_flag` is derived from R-hat, ESS, and divergences, then repeated across every row in an artifact. Those measure computation. A weakly identified effect can have excellent sampler diagnostics under proper priors; a scientifically informative model can mix poorly. The flag is also not sensitive to local historical support.

There is a concrete example in fielding-credit effects. The intercept and each categorical effect are constrained across positions, but not across the factor's levels. For a position vector `c` summing to zero, adding `c` to the intercept and subtracting it from every level of one factor leaves all likelihood predictions unchanged. Proper priors make the posterior proper, but the decomposition is prior-dependent. This is not proof that dense-cell probability predictions are bad.

**Decision:** report sampler diagnostics separately from identification/support. For effect interpretation, use reference coding or constraints across both factor levels and classes, preserving and checking the intended prior predictive distribution. For published event estimates, assess rare/unseen contexts and prior sensitivity before deciding a refit is worth doing.

Evidence: `bc/python_models/statistical/validate.py:469-498,535-625`; `bayes/training.py:3075-3081`; `bayes/manifest_ingest.py:50-88`; `models/credit.py:58-74`; `models/ball_handler.py:46-62`.

### 7. Medium: the estimands and comparators should be simpler and more explicit

The park model offsets by realized plate appearances. Its park effect is a conditional runs-per-PA rate ratio, not automatically a total-game scoring effect. Plate appearances are themselves affected by the scoring process. The broader wording in the public estimand description should be narrowed unless a different exposure or joint opportunity model is adopted. Do not silently swap denominators: that would change the question being answered.

For retrospective geometry, the deep “pre-event only” rule excludes measurements that may legitimately be known after the play: result family, outs, and some fielding evidence. Those are not automatically target leakage in historical reconstruction. Some are recorded only when the target is recorded, so availability and source coupling must govern their use. The Bayesian geometry model already uses result family.

A newly computed add-one-smoothed decade × result-family frequency baseline on location-side primary TEST achieved cross-entropy 0.98668 versus 1.05220 for the TRAIN marginal, an improvement of 0.06552 nats over 753,742 rows. This is not a head-to-head win over the published Bayesian model: its test population and training budget differ. It establishes that a cheap, relevant comparator exists and should be included.

The current multiclass ECE pools maximum-confidence correctness and cannot certify all the probabilities that users sum. An isolated example returned ECE zero for predicted class shares `[0.50, 0.49, 0.01]` with empirical shares `[0.50, 0.00, 0.50]`. Classwise and subgroup calibration are different requirements; see [Gupta and Ramdas](https://arxiv.org/abs/2107.08353).

**Decision:** define each output's target quantity and available-information contract, then compare unconditional frequency, contextual frequency, a regularized categorical model, and the existing Bayesian model on identical folds. Use log loss, Brier score, classwise calibration, aggregate error, and clustered uncertainty on differences. Add deep proposals only if they improve those results.

Evidence: `bc/python_models/statistical/models/_park_factor_data.py:170-189`; `models/park_factor.py:161,205-228`; `docs/estimated-models.md:290-300`; `deep/feature_layout.py:25-48,67-93`; `models/_geometry_data.py:125-131`; `calibration.py:62-105`. Exact baseline SQL and the ECE counterexample are in the evidence bundle.

## Disposition by family

| Family | Evidence worth retaining | What I would permit the evidence to support |
| --- | --- | --- |
| Observation propensity | Recorded-game AUC and calibration; explicit source-status inputs | Describing recording propensity within the supported event population. Not an identified MNAR correction. |
| Geometry | Location dimensions beat marginal log loss; trajectory has larger measured lift | Exploratory reconstruction distributions with support and sensitivity disclosures. Revalidate trajectory without shared target-supervised pretraining. |
| Fielding and handler | Putout log loss 1.3574 versus 1.8462 oracle held-out marginal comparator; held-out ECE 0.0233 | Useful probability vectors in validated contexts. Missing-credit population and aggregate constraints need their own acceptance evidence. |
| State transitions | Current filtered population; log-likelihood gain 0.00560/event | Descriptive transition probabilities, with structural-support and sparse-slice checks. |
| Run expectancy | RMSE 0.08844 versus 0.09505 baseline | Descriptive shrinkage of cell means. Interval validation remains provisional. |
| Park factors | Small predictive improvement: log-likelihood gain 0.00504/team-game | Conditional runs-per-PA effects. A causal or total-game interpretation needs additional design. |
| Linear weights | Draw-based propagation and finite-count uncertainty are useful foundations | Standard marginal point weights; joint uncertainty calibration remains exploratory. |
| Deep proposals | Real fold-exclusion logic in the downstream runner | Experimental inputs until upstream fold isolation, dependency provenance, and baseline validation are demonstrated. |

These scores have different units and evaluation populations; the table is not a cross-family ranking by metric magnitude. The run-expectancy, transition, and park comparators are posterior plug-in ablations, not separately fitted simple models: they remove a component while retaining other quantities estimated in the full fit. The fielding marginal comparator uses evaluation-label frequencies, making it an optimistic oracle rather than a deployable TRAIN-fitted baseline. Small gains can be useful, but these comparisons and the absence of paired, cluster-aware uncertainty estimates do not establish that each gain justifies its model's complexity.

## What to do next, in order

1. **Repair the evidence contract.** Close the empty/non-finite metric bypasses, resolve pointers through their actual manifest identities, implement the baseline comparator, and bind verdicts to immutable inputs. Split numerical status from predictive/transport/identification status. Acceptance: the counterexamples in this audit fail for the intended reasons; every selected dependency has an explicit validation disposition. Changing production labels or artifacts requires a separately reviewed release.
2. **Build one clean reconstruction benchmark.** Use location side to establish cheap contextual baselines and trajectory to exercise source selection and partial-truth constraints. Freeze the population, allowed measurements, outer folds, and masking scenarios. Fit the simple baselines and existing no-deep Bayesian models on the same training data. Persist labeled held-out predictions and evaluate era/scorer/missingness slices with game-cluster resampling. Acceptance: a reproducible comparison that says where each model helps, where it fails, and where there is no empirical support.
3. **Repair the interval claims on the useful aggregate outputs.** Replace the run-expectancy coverage claim with a genuine predictive check; measure dependence and joint linear-weight uncertainty. Clarify the park estimand. Acceptance: independently computed coverage/width diagnostics by era and sample size, and documentation matching the quantity actually fitted.
4. **Choose extensions from measured residual failures.** If coarse era terms improve missing-population performance, add them before richer embeddings. If audited post-play clues improve reconstruction, admit them with availability masks. If deep proposals still offer worthwhile incremental gains, cross-fit the entire pretrain pipeline. If fielding effects need interpretation or rare-level predictions are prior-sensitive, recode and refit with prior checks. Expand sample budgets only after a learning curve shows what they buy.

I would defer advancement expansion, pitch-sequence deep models, player-level fielding effects, a larger pretrain architecture, and broad refitting. I would not spend the next run merely turning smoke-budget MNAR experiments into larger versions of the same oracle exercise. I would also avoid treating publication logistics as evidence of model quality, or tightening R-hat thresholds as a solution to historical identification.

The intended outcome is a smaller set of clearly named estimates whose validation matches their use, with uncertain historical reconstruction explicitly represented as uncertain. The iterative model-checking approach is consistent with [Bayesian Workflow](https://arxiv.org/abs/2011.01808); the ordering above comes from this repository's code, artifacts, and audit experiments.
