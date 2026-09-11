# Airborne recording-regime posterior development

Experiment: `geometry-air-regime-posterior-v1`. This specification precedes predictive scoring of this revision. It is development on the already inspected 373 fitting games, not independent confirmation. The 121 deferred angle outcomes and 6,105-game historical reserve remain sealed. The failed pooled experiment remains an archived comparison.

## Target and applicability

Use `trajectory-air-standard-v1`: preserve recorded Ground versus Air and standardize only airborne subtypes. Preserve original labels and the separate bunt attribute. Ground preservation is a deterministic convention and contributes nothing to airborne predictive scores. Unknown broad type remains unsupported.

The estimand is the distribution of the standardized reference category conditional on the available recorded airborne label, result family, and represented recording regime. It is not an independently verified physical flight category or an estimate of a named scorer's behavior. Numerical Statcast references can include estimates; row-level measurement origin is unknown in this acquisition.

Use the content-bound development frame from `20260911-air-development-full-v1/coverage_frame.parquet`, SHA-256 `cfdf2918180812f6b1333a76a2875f87cd75b581fab07c35bb23dd648c4e3e70`. Retain all 19,436 rows in coverage accounting. Fit and score only the 9,869 eligible local-Air rows with resolved standardized airborne references. Retain all 899 unresolved local-Air cases in applicability denominators. No reference angle, Statcast category, target status, or eligibility flag becomes a prediction feature.

Regime `early` means the observed seasons 2015 and 2019; `late` means 2023 and 2025. These are descriptive groups suggested by the previous development diagnostic, not identified dates or causes of a recording change. All other seasons are unsupported. A represented predictor cell with no observations in one regime can borrow from its other regime; a regime entirely absent from training is unsupported for ordinary predictions.

## Likelihood and priors

Class order is Fly, LineDrive, PopUp. Fit four separate arms on identical eligible training rows, with cell keys respectively: no predictors, result family, recorded subtype, and recorded subtype by result family. All four arms receive the same regime information. Within each arm, cells have independent priors:

`q_cell ~ Dirichlet(1, 1, 1)`

`p_cell,regime | q_cell ~ Dirichlet(kappa * q_cell)`

`counts_cell,regime | p_cell,regime ~ Multinomial(n_cell,regime, p_cell,regime)`

The primary concentration is 30; fixed sensitivities are 3 and 300. They cannot replace the primary based on predictive results. At the balanced global composition, the conditional standard deviation of a regime's class probability is about 23.6, 8.5, and 2.7 percentage points respectively. The global prior remains broad: each class has marginal mean one third and standard deviation about 23.6 points. These are priors on label translations, not assertions about league-wide batted-ball prevalence. Prior predictive simulation must verify the implemented distribution and show game-scale count dispersion before scoring.

Each valid unseen cell receives its own zero-count hierarchical prior, with explicit `prior_only_cell` status. It must not silently substitute a separately fitted reference arm. Result families are nonempty strings, including an explicit unknown level; airborne labels must belong to the declared three classes. Missing fine labels are unsupported by arms requiring that label. Evaluate the result-only arm separately for the fine-label-masked task. This separates conditional estimands and avoids pretending independent arm posteriors form one joint posterior.

## Numerical computation and shared uncertainty

Integrate regime probabilities analytically and approximate the two-dimensional posterior for each global composition with Gauss-Legendre quadrature on the simplex, including its Jacobian. Compare all global and regime first and second moments at orders 64 and 128. If the maximum absolute difference exceeds `1e-5`, compare 128/256, then 256/512. Use the higher order from the first passing pair. If none passes, stop that fit with a numerical failure. Refinement responds only to integration accuracy, never predictive scores. Archive every comparison and selected order. This check establishes moment stability, not a proof of tail accuracy.

Draw 4,096 posterior samples per fitted cell using deterministic SHA-256-derived seeds from experiment, fit, arm, concentration, and cell identity, with root seed 20260911. Sample one global composition per draw, then conditional regime probabilities. All events using a cell and regime share the corresponding probability draw. Different cells are independent under the stated model. Independently fitted arms and folds have distinct `fit_id` values: matching draw numbers across folds do not constitute a joint posterior.

For each held-out game, draw multinomial counts within each cell and regime using its held-out event count and shared probability draw; sum cell counts. Also generate held-out season totals within each fit using the same parameter draws. Future events are conditionally independent within a cell under this model; unmodeled within-game effects may invalidate intervals and must be tested. Never replace these draws with independent draws from posterior mean probabilities.

Validate analytic prior moments, class and regime permutations, simulation recovery, normalized probabilities, deterministic replay, shared covariance, and count conservation. Compare posterior predictive interval endpoints at the selected and next available quadrature order on the unscored smoke cases using Monte Carlo uncertainty; investigate material discrepancies before full execution. Orders beyond 512 require a declared numerical revision.

For simulated recovery, generate 200 independent datasets per concentration from the declared hierarchy with two regimes of 20 and 200 training events and 100 future events per regime. Use 4,096 posterior samples and root seed 20260911. Report equal-tailed 95% coverage and width separately for each regime's latent class probability and future class count, plus exact count conservation. Require at least 90% coverage in each concentration-by-regime-by-class check as a necessary computational screen, and report binomial uncertainty rather than claim that this establishes exact calibration. A five-dataset-per-concentration smoke is operational only. Simulation uses no held-out baseball targets and does not establish model adequacy for real recording practices.

## Development splits and comparisons

Use the existing five whole-game folds, five whole-park folds, and four leave-one-season-out folds without changing their assignments. Leave-one-season-out uses the other observed season in the same regime and tests limited within-regime transfer. Report each held-out fold and each season; no split establishes pre-2015 transport. An entire-regime holdout is an explicit unsupported-regime behavior test, not scored evidence of historical prediction.

Retain the old pooled predictions as an additional comparison on identical event keys. Within the new model, compare the candidate to both the result-only and recorded-only arms with the same concentration and regime structure. No fit uses an evaluation target to construct counts, priors, support, or fallback behavior.

## Decision rules

Preserve the previous primary development screen: positive lower endpoints for both 95% paired whole-game bootstrap gain intervals for log loss and Brier against both required references, in every split family. Use 500 paired bootstrap replicates with seed 20260911. Require classwise 15-bin ECE at most 0.05 and absolute class-share bias at most 0.02 overall and in each season with at least 30 games and 500 eligible events. Show smaller slices without declaring a pass. Report park and header-scorer slices descriptively without interpreting them as identified effects.

In addition, report equal-tailed 90% and 95% posterior predictive count coverage and interval width by class, split family, and season. A necessary development screen is at least 90% empirical coverage for nominal 95% intervals in every supported class-by-family and class-by-season slice. This permissive screen can reject poor intervals; passing it does not establish nominal calibration. Report whole-game bootstrap intervals for coverage and Monte Carlo uncertainty, and retain fold membership because OOF fits share training data. Show held-out season aggregate coverage separately; four seasons cannot establish its nominal rate. Simulated recovery under the declared model must support the uncertainty implementation before any empirical coverage claim.

A concentration sensitivity that reverses the primary predictive or calibration verdict is a robustness failure requiring investigation. Do not select the best sensitivity. Report both score uncertainty conditional on fits and posterior predictive uncertainty with their different meanings.

Repeat unresolved-reference class-share bounds and fixed-prediction adversarial relabeling tipping calculations for this revision, by represented regime. These are sensitivity assumptions, not estimated error rates. Passing complete-case development screens cannot clear unknown measurement-origin or selection risks.

## Artifacts and remaining gates

Before scored execution, bind this protocol, all executable source dependencies, target contract, input hashes, package versions, and seeds. Write checkpoints and partial results. Retain training/evaluation game IDs, cell counts, numerical diagnostics, posterior specifications, event probability vectors and statuses, per-game scores, predictive count draws with fit IDs, coverage tables, and terminal status. Run a small real-data smoke before the full experiment.

This experiment cannot by itself clear independent modern confirmation, genuinely missing-label validity, historical transport, reference measurement quality, scorer authorship, or the separate side/location model. Original-source label reconstruction is a separate output still requiring its own model and validation. A successful modern translation is not authorization to publish historical reconstructed facts.
