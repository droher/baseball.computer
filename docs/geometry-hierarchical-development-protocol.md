# Hierarchical geometry development protocol

Frozen September 11, 2026 before candidate evaluation. This is a development experiment within primary TRAIN. It does not consume the component-local confirmation reserve or establish identification of naturally missing outcomes.

## Candidate and estimand

Estimate recorded categorical trajectory or global side conditional on result family, decade, starting base/out state, alignment regime, and batter hand. The missing-outcome estimand requires subsequent source/selection sensitivity evidence. Preserve the existing five-class trajectory mapping and four-class corrected-side ontology.

Logits are population class intercept + result-family main effect + era main effect + era-by-result deviation + four context effects. The class intercept has zero-sum Normal scale 1.5. The result main effect has zero sums over result and class, scale 0.5. Era main effects are noncentered unit class-zero-sum Normal vectors times a learned HalfNormal(0.5) scale. Each era's interaction is a unit Normal matrix with zero sums over result and class, times a learned HalfNormal(0.5) scale. Context effects are class-zero-sum Normal vectors of scale 0.5 per level. Unlike the old reference, effects are not constrained to sum to zero over observed eras or context levels. Proper population priors separate likelihood-confounded finite-group averages; individual components are not claimed to be likelihood-identified or causal.

Use a fixed result/class domain from the pinned original reference fit contract, never from evaluation truth. Construct the full observed-era by declared-result tensor, including combinations with no fitted labels. New eras get one shared class-vector draw and one shared result-by-class interaction matrix per posterior draw and era identity. New context levels similarly get shared population-prior draws at their declared fixed scale. Known pairs use posterior draws. Unknown result labels outside the declared ontology fail closed. Stable identity-derived random seeds preserve shared effects across event order, prediction chunks, and repeated calls. Average softmax probabilities over draws; do not softmax an average logit or replace unknown effects with zero.

## Data and development blocks

Use the read-only corrected source clone and restrict every new target query to `primary_fold='TRAIN'`. Validate selected-row equality and full-TRAIN class counts against the frozen prior extracts. Persist full game scorer lists; no event duplication through source linkage.

Assign game `inner_evaluation` if the first eight bytes of SHA-256 over UTF-8 `geometry-reliability-inner-v1:` plus game ID, interpreted unsigned big-endian, have remainder zero modulo five. All other games are `inner_fit`. Use the same rule for both targets. The fit pool is the original selected 100,000 TRAIN rows restricted to inner-fit games and the scenario. Evaluation uses all eligible observed primary-TRAIN events in inner-evaluation games and the scenario. Domain metadata may be fixed from the original TRAIN contract; no evaluation target labels fit encoders, parameters, baseline counts, or calibration.

Initial scenarios, in order:

1. `game`: fit inner-fit games and evaluate inner-evaluation games across all eras.
2. `backward_1988`: fit inner-fit games from 1988 onward; evaluate inner-evaluation games before 1988.

These are the first diagnostic comparison, not the entire reliability requirement. Subsequent predeclared historical interpolation, alternative cutoffs, source/scorer, and observation-process tests remain required. No recalibration layer or source effect is included in this candidate.

The comparator is the existing add-one contextual baseline, fitted on exactly the same retained rows, with decade/result, result-only, decade-only, then marginal fallback. Score both targets independently with log loss, multiclass Brier, 15-bin classwise ECE, expected/observed class shares, and paired game-bootstrap loss-gain intervals (500 repetitions). Retain historical, decade, and support slices. Report the same existing screen: positive lower 95% bounds for both overall loss gains, ECE <=0.05, and maximum class-share error <=0.02. Supported historical and decade cohorts must separately satisfy calibration criteria to support their historical use. Cohorts below 500 events or 50 games remain explicitly unsupported; their population mass remains reported. These are necessary development screens, not a sufficient confirmation/publication rule.

## Computation and checks

Run a short real-data smoke before invariant tests and before full fits. Smoke evaluations are not scored. Full fits use four chains, 1,000 warmup and 4,000 saved draws per chain, nutpie, target acceptance 0.95, maximum tree depth 12, seed 20260911. Require R-hat <=1.05, bulk and tail ESS >=100, and zero divergences. Any diagnostic-only sampling repair gets a new artifact ID before execution.

Check priors on observed and unseen-era probabilities and expected class shares. Test new-era shared effects, fixed ontology, absent pairs, normalized probabilities, row/chunk invariance, and class-sum constraints. Subsequent simulation recovery, prior sensitivity, coherent aggregate predictive draws, and interval coverage are still required even if these first scores pass.

Archive exact code, protocol, input and output hashes, runtime versions captured before fitting, fitted rows, evaluation rows, probabilities, support flags, diagnostics, and reports. Preserve every failed result. Production and confirmation remain unchanged.
