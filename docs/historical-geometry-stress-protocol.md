# Historical geometry stress protocol

Frozen September 11, 2026 before stress fitting or scoring. These tests evaluate recorded labels under specified context masks and supervised training exclusions. They do not identify MNAR distributions or reconstruct absent games. Primary TEST has already been inspected and remains development evidence.

## Population and reservation

Run trajectory first, then corrected global location side. Reuse the exact 100,000-event TRAIN selection and full TEST extract from each accepted reference. Do not enlarge the training selection. For trajectory, retain its five-class Bunt remapping and decade-by-result interaction; for side, retain the corrected All/Left/Middle/Right classes and six additive factors. All is a coarse source category, not a measured direction.

Regenerate and verify the label-free candidate and reserve digests in `geometry-confirmation-boundary-2026-09-11.json`. Preserve the resulting 6,105 game IDs under `geometry-confirmation-reserve-v1`. Exclude them from every training, masking, calibration, and scoring input in this experiment. They are unused by the current no-learned-input reference family, but legacy validation exposure prevents a globally untouched claim. No reserve labels may be selected or scored here.

## Natural missingness design

Use only primary TRAIN rows with natural unknown/missing target status and positive training weight to estimate joint context-availability patterns. Tabulate derived targets separately; never treat their deduced values as recorded truth. A context mask has six bits ordered decade, result family, base state, outs, alignment regime, and batter hand. Missing/null/unknown context can remove a feature; reliable deterministic context is not missing merely because it is derived. Calendar and rule-era features remain available where their inputs are known.

Sample one joint pattern per TEST game using SHA-256 of fixed seed, target, and game ID. Use event-count-weighted pattern frequencies within decade and cleaned scorer where available; otherwise pool by decade, then globally. Persist the pattern and pooling route. Apply the same pattern to every event of that game and preserve already-missing context. This conservatively imposes game-level co-missingness; the profile estimates marginal pattern frequencies, not the unobserved within-game joint law. Repeated mask realizations and outcome-dependent missingness sensitivity are outside this first stress test.

Games can list multiple scorers or source credits. Retain the full cleaned identity list and exclude a whole game if any listed identity is selected for the scorer holdout. Exact conditional masking uses a single-scorer signature; multi-scorer signatures fall back to exact decade-pooled counts. Scorer-profile weights split each game's event contribution across its source rows and sum collision shares when different raw rows normalize to the same identity. Decade pools count original events once, independently of scorer linkage. These rules were fixed from provenance inspection before any stress fit or score.

Truth lives only in the held-out scoring frame. Recompute interaction features after masking. Neither masking nor a baseline may inspect TEST truth. Scorer metadata is a source-process proxy, not an authenticated acquisition identifier: all recorded source-type labels in the current frame are PlayByPlay.

## Scenarios

1. **Recorded-context reference:** reuse the accepted posterior, score all frozen TEST events with recorded context.
2. **Empirical joint mask:** reuse the same posterior, score all frozen TEST events with natural-pattern context masking.
3. **Backward era transfer:** refit on selected TRAIN events from 1988 onward only; evaluate all pre-1988 TEST events with both recorded and masked context. No pre-1988 labels enter this fit. Report unseen decade/interaction levels explicitly; their model contribution follows the existing zero-effect policy.
4. **Whole-scorer exclusion:** eligible cleaned, known scorer identities have at least 20 primary TRAIN games. Select identities whose SHA-256 of `historical-stress-scorer-v1:` plus scorer, interpreted as an unsigned full-digest integer, is divisible by 5. Exclude their entire games from the supervised refit, then score their TEST events with recorded and masked context. Unknown scorer identity is not one person and is excluded from the held-out scorer set. Report historical support; this is not an acquisition-source holdout. Natural missingness design uses unlabeled TRAIN unknown-target context, including the selected scorers, and is distinct from supervised target fitting.

Fit the model on the retained subset of the original TRAIN selection in each refit scenario. Keep the model specification and priors fixed. No learned representations, propensity, handler posterior, new covariates, or recalibration are introduced. All new full refits use four chains, 1,000 warmup and 4,000 saved draws per chain, nutpie, target acceptance 0.95, maximum depth 12, seed 20260911. Smoke uses a stable subset of at most 3,000 TRAIN and 5,000 TEST rows, two chains and 50 warmup/saved draws. Numerical repairs may depend only on diagnostics and must be frozen under a new artifact ID before execution.

## Comparator, scoring, and interpretation

Fit add-one contextual class frequencies using exactly the same retained TRAIN events. An observed decade/result cell is preferred; unseen cells fall back to result-only frequencies, then decade-only frequencies, then the TRAIN marginal. This declared fallback tests backward transfer against a usable contextual baseline rather than an arbitrary uniform predictor. Record the route. Apply masked inputs to the baseline as well as the model. This comparator differs from the earlier reference's uniform fallback and must be named accordingly.

Save aligned event truth, probabilities, mask pattern, scorer, baseline fallback, and model-support flags. Score log loss, multiclass Brier, 15-bin classwise ECE, expected and observed class counts/shares, and maximum absolute class-share error. Report overall, recorded pre-1988, decade, and masking/support groups; small groups below 500 events or 50 games are descriptive and insufficient for a validation verdict.

Use 500 paired whole-game bootstrap repetitions with event weighting and fitted TRAIN models fixed. Report confidence intervals for loss improvement and class-share bias. The screening criteria are positive lower bounds for both loss gains, overall ECE at most 0.05, and maximum absolute class-share error at most 0.02, with R-hat at most 1.05, bulk/tail ESS at least 100, and zero divergences. These are per-scenario development screens, not a publication gate. Unseen contexts, unavailable source identities, and unsupported eras remain explicit limitations even if a score passes. Do not tune to these results.

Archive the protocol, exact implementation, source/report hashes, reservation IDs, selected rows, masks, priors, posteriors, diagnostics, and scores. Use new artifact directories and read-only source connections. Production and canonical pointers stay unchanged.

Blocked evaluation should match the intended transfer question rather than merely produce another random split; see [Roberts et al. (2017)](https://www.wsl.ch/lud/biodiversity_events/papers/Roberts_et_al-2017-Ecography.pdf). The separation of computational diagnostics, predictive checks, and model revision follows [Bayesian Workflow](https://arxiv.org/abs/2011.01808). The specific design and thresholds here are project decisions, not results established by those references.
