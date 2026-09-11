# Historical geometry stress test

September 11, 2026. Completed development tests of the frozen trajectory interaction and corrected global-side references. Production and canonical artifacts remain unchanged. The experiment follows the [frozen protocol](historical-geometry-stress-protocol.md), with [input bindings](historical-geometry-stress-inputs.json) and [machine-readable results](historical-geometry-stress-2026-09-11.json).

## Decision

Do not advance either model to historical reconstruction or publication. Both pass their original within-population development comparison, but both fail the frozen backward-transfer screen with healthy sampling. Trajectory is worse than its usable contextual baseline; side improves proper scores while materially misestimating historical class shares. Preserve the current models as research comparators and address temporal transport and cohort calibration before opening confirmation.

## What was tested

Each target retains its original 100,000-event TRAIN selection and full TEST extract. The recorded-context reference reuses its accepted posterior. The backward refit removes every selected TRAIN event before 1988. The scorer refit removes entire games mentioning any of 106 deterministically selected cleaned scorer/source-credit identities. Both refits keep the original model specification, priors, and sampler settings fixed. Every scenario is scored with recorded context and with the same predeclared empirical joint masking procedure.

Side means the corrected recorded global-side category: All, Left, Middle, or Right. All is a coarse source category, not a measured direction; inferred fielder side and angle defaults are never scoring truth. Trajectory retains the frozen five-class representation and Bunt remapping. These are checks of recorded categories, not independently measured ball geometry.

The comparator is an add-one contextual baseline fitted on exactly the retained TRAIN rows. It prefers decade/result cells and falls back to result, decade, then marginal frequencies. This usable backward-transfer fallback was declared before scoring. It differs from the old reference runner's uniform fallback; baseline comparisons below use this experiment's comparator throughout.

All six scenarios passed their numerical checks. The four new refits each use four chains, 1,000 warmup and 4,000 saved draws per chain, with zero divergences. Numerical diagnostics and predictive screens are separate: healthy sampling does not rescue failed predictions. Small smoke fits checked both targets and all scenarios before the full fits; smoke predictions were not scored.

## Predictive results

| Target / scenario | Model log loss | Baseline log loss | 95% gain interval | ECE | Max share error | Screen |
|---|---:|---:|---|---:|---:|---|
| Trajectory / reference | 1.191523 | 1.202406 | [0.010542, 0.011230] | 0.007105 | 0.35 pp | Pass |
| Trajectory / backward | 1.688360 | 1.614666 | [-0.077708, -0.069192] | 0.092780 | 14.37 pp | Fail |
| Trajectory / scorer | 1.200594 | 1.211842 | [0.010433, 0.012015] | 0.011297 | 0.97 pp | Pass |
| Side / reference | 1.094595 | 1.103590 | [0.008570, 0.009425] | 0.013937 | 0.07 pp | Pass |
| Side / backward | 1.080878 | 1.123647 | [0.041218, 0.044225] | 0.076833 | 12.35 pp | Fail |
| Side / scorer | 1.093553 | 1.102509 | [0.008067, 0.009874] | 0.015677 | 0.51 pp | Pass |

| Target / scenario | TRAIN / TEST events | Model / baseline Brier | R-hat / bulk ESS / tail ESS |
|---|---:|---:|---:|
| Trajectory / reference | 100,000 / 922,812 | 0.646729 / 0.652720 | 1.0204 / 307.2 / 478.9 |
| Trajectory / backward | 77,039 / 211,340 | 0.814964 / 0.802058 | 1.0029 / 1770.5 / 2850.7 |
| Trajectory / scorer | 77,731 / 204,657 | 0.653532 / 0.658946 | 1.0174 / 351.1 / 754.5 |
| Side / reference | 100,000 / 753,742 | 0.644828 / 0.651245 | 1.0126 / 435.5 / 833.2 |
| Side / backward | 92,072 / 59,759 | 0.638987 / 0.668458 | 1.0021 / 2532.8 / 3868.6 |
| Side / scorer | 79,208 / 156,570 | 0.644548 / 0.650932 | 1.0041 / 1991.2 / 3376.1 |

Joint masking preserves every pass/fail verdict. The largest absolute change in overall model log loss across the six scenarios is 0.000092. Exact masked scores, Brier results, class counts, share intervals, support groups, and reliability bins are retained in the artifacts; the combined JSON retains compact summaries of every slice and full overall counts, with detailed subgroup count/share intervals and reliability bins in the hash-bound scenario reports.

Lower log loss and Brier are better. Gain intervals are 500 paired whole-game bootstrap intervals, holding TRAIN fits fixed. The frozen overall screen also requires classwise ECE at most 0.05 and maximum absolute class-share error at most 0.02. It is a development screen, not a publication gate. Historical subgroup findings remain consequential even when the overall screen passes.

Trajectory's backward failure is substantial: 185,385 of 211,340 historical events (87.7%) have unseen decade/result combinations. The existing zero-effect policy then removes both decade and result information, while the baseline retains its result-only frequencies. This is an architectural weakness of the combined factor. It is not the entire explanation: the 25,955 context-supported events in the historical portion of the 1980s still have ECE 0.0713 and maximum class-share error 15.21 percentage points. Fixing the fallback alone would not establish historical calibration.

Trajectory's scorer-holdout overall pass also hides material historical error. On the pre-1988 slice, line-drive share is underpredicted by 4.31 percentage points. The 1950s holdout cohort contains 6,113 events from 435 games, with ECE 0.1069 and line-drive underprediction of 23.91 percentage points (game-bootstrap interval: -26.05 to -21.77 points). The baseline also underpredicts that share by 24.05 points: the cohort's recorded line-drive share is 43.58%, versus model expectation 19.67%. This is a failure shared by both transport assumptions, not evidence that recorded historical labels reveal the unbiased underlying baseball distribution. The cohort clears the reporting support threshold and cannot be dismissed as empty or negligible. The full reference itself has a smaller 1920s fly-share error of -2.27 percentage points. These are diagnostic subgroup comparisons on inspected TEST, not newly imposed formal rejection thresholds or multiplicity-adjusted discoveries.

Side demonstrates why relative predictive improvement is insufficient. In backward transfer, its log loss improves from baseline 1.123647 to 1.080878, with positive paired gain interval [0.041218, 0.044225]. Yet ECE is 0.076833 and Middle share is overpredicted by 12.35 percentage points. There are 48,570 events with unseen era levels out of 59,759. Even its 11,189 context-supported historical events have ECE 0.053119 and Middle-share overprediction of 5.24 points. The failure concerns calibration as well as unseen-era handling.

Side passes the overall scorer-holdout screen: log loss 1.093553 versus baseline 1.102509, ECE 0.015677, and maximum share error 0.51 percentage points. Its aggregate pre-1988 result also looks reasonably calibrated (ECE 0.043640, maximum share error 0.59 points), but opposing decade errors cancel. The 1910s cohort regresses against the baseline, with log-loss gain interval [-0.033573, -0.005847] and Right-share underprediction of 5.26 points. The 1950s cohort has ECE 0.114190 and Left-share underprediction of 6.03 points across 1,253 events and 343 games. The 1930s through 1970s cohorts all exceed ECE 0.05. These findings reinforce the need for cohort checks alongside an overall or pooled historical pass.

## What the missingness and source tests establish

The natural TRAIN profile includes unknown, missing, and default-code targets with positive training weight; fielder-derived targets are excluded. All observed source-family labels are PlayByPlay/play_by_play/observed. Consequently there is no independent acquisition-source category to hold out. Cleaned scorer/source-credit identities are a proxy and can denote newspapers or numeric source codes, not necessarily individual scorers.

The profile contains 2,795,727 naturally unknown trajectory events and 615,501 naturally unknown side events. Five reference context fields are complete. Only batter handedness varies in availability: 5,542 unknown-trajectory events and 5,008 unknown-side events lack it. A stable game-level draw therefore produces only the all-available or hand-missing pattern. Calendar and rule-era context remain available where their inputs are known. This test cannot establish robustness to richer context loss or to outcome-dependent target recording.

Multi-scorer games retain every cleaned identity for whole-game exclusion. Exact single-scorer masking uses scorer/decade frequencies; multi-scorer signatures use independently counted decade pools. Provenance inspection found raw-to-clean identity collisions in the first profile. Before any stress fit or score, version 2 summed collision weights and normalized each game's source contribution, conserving the original event mass. Version 1 was never used for a fitted or scored result. The trusted input contract pins version 2's manifest and all source extracts.

The [support inventory](historical-geometry-support-2026-09-11.json) uses fractional scorer attribution to avoid double counting. Across all scorer-decade groups, the threshold of 500 weighted recorded TEST events and 50 recorded games covers 56.63% of naturally unknown trajectory TRAIN mass and 35.52% of side mass. The remaining 43.37% and 64.48% have insufficient observed support under that rule; this does not imply source absence. Among the qualifying groups, those selected for this holdout account for only 9.19% and 1.32% of total unknown mass, respectively. Thus the deterministic scorer holdout is particularly weak evidence for naturally unknown side records. Support counts labeled TRAIN in that inventory describe the full primary TRAIN pool, not the 100,000-row fit sample.

## Confirmation and uncertainty boundaries

The [confirmation audit](geometry-confirmation-boundary-2026-09-11.md) found legacy validation exposure for every currently eligible primary VALIDATE game. A globally untouched confirmation claim is unavailable. The current reference family uses no pretrained inputs and has not used those games, allowing a narrower prospective component-local reserve.

The label-free eligibility query was regenerated and its candidate and reserve digests verified. It freezes 6,105 game IDs, all excluded from this experiment's fitting, masking, and scoring. No reserve target classes were selected or scored. Preserve this boundary until the next model, calibration procedure, metrics, and thresholds are locked; then score it once. Reusing legacy fitted components would require another contamination audit.

Expected class counts in these results are sums of predictive probabilities. Their bootstrap intervals describe error across held-out games conditional on fitted TRAIN models. They are not posterior predictive intervals for historical totals and omit training-sample uncertainty. Recorded historical truth is selected by the recording process; passing these tests would not identify naturally unrecorded outcomes. Entirely absent games remain outside the event-level claim.

## Next work

Keep the current references as development comparators and leave confirmation sealed. The next scientific experiment should address transport and cohort calibration:

1. Freeze a TRAIN-only blocked temporal comparison with a persistent result-family main effect plus partially pooled era deviations. Include an ordered-era candidate only with a clear extrapolation prior. An unseen era must retain result information and carry explicit uncertainty; silently treating its entire effect as a known zero is inadequate for reconstruction.
2. Evaluate historical class-share error by era and scorer cohort alongside proper scores. Fit any calibration layer only inside the training folds. Use the unknown-record support profile to design stronger historical source-proxy coverage, particularly for side, and retain an explicit unsupported category. Passing a modern-dominated overall screen must not authorize historical totals.
3. Freeze outcome-dependent recording sensitivity scenarios separately from context masking. Recorded labels cannot identify their own selection bias; report the range under stated assumptions rather than claiming that these masks validate the naturally unknown population.
4. Once the model and calibration rule pass the predeclared development requirements, lock them and use the component-local reserve once. Then validate joint aggregate uncertainty with game dependence and posterior predictive checks before any publication migration.

The present results do not justify more draws, more pretraining, or a larger fit merely to seek a better overall score. Sampling is adequate, and the failures concern extrapolation, cohort composition, calibration, and limited truth support. Unsupported historical estimates should remain unavailable or explicitly sensitivity-qualified.

## Evidence and verification

Full artifacts, including posteriors, retained TRAIN/TEST rows, aligned probabilities, masks, support flags, diagnostics, reliability bins, code archives, and reports:

- Trajectory: `artifacts/statistical/backtests/historical_stress/20260911-trajectory-full-v1`.
- Side: `artifacts/statistical/backtests/historical_stress/20260911-side-full-v1`.
- Frozen missingness/source profile: `artifacts/statistical/backtests/historical_stress/20260911-profile-v2`.
- Reserved IDs and verification: `artifacts/statistical/backtests/historical_stress/20260911-reserve-v1`.

The combined evidence verifies every scenario artifact hash, current-versus-archived implementation hashes, and protocol/input bindings. Runtime versions were captured during the trajectory refits after protocol freezing; they are supplemental runtime evidence, not a pre-fit capture. All run commands used `uv run --no-sync`.

The statistical suite passed 1,062 tests; 18 slow tests were excluded. It emitted eight third-party Keras/NumPy deprecation warnings. Ruff lint/format and focused strict type checks passed for all five new Python files, with zero type errors or warnings. Independent review found no unresolved consequential issue in truth isolation, whole-game exclusions, multi-scorer linkage, baseline fitting, probability pairing, fractional mask weights, or pinned evidence. No tests failed.

Production database, state, published catalog, and published-data identities match the earlier materialization evidence. This compares size/mtime, sampled database bytes, and the published-data metadata inventory; it is not a full database checksum. No production promotion or remote push was performed.

To reproduce either target, use a new output directory and the pinned local inputs:

```sh
PYTHONPATH=bc uv run --no-sync python -m python_models.statistical.backtests.historical_stress \
  --profile-root artifacts/statistical/backtests/historical_stress/20260911-profile-v2 \
  --target trajectory \
  --output-root artifacts/statistical/backtests/historical_stress/NEW-RUN-ID
```

Use `--target location_side` for side and `--smoke` for a bounded unscored check. The runner refuses an existing output directory or altered frozen inputs.
