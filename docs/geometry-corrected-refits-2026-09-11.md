# Corrected geometry refits

September 11, 2026. These are research comparisons on previously inspected TEST games. Production tables, state, published pointers, and the public DuckLake surfaces remain unchanged.

Subsequent work: the [historical stress test](historical-geometry-stress-2026-09-11.md) executes the source/era checks proposed here and supplies the current decision. The results below remain the frozen within-population reference comparison.

## Corrected source materialization

The corrected geometry ledger and two modeling views are materialized through SQLMesh in an APFS copy-on-write clone, using schema `main_models__geometry_v2_research_20260911`. The clone attaches no publication catalog and executes no source-initialization or publication hooks. Its directory is now read-only. The [materialization evidence](geometry-materialization-2026-09-11.json) records source identities, SQL hashes, configuration, runtime versions, execution history, and validation.

The build covers 12,038,982 events from 1910–2025 across eight geometry dimensions. There are 5,049,150 directly recorded, source-mapped side labels: All 43,837; Left 1,521,031; Middle 2,038,699; Right 1,445,583. All remains an explicit coarse category and does not mean a precise field direction. Fielder-derived side and ambiguous angle defaults remain separate from training truth. Five provenance, class-domain, stale-input, angle-status, and uniqueness invariants pass.

Trajectory has exact event, raw-value, class, status, and sentinel parity with the frozen production dependency snapshot. The earlier source-correction probe saw 2,416 additional rows through fresher development dependencies; those rows are absent from this clone by design. This build isolates the target correction using the production dependency snapshot. It is not a source refresh or a new canonical wide-dataset export.

The first full build hit an audit memory limit at 24 GB. SQLMesh resumed the isolated build with a 30 GB limit and one DuckDB thread, and all audits completed. The original production database and state, published catalog, and published data identities stayed unchanged. The evidence distinguishes recorded size/mtime and sampled database hashes from a complete database checksum.

## Global location side

The [frozen protocol](geometry-reference-global-side-protocol.md) uses 100,000 selected TRAIN events, all 753,742 TEST events from 26,260 games, six additive context factors, fixed Normal priors, and no pretrained or other learned inputs. The reference and matched contextual baseline use the same selected TRAIN rows. The full contextual baseline uses all 3,539,235 eligible TRAIN events. The old angle scores are not scores for this corrected target.

| Predictor | TEST log loss | TEST Brier | Classwise ECE | Pre-1988 log loss |
|---|---:|---:|---:|---:|
| Matched contextual baseline | 1.103590 | 0.651245 | 0.001430 | 1.051277 |
| Full-TRAIN contextual baseline | 1.102860 | 0.650930 | 0.000798 | 1.047031 |
| Corrected additive Bayesian reference | **1.094595** | **0.644828** | 0.013937 | **1.029542** |

The reference passes the frozen development criteria. Against the matched contextual baseline, paired game-bootstrap 95% intervals for improvement are [0.008616, 0.009378] in log loss and [0.006186, 0.006662] in Brier. Both intervals also remain positive against the full-TRAIN baseline. The posterior has maximum R-hat 1.012606, minimum bulk ESS 435.49, minimum tail ESS 833.19, and zero divergences across 4,000 saved draws.

Calibration is the principal remaining warning from this comparison. On the 59,759 recorded pre-1988 events, classwise ECE is 0.047053, versus 0.003866 for the full contextual baseline. Historical mean predicted Left share is 0.398383 versus an observed 0.408089, an underprediction of 0.97 percentage points. Better event-level proper scores therefore do not establish calibrated historical aggregate estimates. The frozen acceptance criterion uses overall ECE; this historical result is reported separately rather than changing that criterion after scoring.

## Trajectory interaction

The [interaction protocol](trajectory-interaction-protocol.md) replaces additive decade and result-family terms with one decade × result factor, retaining the other four context factors and the specified Normal priors. It reuses the exact earlier 100,000-event TRAIN selection, 922,812 TEST events from 30,816 games, and saved additive predictions. No learned inputs or VALIDATE labels enter the experiment. This reuse isolates the feature specification; the narrow trajectory extract is semantically unchanged by the side correction, but remains an obsolete geometry 0.4 dependency for publication purposes.

| Predictor | TEST log loss | TEST Brier | Classwise ECE | Pre-1988 log loss |
|---|---:|---:|---:|---:|
| Previous additive Bayesian reference | 1.202618 | 0.651852 | 0.011678 | 1.385931 |
| Matched contextual baseline | 1.202406 | 0.652720 | 0.002553 | 1.366557 |
| Full-TRAIN contextual baseline | 1.201124 | 0.652362 | 0.001148 | 1.364277 |
| Bayesian decade × result interaction | **1.191523** | **0.646729** | 0.007105 | **1.343784** |

Against the saved additive reference, the paired overall log-loss improvement is 0.011095, with 95% interval [0.010594, 0.011591]; Brier improvement is 0.005124 [0.004862, 0.005364]. On the 211,340 recorded pre-1988 events, log-loss improvement is 0.042147 [0.040316, 0.043898] and Brier improvement is 0.020087 [0.019094, 0.020918]. All frozen contextual-baseline evaluation flags pass. This supports the missing-interaction hypothesis on recorded events within this sample. It does not identify the causal effect of era or transport to unrecorded events.

The first full run narrowly failed numerical checks: R-hat 1.0728 and bulk ESS 83.8, despite zero divergences. Diagnostics located slow mixing in a correlated intercept/missing-handedness direction. The [predeclared sampling repair](trajectory-interaction-repair-2026-09-11.md) increased only saved draws from 1,000 to 4,000 per chain. Model, priors, data, seed, four chains, warmup, target acceptance, and tree-depth limit stayed fixed. The repaired posterior passes: R-hat 1.020354, minimum bulk ESS 307.24, minimum tail ESS 478.90, and zero divergences across 16,000 saved draws. Predictive results are stable across the two runs.

The failed first run is retained unchanged. It recorded implementation hashes but omitted an exact source-code archive; that provenance limitation is explicit. The accepted repair archives its exact implementation and binds the original additive posterior, configuration, diagnostics, data, and predictions to the original report hashes. Its saved artifact hashes and archived code have been verified. Trajectory's pre-1988 ECE, 0.016975, still exceeds the full contextual baseline's 0.005688, so the calibration tradeoff also warrants examination here.

## Decision and next experiment

Advance the corrected side reference and trajectory interaction to historical validation. Retain the contextual baselines as required comparators and as an aggregate-calibration reference. The next modeling work should target the observed failure modes, not increase pretraining or sample size merely because those options are available.

1. Freeze a historical reconstruction test with whole-game and source/era blocks, including backward transport where recorded historical truth exists. Estimate the joint target/context missingness patterns from unrecorded events, then apply those masks to recorded development events. Mask unavailable clues together; target-only masking would overstate reconstruction accuracy.
2. Measure support, classwise calibration, expected class counts, log loss, and Brier by era and missingness pattern. Side calibration is a specific priority. Any recalibration must fit inside development folds and be compared with the fixed reference on a separately reserved evaluation boundary.
3. Establish that confirmation games were excluded from every fitted or selected component. Renaming an already-used partition does not make it untouched. Continue to label the present TEST results as development evidence.

Keep entirely absent games and unsupported historical strata outside the event-level claim. After those checks, validate aggregate posterior uncertainty against game-level dependence before publishing intervals or replacing canonical artifacts. These experiments justify narrower next tests; they do not authorize publication.

## Evidence and reproduction

- Corrected side artifacts: `artifacts/statistical/backtests/geometry_reference/20260911-global-side-full-v1`. The data extracts, selected rows, full-TRAIN counts, posterior, aligned predictions, protocol, and archived implementation have verified hashes.
- Accepted trajectory artifacts: `artifacts/statistical/backtests/trajectory_interaction/20260911-full-v2-4000draws`; the retained first fit is `20260911-full-v1` beside it.
- Isolated database: `artifacts/statistical/research/geometry-v2-global-side-20260911/bc.db`.
- The compact [combined evidence](geometry-corrected-refits-2026-09-11.json) binds the reports and source evidence and retains the initial failed trajectory run as well as the sampling repair.

To reproduce side fitting from the existing isolated database, use a new output directory:

```sh
PYTHONPATH=bc uv run --no-sync python -m python_models.statistical.backtests.geometry_reference \
  --db artifacts/statistical/research/geometry-v2-global-side-20260911/bc.db \
  --ledger-schema main_models__geometry_v2_research_20260911 \
  --target location_side \
  --output-root artifacts/statistical/backtests/geometry_reference/NEW-RUN-ID
```

To reproduce the accepted trajectory fit, use a new output directory:

```sh
PYTHONPATH=bc uv run --no-sync python -m python_models.statistical.backtests.trajectory_interaction \
  --source-root artifacts/statistical/backtests/geometry_reference/20260911-full-v1/trajectory \
  --output-dir artifacts/statistical/backtests/trajectory_interaction/NEW-RUN-ID \
  --sampling-repair
```

All intervals resample TEST games 500 times, retaining event weighting and holding TRAIN fits fixed. They exclude training-sample uncertainty. Recorded historical labels remain a selected population; neither numerical convergence nor these predictive gains identifies the distribution of unrecorded events or reconstructs absent games.

## Verification

The statistical suite passed 1,047 tests, with 18 slow tests excluded; the isolated materialization suite passed four additional tests. Ruff lint and format checks passed. Strict type checking passed for the reference modules, new trajectory runner and tests, and materialization runner and tests. The reference model's strict configuration has file-scoped exceptions for missing or unknown external PyMC/ArviZ types; the trajectory runner and its tests pass without those exceptions.

An additional package-wide run under basedpyright's default diagnostic policy reported zero errors and 1,836 warnings, so it is not reported as a warning-free package check. The focused strict checks each reported zero errors and warnings. The statistical tests emitted eight Keras/NumPy deprecation warnings; SQLMesh imports emitted third-party deprecation warnings in the materialization tests. There were no test failures.

Final review covered default additive-model preservation, frozen sampler settings, TRAIN-only encoders, prediction alignment, original-report hash binding, code archives, schema-name validation, null provenance rejection, and production isolation. Report/artifact hashes, the selected SQL hashes, population conservation, and documentation links were checked. No production promotion or remote push was performed.
