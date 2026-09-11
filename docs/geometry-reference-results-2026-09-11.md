# Geometry reference: completed comparison and revised decision

September 11, 2026. Both four-chain fits completed, both passed sampling diagnostics, and neither passed the [frozen predictive protocol](geometry-reference-protocol.md). Retain the full-TRAIN contextual baseline as the development reference. Correct the location target's semantics and investigate trajectory's historical regression before expanding training or adding pretrained inputs. No production database, canonical artifact, or publication pointer was changed.

## A target-definition problem precedes further location modeling

The dataset target named `location_side` is a recorded **angle modifier within a general location**, not overall left/center/right field direction. In `event_observation_geometry.sql`, its `raw_value` is `recorded_location_angle`. `calc_batted_ball_type.sql` takes that value directly from `stg_events.batted_location_angle`; its separate `location_side` calculation instead uses fielder and general-location categories. The ledger puts that separate side calculation into `deduced_value` when the angle is unavailable, mixing two different quantities within the dimension.

The existing [angle documentation](../bc/models/doc_global_cols.md) explicitly says `Default` can mean central within the general location or unrecorded detail. Nevertheless, the observation ledger marks every non-null, non-`Unknown` angle, including `Default`, as observed. `Default` accounts for **526,835 of 753,742 TEST labels (69.9%)**. This comparison faithfully evaluates the frozen recorded-field taxonomy; it does not demonstrate recovery of physical field direction or distinguish unrecorded detail from genuinely central locations. The earlier baseline scores have the same limitation.

The source-error flag provides no reassurance here: the ledger documents that no current source-error field maps to a geometry dimension. All selected source rows carry `data_error_risk=none`, which must not be read as verified error-free truth.

Corrective scope: separate global side derived from a recorded general-location code, local angle modifiers, and fielder-based deductions. Define the estimand and observation rules for each, preserve ambiguous defaults explicitly, and create a versioned extract and benchmark. Do not relabel this completed experiment or substitute new labels into its frozen files.

## Comparable fits and results

Each Bayesian reference used 100,000 deterministically selected TRAIN events, with TRAIN-only vocabularies and additive decade, result-family, base-state, outs, alignment-regime, and batter-hand effects. No learned covariates were used. The matched contextual baseline used exactly those same TRAIN events; the full contextual baseline used all eligible TRAIN events. All predictors scored identical TEST events, with zero TRAIN/TEST game overlap.

The reference adds a decade term and removes redundant additive parameter directions relative to the existing geometry family. It is not an exact refit of a published artifact. Its conditional event-independence assumption remains, including within games.

Lower log loss and Brier are better:

| Target and predictor | TEST log loss | TEST Brier |
|---|---:|---:|
| Recorded angle (`location_side`), marginal, 100k TRAIN | 1.052211 | 0.487751 |
| Recorded angle, contextual, 100k TRAIN | 0.988516 | 0.471700 |
| Recorded angle, Bayesian, 100k TRAIN | 0.988486 | 0.471649 |
| Recorded angle, contextual, full TRAIN | **0.986677** | **0.471317** |
| Trajectory, marginal, 100k TRAIN | 1.354713 | 0.708611 |
| Trajectory, contextual, 100k TRAIN | 1.202406 | 0.652720 |
| Trajectory, Bayesian, 100k TRAIN | 1.202618 | **0.651852** |
| Trajectory, contextual, full TRAIN | **1.201124** | 0.652362 |

The recorded-angle model's log-loss gain over its matched contextual baseline was 0.000030 nats/event, with a 95% game-bootstrap interval of **[-0.000136, 0.000198]**. Trajectory's gain was -0.000213, interval **[-0.000748, 0.000360]**. Neither established the required log-loss improvement. Both improved matched-budget Brier, but the protocol required improvement on both scores. Both had worse log loss than the full-data contextual baseline; trajectory retained a Brier advantage. This supports retaining the simpler reference and investigating the score disagreement, not claiming universal dominance of either predictor.

Sampling quality was adequate: maximum R-hat was **1.0034 / 1.0058**, minimum bulk ESS **1,217 / 1,173**, minimum tail ESS **1,698 / 1,622**, and divergences **0 / 0**, for recorded angle / trajectory respectively. Four chains each contributed 1,000 saved draws after 1,000 warmup draws. Poor convergence does not explain the predictive verdict.

## Historical and calibration limits

The pre-1988 observed trajectory slice contained 211,340 events in 17,540 games. Bayesian log loss was **1.385931**, compared with **1.366557** for the matched contextual baseline and **1.364277** for the full-data baseline. The matched-baseline-minus-model interval was **[-0.021446, -0.017691]**: a clear regression under this evaluation. The corresponding Brier scores were 0.718986, 0.709260, and 0.708419.

The recorded-angle model improved over the matched baseline on its pre-1988 observed slice, but remained worse than the full-data baseline. Its ambiguous target semantics further limit that result.

Overall classwise 15-bin ECE was 0.001049 for recorded angle and 0.011678 for trajectory, below the frozen 0.05 ceiling. Trajectory's full-data contextual ECE was 0.001148. Aggregate ECE also hides local failures: for example, 961 events assigned Bunt probability averaging 0.699 had an observed Bunt rate of 0.957. That bin is descriptive and selected after evaluation, not an independent hypothesis test. Passing the coarse ECE ceiling does not establish calibration in every regime or class.

These TEST data had already been inspected during development. Intervals resample TEST games 500 times with TRAIN fits fixed; they exclude training-sample uncertainty. Pre-1988 labels are a selected observed subset. Neither historical transport to missing records nor MNAR identification has been established. Prior checks verified valid probabilities and broad predictive behavior; they are not simulation-based recovery or substantive baseball validation.

## Next work, in order

1. Repair and version the geometry target contract described above. Require separate recorded, mapped, deduced, ambiguous-default, and missing counts before fitting. The current `location_side` benchmark remains a diagnostic of the existing field.
2. Diagnose trajectory on TRAIN/internal development folds by era, result family, and label class. The contextual baseline includes decade × result interactions while the Bayesian reference is additive; an interaction deficit is a plausible explanation to test, not an established cause. Use a small interaction/shrinkage reference before another large fit.
3. Freeze source-block and era masking scenarios against a declared reconstruction population. Retain contextual baselines in every comparison, and establish a genuinely untouched confirmation boundary after auditing earlier supervised-stage use. `VALIDATE` was unused in this run; that alone does not establish that it was unused historically.
4. Advance a candidate only after common-population gains, slice calibration, and masking evidence. Aggregate uncertainty and pretrained-model comparisons remain subsequent work. These two fits do not rule out Bayesian models or richer features generally.

## Reproducibility and verification

The [compact evidence JSON](geometry-reference-results-2026-09-11.json) records exact scores, paired intervals, combined protocol failures, lineage, versions, implementation hashes, and artifact digests. Full extracts, posterior draws, aligned TEST probabilities, reliability bins, and logs are local ignored research artifacts under `artifacts/statistical/backtests/geometry_reference/20260911-full-v1/`. The preceding two-target smoke run is under `20260911-smoke-v1/`.

The archived implementation matches the hashes frozen before full fitting. Subsequent source changes add combined `protocol_pass` reporting, automatic code archiving, and typing casts only. Original executed reports and hashes are preserved; the compact report derives the combined verdict from numerical and predictive checks. The extracts are content-frozen, but the source's `dev` snapshot label is not an immutable upstream transformation lineage.

Verification recomputed all recorded artifact hashes, checked archived code/protocol hashes, confirmed exact event/truth alignment and game separation, recomputed all overall log losses, and reproduced the earlier full-contextual log-loss and Brier scores within 1e-12. Maximum probability-sum deviation was below 5e-15. Twenty-five focused tests passed. Ruff lint/format checks passed on all six new Python files. Package typechecking passed; additional strict checking passed with file-scoped unknown-type exceptions at the untyped PyMC boundary.

Regenerate from the repository root, using distinct new output directories for smoke and full runs:

```bash
PYTHONPATH=bc uv run --no-sync python -m python_models.statistical.backtests.geometry_reference \
  --smoke --output-root artifacts/statistical/backtests/geometry_reference/new-smoke
PYTHONPATH=bc uv run --no-sync python -m python_models.statistical.backtests.geometry_reference \
  --output-root artifacts/statistical/backtests/geometry_reference/new-full
```

The runner opens `bc.db` read-only and refuses to overwrite an existing output directory. This standalone experiment has no publishable model manifest.
