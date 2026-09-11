# Corrected geometry target and trajectory development decision

September 11, 2026. The location target correction is implemented and verified against real source data. A separate TRAIN-only trajectory experiment supports testing decade × result interactions. Production tables and published artifacts remain unchanged; this work does not claim a new validated posterior.

Subsequent work: [corrected geometry refits](geometry-corrected-refits-2026-09-11.md) completed isolated SQLMesh materialization and passing development comparisons for corrected side and the trajectory interaction. The experiment and verification descriptions below record the earlier correction stage.

## Location contract

The ledger now distinguishes three quantities:

| Field | Corrected `location_side` meaning |
|---|---|
| `raw_value` | Directly recorded general-location token |
| `mapped_value` | Global side obtained by mapping that token through the location seed |
| `deduced_value` | Fielder-based fallback when recorded general location is absent or Unknown |
| `value_origin` | Explicit source, source-mapping, deduction, sentinel, or missing provenance |
| Modeling `class` | Mapped side only for observed rows; null for side deductions and unknowns |

Source-mapped side has classes `All`, `Left`, `Middle`, and `Right`. `All` is retained explicitly: all 43,839 such records map from `Catcher`, and this category does not specify a precise left/center/right direction. Taxonomy mapping is deterministic coarsening of a recorded code, not an independently measured coordinate.

Within-zone angle is now a separate `location_angle` ledger dimension. `Default` remains visible as `sentinel_type=default`, `observed_status=default_code`, and is not training eligible. No new angle model family was registered. The shared sentinel audit applies this rule specifically to angle, preserving other dimensions' meaningful default conventions.

Both modeling datasets carry `geometry_target_contract=geometry-v2-global-side`. Geometry dataset version is 0.5.0; observation dataset version is 0.4.0. Both withhold side DL and propensity values from the old artifact joins. Side Bayesian preparation uses the mapped class and disables DL, propensity, and handler covariates; explicit learned-flavor requests are rejected. Standalone research DL training can target the corrected classes, but its output is not automatically consumed by the Bayesian model.

Export and reference extraction reject stale relation markers, observed angle classes in the side target, and non-null legacy side learned inputs. Side Bayesian, observation, and deep preparation check the frozen contract. Publication dependency binding rejects obsolete dataset versions and verifies the current-version Parquet contract. Renaming an old dataset's version alone does not make it acceptable. These checks protect artifact compatibility; they do not establish holdout integrity or historical identification for future composed fits.

## Real-data verification

The [frozen evidence JSON](geometry-target-correction-2026-09-11.json) binds SQL hashes, source database size/mtime, and full-source aggregate results:

| Corrected side population | Events |
|---|---:|
| Observed source-mapped side | 5,049,435 |
| Fielder-derived side | 6,113,640 |
| Unknown side | 878,323 |
| Direct recorded tokens without a seed mapping | 0 |

Observed side classes are Left 1,521,098; Middle 2,038,861; Right 1,445,637; All 43,839. The previous fielder-prioritized calculation disagrees with direct recorded-location mapping on 600,593 events. This difference validates the need for provenance separation; it does not prove that every recorded location is correct.

Angle has 1,525,784 non-default observed modifiers, 3,523,651 ambiguous defaults, and 6,991,963 Unknown values. Trajectory raw values, deductions, sentinels, and statuses match on all 12,038,982 common rows. The corrected query sees 2,416 additional upstream rows relative to the old materialized ledger, so a rebuilt dataset will also include that freshness change.

Validation used read-only source queries, exact SQL expressions in executable regression tests, and SQLMesh rendering with temporary database/state paths. It did not materialize or export a new full modeling dataset. The current `bc.db` modeling views still have the old contract and are intentionally rejected by the new exporter until the corrected SQL is materialized in a development environment. Existing frozen angle experiments remain intact as historical diagnostics.

## Trajectory decision

The [internal development comparison](trajectory-development-2026-09-11.md) split the frozen 100,000-event primary TRAIN selection by game into 79,999 fitting events and 20,001 development events. It used no primary TEST or VALIDATE labels. Fixed L2 logistic fits compared the six additive context terms with decade × result cells plus the other four context terms.

| Predictor | Development log loss | Pre-1988 development log loss |
|---|---:|---:|
| Decade × result frequency baseline | 1.204729 | 1.383802 |
| Additive logistic reference | 1.206605 | 1.415614 |
| Logistic reference with decade × result interaction | **1.194619** | **1.369338** |

The interaction also improves Brier score over both comparators. Its overall ECE, 0.006630, improves on the additive model's 0.012725 but remains above the frequency baseline's 0.004992. These are development point estimates without confidence intervals or historical masking evidence. The L2 parameterization differs from the earlier Bayesian priors. The result supports a structural candidate; it does not prove the cause of the original TEST regression.

The original 500-iteration optimizer limit stopped the additive fit before development scoring. Raising only that limit to 2,000 achieved convergence; both pre-score configurations are preserved. No predictive settings changed in response to held-out scores.

## Next experiment

Materialize the corrected ledger and modeling views in an isolated development environment, freeze new dataset IDs, and rerun the global-side reference under the [corrected protocol](geometry-reference-global-side-protocol.md). Old angle scores are not a comparator for the new target population. For trajectory, predeclare a Bayesian decade × result interaction candidate and its priors, then assess it on development folds and source/era masking before an untouched confirmation boundary. Retain the contextual baseline throughout. Additional pretraining and production publication should follow evidence from those comparisons.

## Verification

The full statistical test suite passed: 1,037 tests, with 18 slow tests excluded. Ruff lint and format checks passed on all 19 changed Python files, and statistical-package typechecking reported zero errors or warnings. Additional strict checking passed for the new guard and development modules, with file-scoped exceptions for scikit-learn's missing/unknown external types; the new SQL tests also passed strict checking. All three affected SQL models rendered. Final review checked stale version restamping, mapped-class use, null propensity handling, handler exclusion, explicit flavor rejection, source sentinel behavior, and preservation of the reference's TRAIN/TEST boundary.

The existing production view was explicitly tested and rejected before export. Database size and modification time remained unchanged. Frozen evidence SQL hashes, population conservation, development input hash, and report links were verified. Eight third-party Keras/NumPy deprecation warnings were emitted by existing deep tests; there were no test failures.
