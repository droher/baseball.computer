# Modeling evidence contract

Implemented September 11, 2026, following the [modeling audit](modeling-audit-2026-09-11.md). This changes validation and publication code and adds a development benchmark. It does not refit existing artifacts, restamp canonical manifests, move published pointers, rebuild production tables, or publish a release.

## What earns a pass

Gate version 3 reports six evidence dimensions separately: numerical, predictive, calibration, transport, identification, and provenance. Each is `passed`, `failed`, or `unsupported`. Passing numerical diagnostics does not establish identification. Passing prediction and calibration on recorded observations does not establish historical transport.

Full Bayesian fits must supply a recognized metric family, positive evaluated sample counts, and finite, range-valid required metrics. Empty metrics, NaN likelihood gains, and accuracy-only classification summaries cannot pass. Classification requires a comparator and held-out ECE; likelihood families currently lack a supported calibration check. Existing family comparators retain their original meaning: some are plug-in ablations or an oracle held-out marginal, not independently fitted contextual baselines.

Deep validation requires aligned model and baseline probabilities on the same `event_key + partition` rows, declared class labels, and explicit `target_class` truth. It computes log loss, multiclass Brier score, and classwise ECE. The policy requires improvement on both scores and classwise ECE no greater than 0.10. These are operational gates, not a claim of statistical significance or historical identification. Missing baseline exports are blocking evidence gaps.

The default `publish-manifest` and `publish-pretrain` paths require current numerical, predictive, calibration, and provenance passes. Publication mode `validated` means those predictive requirements passed; transport and identification remain separately unsupported. Bayesian publication recomputes diagnostics from the saved posterior. Unsupported pretraining artifacts cannot enter a validated dependency chain.

An explicit `--exploratory-reason 'reason'` permits a research pointer with unsupported or failed evidence. It preserves a failed validation result and records exploratory mode in both pointer and manifest. Smoke artifacts remain unpublishable. A later gate sweep cannot turn an exploratory publication into passed table confidence. A validated release requires an explicit publication operation.

## Artifact integrity

Dataset exports record SHA-256 digests of Parquet bytes, canonical schema, the catalog relation definition, and catalog dependency identities. Recorded truth, deterministic derivation, model eligibility, and inference rows have separate counts. On the current geometry relation:

| Population | Rows |
|---|---:|
| All dimension rows | 84,272,874 |
| Recorded observed classes | 37,105,511 |
| Deterministically derived classes | 22,860,135 |
| Eligible observed or derived classes | 59,965,646 |
| Eligible unrecorded classes | 24,307,228 |

Reusing an artifact ID verifies the frozen file and compares a temporary fresh export, including when a source carries a named snapshot label. A changed payload requires a new ID. Exact byte checks can conservatively reject a semantically identical export with different ordering or serialization; they never silently replace the old artifact. Legacy datasets lacking hashes also require a new ID.

Validation binds model settings, artifact files, declared outputs, and recursively resolved input manifests. Dataset hashes must be valid SHA-256 values, content and schema are recomputed, and manifest hashes must agree with dataset metadata. Missing lineage blocks validated publication. Newly fitted Bayesian manifests record the exact dataset manifest. Active learned covariates or marginalization require additional fitted dependencies; legacy deep/pretrain artifacts lack sufficient lineage and remain unsupported.

The catalog transformation digest is intentionally limited: for a view it fingerprints its stored definition; for a materialized table it fingerprints table DDL. A dependency-name digest is not a recursive hash of upstream SQL source or proof that a snapshot label is immutable. Content integrity does not by itself establish leakage-free training. Complete supervised-stage lineage and common training-game boundaries still need to be implemented for future composed fits.

Validation reports and mutable validation stamps are excluded from model-content digests so saving a verdict does not invalidate itself. Publication verification separately checks report identity, gate version, pointer/manifest mode agreement, and current dependency verdicts. Deleting, downgrading, or making a dependency report stale invalidates a validated parent. These hashes detect accidental changes; they are not signed attestations against an actor who can replace both files and verdicts.

## Commands and migration

- `just validate-gates [model...]` is read-only and resolves the pointer's exact manifest, including deep aliases. It recomputes available Bayesian diagnostics and reports unsupported evidence. It does not hash every export by default.
- `just validate-gates [model...] --write` additionally checks content and dependency provenance before saving evidence and stamps. This intentionally can produce a stricter verdict than the quick read-only sweep. It does not change pointer publication mode or refresh a pointer's saved binding.
- `--json-output /absolute/path/report.json` on the sweep exposes the six evidence dimensions. A sweep exits nonzero when any row has failed, exploratory, missing, or error status. A quick row-level pass can still have unsupported evidence dimensions; default publication requires the complete predictive evidence vector.
- `scripts/check_published_pointers.py` verifies publication evidence before a production build, including nested pretrain pointers. Old pointers without publication policy and binding fail this gate. Existing production artifacts were not migrated automatically.
- Repair or regenerate artifacts under new IDs, validate their dependencies, then review and explicitly publish chosen pointers before the next production build. Updating these code paths alone does not bless existing tables or revise their live confidence labels.

The run-expectancy interval diagnostic now reports `coverage_kind=studentized_mean`. It uses held-out sample variance and an approximate Normal mean interval. It does not simulate the fitted negative-binomial observation distribution or account for within-inning dependence, and does not satisfy the calibration evidence requirement. The state-transition interval calculation remains an approximate marginal simulation. Joint uncertainty for run values and linear weights remains outstanding.

## Development benchmark and decision

The [reconstruction benchmark](modeling-reconstruction-benchmark-2026-09-11.json) compares train-only marginal probabilities with Laplace-smoothed decade × result-family probabilities. Both score the same observed TEST rows; derived truth is excluded and no pretraining is used. TRAIN and TEST contain zero overlapping games. These TEST data have been inspected during development and are not a fresh confirmatory test set.

| Target | TEST rows | Marginal log loss | Contextual log loss | Improvement, 95% game-bootstrap interval |
|---|---:|---:|---:|---:|
| Location side | 753,742 | 1.052199 | 0.986677 | 0.065522 [0.064525, 0.066519] |
| Trajectory | 922,812 | 1.354655 | 1.201124 | 0.153532 [0.152105, 0.154846] |

Both also improve Brier score. The bootstrap holds the fitted TRAIN baseline fixed and resamples TEST games; it does not measure training-fit uncertainty. Full aggregate ECE is low, but the trajectory contextual baseline has slightly higher ECE than the marginal baseline despite better predictive scores. Calibration and discrimination should remain separate judgments.

Season and result family are available in every inspected eligible inference cell for these targets. This establishes computability, not transport validity: the pre-1988 observed subset is selected, and games absent from acquired play-by-play are outside the frame. There is no head-to-head comparison against an existing Bayesian fit because those fits use a different holdout and training budget.

Regenerate after a smoke run, choosing separate output paths:

```bash
uv run --no-sync python scripts/modeling_reconstruction_benchmark.py \
  --output /private/tmp/reconstruction-smoke.json \
  --checkpoint-log /private/tmp/reconstruction-smoke.log \
  --bootstrap-repetitions 20
uv run --no-sync python scripts/modeling_reconstruction_benchmark.py \
  --full --output /private/tmp/reconstruction-full.json \
  --checkpoint-log /private/tmp/reconstruction-full.log
```

The next fitting decision is a geometry model without pretrained inputs on this same benchmark, with a frozen training budget and contextual comparator. Use a separate untouched outer holdout for final confirmation; fit all supervised stages strictly inside it. Before historical claims, add scorer/source-block and era masking with a declared target population. Before aggregate publication, generate likelihood-based posterior predictive draws and propagate within-inning and shared-data dependence. More deep pretraining and additional model families should wait for those comparisons.

## Verification

The full statistical suite plus pointer checks passed: 997 tests, with 18 tests marked slow excluded. The final fitted-dependency and publication-save tightening passed another 26 focused tests, with four slow tests excluded. Ruff and formatting checks passed on all 32 changed Python files. The statistical package and benchmark typecheck had no errors; the new evidence-binding and publication modules additionally passed strict typechecking. Initial broad-test failures caused by PyTensor attempting to write outside the sandbox were resolved by placing its compilation cache under `/private/tmp`; all affected tests then passed.

The real-data benchmark ran after a smoke loop, with 500 game-bootstrap repetitions. Its six synthetic invariant tests verify train-only fitting, normalized probabilities, complete score alignment, disjoint games, duplicate-label handling, and explicit output paths. Final review also exercised revoked dependency reports, changed payloads and settings, schema/hash mismatch, incomplete evidence, exploratory status preservation, incorrect model identity, and relative deep pointers.
