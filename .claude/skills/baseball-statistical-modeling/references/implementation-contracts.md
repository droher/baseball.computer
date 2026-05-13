# Implementation Contracts

Use this reference when turning a model design into project implementation tasks.

## Runtime Boundary

SQLMesh should consume stable model artifacts. It should not run full Bayesian inference inside ordinary SQLMesh plans.

Preferred flow:

1. SQLMesh builds deterministic training frames, masks, ledgers, and audit anchors.
2. Offline Python model code fits probabilistic models from frozen frame versions.
3. Posterior artifacts are stored outside canonical raw or staged namespaces.
4. SQLMesh ingests compact summaries, probability tables, expected counters, or draw tables.
5. Published metric models recompute additive summaries from official, deterministic, and probabilistic namespaces explicitly.

## Artifact Types

| Artifact | Use |
| --- | --- |
| Training frame Parquet | Reproducible input snapshot with masks, ledgers, indexes, and source hashes. |
| Posterior draws | Nonlinear downstream summaries, uncertainty propagation, and model criticism. |
| Probability table | Event-level categorical estimates such as handler, geometry, or credit allocation. |
| Expected counter table | Additive metric inputs when posterior draws are too expensive for normal SQL use. |
| Summary table | Display and lightweight analysis only. |
| Metadata JSON or YAML | Model version, source frame version, priors, seeds, diagnostics, and validation results. |

Invariant: never overwrite raw, staged, or deterministic tables with posterior samples or point fills.

## Publication Policy

Publish as official fact only when the value comes from an authoritative source at the target grain.

Publish as deterministic derived value when:

- Inputs are official or canonical at the necessary grain.
- The transformation is rule-based and auditable.
- Failure modes are flagged.

Publish as probabilistic estimate when:

- The model has a named estimand.
- Provenance masks and artifact risks are represented.
- Calibration and conservation checks pass.
- The output stores method, model version, and uncertainty.

Withhold or mark weakly identified when:

- Source, scorer, park, team, era, or league effects cannot be separated.
- Conservation audits fail.
- Posterior sensitivity is dominated by priors.
- Validation only works under random cell masking but fails source-block or era holdouts.

## Model Builder Shape

For PyMC or another probabilistic backend, prefer:

- A pure data-prep layer that emits arrays, integer indexes, masks, and coords.
- A model builder that exposes named dimensions and interpretable deterministic quantities.
- Prior predictive sampling before fitting.
- Short smoke fits before full runs.
- Posterior predictive and held-out prediction paths.
- ArviZ-compatible inference data storage.

## Validation Checklist

- Simulated data recovery for latent estimands.
- Prior predictive checks on baseball-scale counts and rates.
- Posterior predictive checks against held-out observed slices.
- Grouped holdouts by season, era, league, source family, scorer, park, and team.
- Calibration curves and reliability diagrams for probability outputs.
- Conservation audits for outs, score, line score, box totals, innings, and official aggregates.
- Slice reports for missingness regime, player role, handedness, park, scorer, and source family.
- Sensitivity analysis for MNAR mechanisms and source-artifact downweighting.

## Deep Learning Boundary

Deep learning can supplement imputation when it produces:

- Calibrated probability vectors.
- Embeddings for players, parks, scorers, teams, or eras.
- Residual diagnostics that reveal missing interactions.
- Proposal distributions for constrained Bayesian or deterministic reconciliation.

Deep learning should not lead:

- Official fielding credit allocation.
- Box residual reconciliation.
- Entity linkage and personnel eligibility.
- Source-artifact decisions.
- Exposure and denominator policy.
- Official scoring convention regimes.
