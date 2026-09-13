# Modeling documentation index

[Documentation guide](../README.md)

This index is a map of the modeling documentation in `docs/`. Start with the reader task that matches your work, then follow the contract or protocol before reading its dated evidence. Dated documents are historical development records; their results do not, by themselves, establish production readiness, historical transport, identification, or publication approval.

For the broader project framing and dependency order, see the [data coverage implementation plan](../../notes/data-coverage-implementation/README.md). For the currently described estimated output tables, see [Estimated Models — Reference](../estimated-models.md).

The current completion objective is [full-history PBP imputation](../../notes/full-history-imputation-plan.md): all applicable baseball fields for the available play-by-play game population, accepting explicitly rough estimates. It excludes aggregate-only games and synthetic histories for games without PBP; box-score and season-based imputation is a separate project. The plan separates complete coverage from evidence about prediction quality.

## Continue an existing modeling thread

The [geometry modeling handoff](../geometry-modeling-handoff.md) is the continuation entrypoint for the airborne recording-regime work. It collects the current thread, boundaries, artifacts, and next decisions. It is a handoff document, not an independent assertion that the scientific work is ready.

## Read a durable contract or protocol

These documents define targets, populations, inputs, splits, gates, provenance, or publication behavior. Read the applicable one before interpreting a result.

### Cross-model contracts and review

- [Modeling evidence contract](../modeling-evidence-contract.md) — evidence dimensions, validation gates, artifact binding, and publication policy.
- [Geometry reliability work](../geometry-reliability-plan.md) — active reliability goal and remaining evidence gaps for historical geometry use.
- [Airborne standardization target contract](../geometry-air-standard-target-contract.md) — preserves ground versus air and standardizes airborne subtypes.
- [Retrosheet scorer provenance contract](../scorer-provenance-contract.md) — separates official and administrative scorer source fields and their statuses.

### Geometry and airborne translation

- [Hierarchical geometry development protocol](../geometry-hierarchical-development-protocol.md) — frozen TRAIN-only geometry experiment.
- [Geometry Statcast development protocol](../geometry-statcast-development-protocol.md) — development bridge between Retrosheet trajectory classes and launch-angle bands.
- [Statcast matching amendment](../geometry-statcast-matching-amendment.md) — amended plate-appearance matching rule for the acquisition protocol.
- [Geometry Statcast bridge acquisition protocol](../geometry-statcast-bridge-protocol.md) — bounded 2016–2018 bridge acquisition.
- [Pooled airborne translation development](../geometry-air-development-protocol.md) — first pooled translation experiment and its fixed comparison design.
- [Airborne recording-regime posterior development](../geometry-air-regime-posterior-protocol.md) — hierarchical regime translation experiment.
- [Airborne translation by recording pipeline](../geometry-air-pipeline-translation-protocol.md) — pipeline-specific translation experiment.
- [Airborne source and measurement bridge](../geometry-measurement-bridge-design.md) — design for source and measurement questions after the pooled experiment.
- [Corrected global-side reference protocol](../geometry-reference-global-side-protocol.md) — corrected location-side development comparison.
- [Shared-split geometry reference protocol](../geometry-reference-protocol.md) — original shared-split reference comparison.

### Historical stress and trajectory

- [Historical geometry stress protocol](../historical-geometry-stress-protocol.md) — frozen context-mask and training-exclusion stress tests.
- [Trajectory era-by-result interaction protocol](../trajectory-interaction-protocol.md) — narrow frozen interaction comparison on inspected TEST data.
- [Trajectory interaction sampling repair](../trajectory-interaction-repair-2026-09-11.md) — frozen sampler repair specification following diagnostics.

## Inspect dated evidence and decisions

These are dated results, audits, diagnostics, and research reviews. Use them to understand what was learned, what failed, and what remains bounded by the associated protocol.

### Geometry and airborne translation evidence

- [Geometry development exposure ledger](../geometry-development-exposure.md) — exposure and reuse accounting for the development work.
- [Statcast mechanics results](../geometry-statcast-mechanics-results-2026-09-11.md) — amended acquisition mechanics check.
- [Statcast fitting audit](../geometry-statcast-fitting-audit-2026-09-11.md) — fitting acquisition integrity and reservation checks.
- [Statcast and scorer findings](../geometry-scorer-statcast-findings-2026-09-11.md) — historical scoring and Statcast reference diagnostics; superseded target proposal is retained as history.
- [Airborne translation development results](../geometry-air-development-results-2026-09-11.md) — pooled screen and its failed calibration decision.
- [Airborne regime computation](../geometry-air-regime-computation-2026-09-11.md) — computational validation without new real-data predictive scores.
- [Airborne regime development results](../geometry-air-regime-development-results-2026-09-11.md) — regime posterior screen and robustness outcome.
- [Airborne regime grounding research](../geometry-air-regime-grounding-research-2026-09-11.md) — research review of grounding options and source quality.
- [Airborne translation estimate](../geometry-air-translation-estimate-2026-09-11.md) — season-by-season estimate under stated assumptions.
- [Airborne reference sensitivity](../geometry-air-reference-sensitivity-2026-09-11.md) — sensitivity to unresolved reference mass and relabeling.
- [Airborne bridge check](../geometry-air-bridge-check-2026-09-11.md) — bridging-hitter transport diagnostic and pipeline boundary.
- [Pipeline translation results](../geometry-air-pipeline-translation-results-2026-09-11.md) — dated results for the pipeline-specific translation protocol.
- [Modern observation identifiability](../geometry-modern-observation-identifiability-2026-09-11.md) — TRAIN-only identifiability audit.
- [Geometry target correction](../geometry-target-correction-2026-09-11.md) — corrected location target and trajectory development decision.
- [Geometry reference results](../geometry-reference-results-2026-09-11.md) — completed shared-split comparison and revised decision.
- [Corrected geometry refits](../geometry-corrected-refits-2026-09-11.md) — research comparisons after target correction.
- [Historical location-side provenance](../geometry-side-clue-provenance-2026-09-11.md) — provenance distribution for observed, derived, and unknown side clues.
- [Global-side hierarchical results](../geometry-side-hierarchical-results-2026-09-11.md) — corrected global-side recorded-label control outcome.
- [Geometry confirmation boundary](../geometry-confirmation-boundary-2026-09-11.md) — limits on calling current data globally unused.

### Historical stress and trajectory evidence

- [Historical geometry stress test](../historical-geometry-stress-2026-09-11.md) — completed stress tests under the frozen protocol.
- [Trajectory internal development diagnosis](../trajectory-development-2026-09-11.md) — TRAIN-only interaction diagnosis.

### Scorer/source evidence

- [Scorer source repair and parser reproducibility](../scorer-source-repair-validation-2026-09-11.md) — controlled parser repair, parity, and materialization boundary.

### Modeling-wide audit

- [Modeling audit](../modeling-audit-2026-09-11.md) — dated audit, evidence gaps, family dispositions, and proposed work order.

## Look up published estimated surfaces

- [Estimated Models — Reference](../estimated-models.md) — active reference for estimated `main_models.*` tables, provenance columns, confidence status, airborne translation surfaces, and known issues. It documents published table shapes and status semantics; it does not replace the evidence contracts or dated experiment records above.
