---
title: Phase 3 Exit — Deep Learning Supplements
type: phase-exit-summary
status: ready-for-fits
audience: humans-and-agents
last-verified: 2026-05-14
---

# Phase 3 Exit — Deep Learning Supplements

PR1–PR7 collectively land the Phase-3 scaffolding required by
`notes/data-coverage-implementation/04-deep-learning-supplements.md`.
This file is the tracked closure record; the formal exit-gate run will
update it with concrete artifact IDs and metrics once the production
fits on bc_dev.db (and later bc.db) complete.

## Squash-merge history (data_coverage branch)

| PR | Title | Scope summary |
| --- | --- | --- |
| PR1 | FeatureLayout + Phase-3 deep package scaffolding + justfile cleanup | Threads `FeatureLayout` through the legacy ml/ training stack; scaffolds `bc/python_models/statistical/deep/` (target_spec, io, training fold runner, artifacts, registry, leakage_probes stub). Fixes 4 buggy named-positional just recipes (Pattern C). |
| PR2 | validate library + held-out source_family probe | `validate_artifact()` dispatches on `manifest.kind`; deep checks for dl_p_class normalization, no-argmax schema guard, grain uniqueness per partition. `source_probe_held_out()` implemented as sklearn LR stratified probe with AUC-tier classification. |
| PR3 | Manifest schema migration + Geometry deep target | `dl_p_class VARCHAR + dl_logit_class DOUBLE` becomes `dl_p_class DOUBLE[]` across all 7 model_input_* views. `dl_proposal_manifest` becomes a Python @model. Two new sibling manifests (`dl_credit_proposal_manifest`, `dl_advancement_proposal_manifest`). Geometry target registered: 5 specs (trajectory, location_side, location_depth, location_edge, region). Calibrators (`fit_multiclass_temperature`, `fit_isotonic_per_class`, `fit_platt_per_slice`). Dataset versions bumped 0.1.0 → 0.2.0. |
| PR4 | Pitch-summary deep proposal target | 1 binary DeepTargetSpec on `has_count` registered to `dl_proposal_manifest` with `proposal_dimension="pitch_summary"`. Advancement target deferred pending a target-column re-scoping. |
| PR5 | Fielding-credit deep proposals (per-credit_type specs) | 3 binary DeepTargetSpec — putout, assist, error — each filtered to its credit_type AND `eligible_for_allocation`. All three publish to `dl_credit_proposal_manifest`. |
| PR6 | Deep entity embeddings export + ingestion | `deep/embeddings.py` extraction + parquet write helpers. `dl_embedding_artifact` SQLMesh @model aggregates embeddings.parquet across all published targets. PR2's source_probe_held_out remains the AUC grader. |
| PR7 | Phase 3 exit gate (this PR) | `scripts/check_phase3_exit.py` aggregator over published deep artifacts. Implementation-checklist Phase 3 boxes flipped. |

## Acceptance criteria status

| # | Criterion | Status | Where it lives |
| --- | --- | --- | --- |
| 1 | OOF predictions present for every training row | scaffolding ready | `deep/training.run_target()` produces 1 OOF row per TRAIN event_key; `test_fold_runner.py` asserts. |
| 2 | Calibration passes by slice (era / scorer / source / hit-vs-out / missingness) | calibrator wrappers shipped | `deep/calibrators.py`; per-slice fit-Platt + parent-fallback live. Slice-wise eval runs at fit time once production targets fit. |
| 3 | Probability vectors normalize within key/class groups | validator shipped | `validate.py:_check_probability_normalization` (tol 1e-3). |
| 4 | Personnel + structural masks applied before fielding proposals | spec hooks ready | `deep/targets/fielding_credit.py` filter_predicate includes `eligible_for_allocation`; full fold-runner mask work is the documented follow-up. |
| 5 | Embedding probes documented and per-source-family AUC recorded | probe shipped | `deep/leakage_probes.source_probe_held_out` + ProbeResult.publication_tier. Fold runner integration emits the AUCs once a target publishes embeddings. |
| 6 | Baseline (deterministic geometry rule) log-loss/Brier comparison present | hook present | `validate.py:_compare_against_baseline` reads optional `exports/baseline_predictions.parquet` and reports row count + warns on schema gaps. Baseline parquet emission is a per-target decision. |
| 7 | No SQL artifact exposes only argmax — probabilities, not classes | guarded | `dl_p_class DOUBLE[]` across all 7 model_input_* views; `validate.py:_check_deep_schema` blocks if a deep artifact exposes any of {predicted_class, argmax_class, argmax, dl_argmax_class}. |

## How to evaluate

```sh
uv run --group ml python scripts/check_phase3_exit.py
```

The script reports per-target validation status and prints the
acceptance-criteria checklist with `[x]` markers when no target is in
the failed state, or `[?]` when any target is blocked. Until at least
one target's fit-deep + publish-manifest round-trip completes, the
script prints a warning and exits 0 (nothing to evaluate yet).

## Follow-ups before Phase 4 starts in earnest

1. Run the real Geometry fits on bc_dev.db (5 dimensions × fold_count+1 fits).
2. Regenerate the Phase-2 EDA artifact for `model_input_observation_batted_ball`
   against the post-PR3 schema and bump
   `phase2-exit-eda-batted-ball.md` with the new artifact_id.
3. Update advancement target column resolution and unstub
   `deep/targets/advancement.py`.
4. Add the full personnel-eligibility mask to the fielding-credit fold
   runner so ineligible (event, player, position) triples force
   `dl_p_class = ZEROES` per doc-04 §"Fielding Credit Proposal".
5. Wire the embedding-export step into the fold runner so the
   `dl_embedding_artifact` table is no longer a typed empty stub.
