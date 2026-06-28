---
title: Phase 3 Exit — Deep Learning Supplements
type: phase-exit-summary
status: closed
audience: humans-and-agents
last-verified: 2026-05-17
---

# Phase 3 Exit — Deep Learning Supplements

Phase-3 ships every DL supplement on inference-aligned inputs and
warms each per-target Embedding from a shared `event_universe`
residual-decomposition pretrain. Stage-1 fits a context-only
spec; offsets are emitted per head; stage-2 fits the full layout
(with `('player', ('batter_id', 'pitcher_id'))` embedding group plus
ungrouped `park_id` / `scorer`) on the residual.

## Scope

| Surface | Phase 3 |
| --- | --- |
| Pretrain architecture | residual decomposition: stage-1 context-only logits → stage-2 full layout fits the residual via cached offset inputs |
| Pretrain inputs | pre-event context + observed-outcome columns (`pa_result`, `outs_on_play_capped`, `runs_on_play_capped`, `r1/r2/r3_advancement`); pretrain layout is intentionally exempt from `validate_pre_event` |
| Pretrain heads | 5 imputation targets: `trajectory_remapped`, `batted_location_general`, `batted_location_depth`, `batted_location_edge`, `batted_to_fielder_class` |
| Pretrain row filter | `pa_result IN (11 batted-ball outcomes)` |
| Training labels (downstream + heads) | `observed_status = 'observed'` only — no heuristic deductions in DL training-label path |
| Pitch-summary DL | dropped — `has_count` + `has_pitch_sequence` + per-stream dims move to Phase 4+ |
| Fielding-credit DL | dropped — `dl_credit_proposal_manifest` materializes as zero-row stub; Phase-4 hierarchical Bayes owns spatial allocation. `batted_to_fielder_class` remains as an auxiliary pretrain head |
| Advancement DL | deferred — `model_input_advancement` lacks `advancement_class` + `time_forward_fold` columns. Specs defined in tree but not registered on import |
| Active downstream supplements | trajectory + 3 location dims (4 specs total) |

## Acceptance criteria

| # | Criterion | Status | Where |
| --- | --- | --- | --- |
| 1 | OOF predictions present for every training row | scaffolding ready | `deep/training.run_target()` |
| 2 | Calibration passes by slice (era / scorer / source / hit-vs-out / missingness) | calibrator wrappers shipped | `deep/calibrators.py` |
| 3 | Probability vectors normalize within key/class groups | validator shipped | `validate.py:_check_probability_normalization` |
| 4 | Personnel + structural masks applied before fielding proposals | N/A (fielding-credit DL out of scope) | `deep/targets/fielding_credit.py` |
| 5 | Embedding probes documented and per-source-family AUC recorded | probe shipped | `deep/leakage_probes.source_probe_held_out` |
| 6 | Baseline (deterministic geometry rule) log-loss/Brier comparison | hook present | `validate.py:_compare_against_baseline` |
| 7 | No SQL artifact exposes only argmax | guarded | `dl_p_class DOUBLE[]` across model_input views; `validate.py:_check_deep_schema` |
| 8 | Every supplement layout is pre-event | enforced at registration | `feature_layout.validate_pre_event` + `bc/tests/statistical/deep/test_pre_event_layout.py` |
| 9 | Pretrain heads non-redundant | enforced by spec | `bc/tests/statistical/deep/test_pretrain_layout.py` |
| 10 | Per-supplement entity Δ_CE ratio ≥ ×1.5 batter/scorer, no regress park/pitcher | per supplement | [`phase3-acceptance-gates-v6.md`](phase3-acceptance-gates-v6.md) |

## How to evaluate

```sh
# 1. Verify layouts + pretrain spec contracts
PYTHONPATH=$(pwd)/bc uv run --group ml pytest \
    bc/tests/statistical/deep/test_pre_event_layout.py \
    bc/tests/statistical/deep/test_pretrain_layout.py \
    bc/tests/statistical/deep/test_advancement_targets.py

# 2. Per-supplement gates (after the publish cascade)
just validate-artifact phase3-trajectory-v8
just validate-artifact phase3-location-side-v8
just validate-artifact phase3-location-depth-v8
just validate-artifact phase3-location-edge-v8

# 3. Exit-gate aggregator (must exit 0)
BC_STATS_PUBLISHED_ROOT=$(pwd)/artifacts/statistical/published-data_coverage/ \
    PYTHONPATH=$(pwd)/bc \
    uv run --group ml python scripts/check_phase3_exit.py
```

The active pretrain pointer at
`artifacts/statistical/published-data_coverage/pretrain/event_universe.json`
resolves `event_universe` to `phase3-pretrain-clean`. Downstream
`pretrained_embeddings_artifact_id="event_universe"` resolves directly
— no `BC_DEEP_PRETRAIN_ARTIFACT_OVERRIDE` needed.

## Open follow-ups

See `notes/followups.md`:

- `model_input_advancement` SQL gaps blocking advancement fits.
- Park-factors / run-values stay in Phase 4 (Bayes), not DL.
- Pitch-summary DL + fielding-credit DL re-evaluation in Phase 4+.
