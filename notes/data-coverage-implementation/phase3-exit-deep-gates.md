---
title: Phase 3 Exit — Deep Learning Supplements (v6)
type: phase-exit-summary
status: in-flight
audience: humans-and-agents
last-verified: 2026-05-16
---

# Phase 3 Exit — Deep Learning Supplements (v6)

Phase-3 ships every DL supplement on **inference-aligned inputs** and
warms each per-target Embedding from a shared `event_universe`
pretrain. v6 is the rearchitecture that replaces v5 after the v5 A8
ablation winner failed the trajectory entity gates because the
trajectory input layout included post-event columns
(`fielder_chain`, `pa_result`, etc.) — the model shortcuts to those and
never has to learn entity priors. v6 closes the leak across every
supplement.

## What changed vs v5

| Surface | v5 | v6 |
| --- | --- | --- |
| Trajectory layout | Includes `fielder_chain`, `pa_result`, `batted_to_fielder*`, `outs_on_play*` | Pre-event only |
| Pitch_summary LOW_CARD | + `result_family` | Pre-event only |
| Fielding_credit LOW_CARD | + `gap_class`, `fielding_evidence_status` | Pre-event only |
| Pretrain pretext heads | 3 (`pa_result`, `hit_or_out`, `trajectory_remapped`) in `EVENT_UNIVERSE_HEADS`; OR 13 in `EVENT_UNIVERSE_HEADS_V4` | 11 non-redundant (`EVENT_UNIVERSE_HEADS_V6`) — drops `result_family` and `hit_or_out` (both deterministic from `pa_result`); remaining 11 still correlate but each adds residual variance |
| Hard-head EarlyStopping watchlist | `(pa_result, trajectory_remapped, hit_or_out)` | `(pa_result, trajectory_remapped, batted_to_fielder_class)` — replaces deterministic-from-pa_result watcher with a non-derivable one |
| `validate_pre_event` deny-list | absent | `feature_layout.validate_pre_event(layout)` called from every `_register()` |
| Acceptance gate slice | `primary_fold` (HASH(game_id)) | `time_forward_fold` (season=2023) |
| Advancement spec | not registered | 3 per-runner specs registered (fit pending dataset gap) |

## Acceptance criteria

| # | Criterion | Status | Where |
| --- | --- | --- | --- |
| 1 | OOF predictions present for every training row | scaffolding ready | `deep/training.run_target()` |
| 2 | Calibration passes by slice (era / scorer / source / hit-vs-out / missingness) | calibrator wrappers shipped | `deep/calibrators.py` |
| 3 | Probability vectors normalize within key/class groups | validator shipped | `validate.py:_check_probability_normalization` |
| 4 | Personnel + structural masks applied before fielding proposals | filter_predicate enforces `eligible_for_allocation` | `deep/targets/fielding_credit.py` |
| 5 | Embedding probes documented and per-source-family AUC recorded | probe shipped | `deep/leakage_probes.source_probe_held_out` |
| 6 | Baseline (deterministic geometry rule) log-loss/Brier comparison | hook present | `validate.py:_compare_against_baseline` |
| 7 | No SQL artifact exposes only argmax | guarded | `dl_p_class DOUBLE[]` across model_input views; `validate.py:_check_deep_schema` |
| 8 | **v6: every supplement layout is pre-event** | enforced at registration | `feature_layout.validate_pre_event` + `bc/tests/statistical/deep/test_pre_event_layout.py` |
| 9 | **v6: pretrain heads non-redundant (no deterministic derivation between heads)** | enforced at registration | `bc/tests/statistical/deep/test_pretrain_heads_v6.py` |
| 10 | **v6: per-supplement entity Δ_CE ratio ≥ ×1.5 batter/scorer, no regress park/pitcher** | per supplement | [`phase3-acceptance-gates-v6.md`](phase3-acceptance-gates-v6.md) |

## How to evaluate

```sh
# 1. Verify every layout passes pre-event validation
PYTHONPATH=$(pwd)/bc uv run --group ml pytest \
    bc/tests/statistical/deep/test_pre_event_layout.py \
    bc/tests/statistical/deep/test_pretrain_heads_v6.py \
    bc/tests/statistical/deep/test_advancement_targets.py

# 2. v6 pretrain sidecar
PYTHONPATH=$(pwd)/bc BC_DB_PATH=$(pwd)/bc_dev.db \
    uv run --group ml python scripts/pretrain_eval_pretrain.py phase3-pretrain-v6

# 3. Per-supplement gates (after each supplement re-fit)
just validate-artifact phase3-trajectory-v6
just validate-artifact phase3-pitch-summary-v6
just validate-artifact phase3-fc-putout-v6
just validate-artifact phase3-fc-assist-v6
just validate-artifact phase3-fc-error-v6
```

## Open follow-ups

See [`notes/followups.md` → Phase-3 v6 rearchitecture
follow-ups](../followups.md#phase-3-v6-rearchitecture-follow-ups) for:

- `model_input_advancement` SQL gaps blocking advancement fits.
- Park-factors / run-values stay in Phase 4 (Bayes), not DL.
- Per-supplement gate measurements pending v6 publish + supplement
  refit cascade.
