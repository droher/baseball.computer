---
title: Phase 3 v6 Acceptance Gates
type: phase-exit-summary
status: in-flight
audience: humans-and-agents
last-verified: 2026-05-16
---

# Phase 3 v6 Acceptance Gates

Per-supplement acceptance gates for the Phase-3 v6 rearchitecture. Each
table records the no-pretrain Δ_CE floor (stripped layout,
`BC_DEEP_DISABLE_PRETRAIN=1`) and the v6-pretrained Δ_CE measured on
`time_forward_fold = 'VALIDATE'` (season = 2023, production-aligned
temporal slice). v6 must hit **≥ ×1.5 batter / scorer** and **not
regress on park / pitcher** to publish.

Reproduce per row via:

```sh
PERMIMP_TARGET=<spec_name> \
PERMIMP_DATASET_PARQUET=artifacts/statistical/datasets/<dataset>/<artifact>/dataset.parquet \
[BC_DEEP_DISABLE_PRETRAIN=1] \
    uv run --group ml python scripts/permutation_importance_generic.py \
        --time-forward \
        2>&1 | tee logs/gates/<spec>_v6.log
```

Gate floors and v6 ratios populate as each supplement's pair of fits
completes.

## v6 pretrain artifact

| Field | Value |
| --- | --- |
| Artifact id | `phase3-pretrain-v6` |
| Heads | 11 non-redundant (drops `result_family`, `hit_or_out`); remaining heads still correlate but no head is a deterministic function of another |
| Dataset artifact | `phase3-pretrain-v2-prep` (reused from v5; lacks `time_forward_fold` — eval falls back to season) |
| Optimizer | A8 split (trunk Adam 1e-3, embed Adam 5e-3) |
| Schedule | Single-stage joint fit, stock `val_loss` EarlyStopping (patience 5) |
| Sidecar | `scripts/pretrain_eval_pretrain.py phase3-pretrain-v6` |

### v5-A8 baseline (regression guard target)

v6 must hit `downstream_proxy_score` ≥ v5-A8 − 0.5σ:

| Metric | v5-A8 |
| --- | --- |
| `downstream_proxy_score` (mean macro-F1 over `pa_result` / `trajectory_remapped` / `outs_on_play_capped`) | 0.2022 |
| `confound_leak.era_decade` Δ vs random | +0.161 |
| `confound_leak.primary_league` Δ vs random | +0.047 |
| `confound_leak.dh_regime` Δ vs random | +0.150 |

v6 sidecar will populate `<artifact_dir>/eval/eval_report.json`.

## Per-supplement gates

### geometry_trajectory

| Feature | No-pretrain Δ_CE | v6-pretrained Δ_CE | Ratio | Pass |
| --- | --- | --- | --- | --- |
| batter_id | _pending_ | _pending_ | _pending_ | _pending_ |
| pitcher_id | _pending_ | _pending_ | _pending_ | _pending_ |
| park_id | _pending_ | _pending_ | _pending_ | _pending_ |
| scorer | _pending_ | _pending_ | _pending_ | _pending_ |

### pitch_summary_has_count

| Feature | No-pretrain Δ_CE | v6-pretrained Δ_CE | Ratio | Pass |
| --- | --- | --- | --- | --- |
| batter_id | _pending_ | _pending_ | _pending_ | _pending_ |
| pitcher_id | _pending_ | _pending_ | _pending_ | _pending_ |
| park_id | _pending_ | _pending_ | _pending_ | _pending_ |

### fielding_credit_putout

| Feature | No-pretrain Δ_CE | v6-pretrained Δ_CE | Ratio | Pass |
| --- | --- | --- | --- | --- |
| batter_id | _pending_ | _pending_ | _pending_ | _pending_ |
| pitcher_id | _pending_ | _pending_ | _pending_ | _pending_ |
| park_id | _pending_ | _pending_ | _pending_ | _pending_ |

### fielding_credit_assist

| Feature | No-pretrain Δ_CE | v6-pretrained Δ_CE | Ratio | Pass |
| --- | --- | --- | --- | --- |
| batter_id | _pending_ | _pending_ | _pending_ | _pending_ |
| pitcher_id | _pending_ | _pending_ | _pending_ | _pending_ |
| park_id | _pending_ | _pending_ | _pending_ | _pending_ |

### fielding_credit_error

| Feature | No-pretrain Δ_CE | v6-pretrained Δ_CE | Ratio | Pass |
| --- | --- | --- | --- | --- |
| batter_id | _pending_ | _pending_ | _pending_ | _pending_ |
| pitcher_id | _pending_ | _pending_ | _pending_ | _pending_ |
| park_id | _pending_ | _pending_ | _pending_ | _pending_ |

### Specs registered but unfittable in this round

- `advancement_r1`, `advancement_r2`, `advancement_r3` — blocked on
  `model_input_advancement` SQL gaps (`advancement_class` +
  `time_forward_fold` columns missing). See [`notes/followups.md`
  → Phase-3 v6 rearchitecture
  follow-ups](../followups.md#phase-3-v6-rearchitecture-follow-ups).

### Specs out of DL scope

- `park_factors`, `run_values` — kept in Phase-4 hierarchical-Bayes
  layer per `04-deep-learning-supplements.md` §"Where Deep Learning
  Helps". DL residual diagnostics are still optional EDA inputs.

## Methodology notes

- **Training split** stays on `primary_fold` (HASH(game_id)) to
  maximize data. Only the eval slice for gates uses
  `time_forward_fold`.
- **Subsampling.** `permutation_importance_generic.py` subsamples to
  500K train / 100K val rows for fast iteration; use
  `--full` / `PERMIMP_FULL=1` to disable.
- **No-pretrain baseline.** Set `BC_DEEP_DISABLE_PRETRAIN=1` so
  `_fit_keras` skips `set_pretrained_embeddings`. Embedding layers
  initialize from scratch — measures what entity signal the supplement
  layout alone can recover.
- **Why batter / scorer benefit most.** v5 perm-imp on stripped
  trajectory showed A8 pretrain lifts batter Δ_CE ×2.45, scorer ×1.80,
  pitcher ×1.36, park ×1.35 vs no-pretrain. Pretrain is most valuable
  for entities whose downstream supplement has too few per-entity
  events to recover the prior alone.
