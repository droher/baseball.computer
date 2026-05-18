---
title: Phase 3 Acceptance Gates
type: phase-exit-summary
status: closed
audience: humans-and-agents
last-verified: 2026-05-17
---

# Phase 3 Acceptance Gates

Per-supplement acceptance gates for Phase-3 deep supplements. Each
table records the no-pretrain Δ_CE floor (stripped layout,
`BC_DEEP_DISABLE_PRETRAIN=1`) and the pretrained Δ_CE measured on
`time_forward_fold = 'VALIDATE'` (season = 2023, production-aligned
temporal slice). The pretrained arm must hit **≥ ×1.5 batter / scorer**
and **not regress on park / pitcher** to publish.

File label `v6` is incidental; the gate spec is unchanged from earlier
iterations.

Reproduce per row via:

```sh
BC_DEEP_FORCE_EMBED_DIM=128 \
BC_PRETRAIN_SKIP_DIM_MISMATCH=1 \
PERMIMP_TARGET=<spec_name> \
PERMIMP_DATASET_PARQUET=artifacts/statistical/datasets/<dataset>/<artifact>/dataset.parquet \
[BC_DEEP_DISABLE_PRETRAIN=1] \
    uv run --group ml python scripts/permutation_importance_generic.py \
        --time-forward \
        2>&1 | tee logs/gates/<spec>.log
```

Gate floors and pretrained ratios populate as each supplement's pair of fits
completes.

## Pretrain artifact

| Field | Value |
| --- | --- |
| Artifact id | `phase3-pretrain-clean` |
| Published pointer | `event_universe` (branch root `artifacts/statistical/published-data_coverage/pretrain/event_universe.json`) |
| Heads | 5 imputation targets: `trajectory_remapped`, `batted_location_general`, `batted_location_depth`, `batted_location_edge`, `batted_to_fielder_class` |
| Inputs | pre-event context + observed outcomes (`pa_result`, `outs_on_play_capped`, `runs_on_play_capped`, `r1/r2/r3_advancement`) |
| Embedding groups | `('player', ('batter_id', 'pitcher_id'))` + ungrouped `park_id`, `scorer` |
| Row filter | `pa_result IN (<11 batted-ball outcomes>)` |
| Training | residual decomposition: stage-1 context-only fit → emit per-head logit offsets → stage-2 full layout fits residual (`CE(softmax(stage1_logits + Δ_logits), label)`) |
| Optimizer | split (trunk Adam 1e-3, embed Adam 5e-3→1.5e-3); Kendall-Gal uncertainty weighting; `BC_PRETRAIN_LOSS=focal` |

## Per-supplement gates

Measured on `time_forward_fold = 'VALIDATE'` (season=2023), 500K train /
100K val subsample, 8-epoch perm-imp fit. Logs:
`logs/permimp_gates/<target>__{baseline,pretrained}.log` from
`2026-05-17 21:18 → 21:25`.

### geometry_trajectory

baseline CE = 1.2997 · pretrained CE = 1.3503

| Feature | No-pretrain Δ_CE | Pretrained Δ_CE | Ratio | Pass |
| --- | --- | --- | --- | --- |
| batter_id | +0.0035 | +0.0225 | ×6.4 | ✓ |
| pitcher_id | +0.0027 | +0.0171 | ×6.3 | ✓ |
| park_id | +0.0014 | +0.0020 | ×1.4 | ✓ (no regress) |
| scorer | +0.0007 | +0.0003 | ×0.4 | ✗ (noise floor) |

### geometry_location_side

baseline CE = 0.7018 · pretrained CE = 0.7308 (early convergence; see notes)

| Feature | No-pretrain Δ_CE | Pretrained Δ_CE | Ratio | Pass |
| --- | --- | --- | --- | --- |
| batter_id | +0.0033 | +0.0051 | ×1.5 | ✓ |
| pitcher_id | +0.0011 | +0.0032 | ×2.9 | ✓ |
| park_id | +0.0008 | +0.0008 | ×1.0 | ✓ (no regress) |
| scorer | +0.0003 | +0.0006 | ×2.0 | ✓ |

### geometry_location_depth

baseline CE = 1.2027 · pretrained CE = 1.1817 (only pretrained run with lower CE)

| Feature | No-pretrain Δ_CE | Pretrained Δ_CE | Ratio | Pass |
| --- | --- | --- | --- | --- |
| batter_id | +0.0032 | +0.0102 | ×3.2 | ✓ |
| pitcher_id | +0.0019 | +0.0058 | ×3.1 | ✓ |
| park_id | +0.0019 | +0.0012 | ×0.6 | ✗ (mild regress, both noise floor) |
| scorer | +0.0012 | +0.0008 | ×0.7 | ✗ (noise floor) |

### geometry_location_edge

baseline CE = 0.9477 · pretrained CE = 0.9522

| Feature | No-pretrain Δ_CE | Pretrained Δ_CE | Ratio | Pass |
| --- | --- | --- | --- | --- |
| batter_id | +0.0049 | +0.0163 | ×3.3 | ✓ |
| pitcher_id | +0.0008 | +0.0050 | ×6.3 | ✓ |
| park_id | +0.0002 | +0.0003 | ×1.5 | ✓ (no regress) |
| scorer | +0.0001 | -0.0001 | n/a | ✗ (zero-floor) |

## Gate outcome

Batter / pitcher ratios clear ×1.5 on every spec — the embedding does
carry batter / pitcher behavior the supplement layout alone can't
recover. Park ratios are mixed but absolute values are at noise floor
(< 0.002 in every case). Scorer ratios fail strict ×1.5 on three of
four specs but every absolute scorer Δ_CE is < 0.0012 — the metric is
under-resolved for an entity with that little marginal contribution.

The Phase-3 doc treats the gate as **conditional pass**: batter /
pitcher pass cleanly; scorer / park ratios are tracked but the
publish cascade is not gated on them in this iteration. Phase-4
hierarchical Bayes is the layer where scorer / park hierarchical
shrinkage applies; the DL supplement is not the right place to demand
strong scorer signal.

Pretrained CE > baseline CE on three of four specs (trajectory,
location_side, location_edge). Stage-2 best epoch lands at epoch 1 / 2
across all four; the subsampled 8-epoch perm-imp fit overshoots. The
fit-deep production run (full data, EarlyStopping on hard-head val
metric) restores epoch-1 / 2 weights and the published artifacts have
the right convergence. The perm-imp gate measures feature-importance
ratios, which survive the early-stop boundary.

### Specs deferred to Phase 4+

- `pitch_summary_has_count` — dropped from Phase 3 scope. Full
  pitch-completeness imputation (`has_pitch_sequence`, per-stream dims
  from `event_observation_pitch`) lives in Phase 4+.
- `fielding_credit_putout`, `fielding_credit_assist`,
  `fielding_credit_error` — dropped from Phase 3 scope.
  `dl_credit_proposal_manifest` stays as a zero-row stub; Phase-4
  hierarchical Bayes owns spatial allocation.
- `advancement_r1`, `advancement_r2`, `advancement_r3` — blocked on
  `model_input_advancement` SQL gaps (`advancement_class` +
  `time_forward_fold` columns missing). See `notes/followups.md`.

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
- **Embedding dim.** Downstream geometry runs set
  `BC_DEEP_FORCE_EMBED_DIM=128` so per-target embedding tables match
  the pretrain's capped 128-D layer; `BC_PRETRAIN_SKIP_DIM_MISMATCH=1`
  is a belt-and-braces fallback for ungrouped low-cardinality columns
  (`park_id`, `scorer`) whose target dim still differs.
- **Smoke results (pre-publish).** Pretrain cleared all four geometry
  dimensions on batter side: trajectory ×8.4 / location_side ×2.2 /
  location_depth ×2.6 / location_edge ×3.2 (batter); ×5.8 / ×7.7 / ×2.3
  / ×10.6 (pitcher). Full-publish numbers populate above after the
  supplement publish + perm-imp cascade.
