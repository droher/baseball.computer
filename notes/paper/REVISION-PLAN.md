---
title: Revision plan — modeling response to the referee report
type: design-doc
status: draft
audience: agents, David
last-verified: 2026-07-13
---

# Revision plan

Responds to notes/paper/REFEREE-REPORT.md (major revision). Branch:
`paper-revision`. Referee majors 1–9 map to the work below; three majors
(related work, B/I/K framing, pipeline-vs-contribution framing) are
paper-writing only and land with the final revision pass.

## Decisions locked by code recon

- **Anchored δ (referee 2, 3).** New module `bc/python_models/statistical/
  mnar_anchor.py` + driver `scripts/mnar_anchor.py`. For trajectory, contrast
  the observed slice (`observed_status='observed'`, `raw_value`) against the
  derived slice (`observed_status='derived'`, `deduced_value`) of
  `main_models.model_input_geometry`, per season bucket. The anchor targets
  the masked-slice distribution: p_masked(c|e) from the derived slice alone,
  δ_c(e) = log(p_masked_c / p_obs_c), centered to zero mean per era — offsets
  are identified only up to an additive constant. Era key: explicit season
  buckets defined in the module (pre-1950, 1950–1987, 1988+ to match the
  paper, plus per-decade for the export); do NOT reuse the multi-hot
  ERA_REGIME_LABELS — it is not a partition. Stated assumption, published with
  the artifact: the derived slice recovers only deduced-recoverable classes
  (ground balls via fielding strings), so the anchor is a partial-truth
  estimate of selection, not full truth; rows with observed_status in
  ('unknown_code','missing') remain uncharacterized.
- **Joint ribbon (referee 3).** `sensitivity.py::marginal_shares` is already
  vector-capable. Extend the sweep, don't replace it: new
  `joint_sensitivity_ribbon()` sweeping t · δ_anchor for t ∈ {0, 0.5, 1.0,
  1.5} plus per-class perturbations of ±0.25 nats around t=1, reusing the
  closed-form per-event renormalization generalized to vector offsets.
  `ribbon_band()` is grid-agnostic and needs no change. Export gains
  `era_bucket` and `sweep_kind` (marginal | joint_anchor) columns;
  `scripts/sensitivity_ribbon.py::publish_ribbons` grows the era group-by and
  a `--joint` mode consuming the anchor artifact.
- **Backtest robustness (referee 2).** The harness is parameterized by
  `MaskConfig` (`backtests/mnar_masked.py:150`); add three registered designs,
  not a rewrite: (i) covariate-joint selection — mask probability depends on
  class × a covariate (batter hand or park) so the true selection is not the
  pure per-class form the correction assumes; (ii) scorer-blocked — whole
  games/scorer buckets masked at high rate (mimics source-block absence);
  (iii) era-graded intensity — intensity a function of season, not a salted
  hash. Run all designs at the existing smoke budget through the existing
  offset arm (`evaluate_offset_arm`); report where the fixed per-class offset
  degrades. Expectation to publish either way: the correction is exact under
  (iii), approximate under (i), and should fail under (ii) — that failure is
  the honest scope statement.
- **Linear weights finite-sample (referee 9).** Do NOT add a per-draw
  state-transition export (dropped deliberately for scale). Instead, in
  `linear_weights_estimated.py::propagate_linear_weights_draws`, replace fixed
  transition counts with Dirichlet-multinomial conjugate draws: per
  (season, league, play), draw the play-frequency vector from
  Dirichlet(counts + α), α = 0.5 (Jeffreys), one draw per RE-posterior draw,
  and propagate jointly. Sparse cells widen automatically; dense cells are
  unchanged to first order. Acceptance check: band width strictly
  non-decreasing vs the current artifact on ≥99% of cells, with the largest
  widenings on the smallest-n cells; report the old-vs-new width table.
- **Calibration gates (referee 1, 9, 6, 7).** The gap is wiring, not
  primitives: `validation/held_out_metrics.json` (already written by the
  Bernoulli branch with roc_auc/pr_auc/ece_held_out) is never read by
  `validate.py` or `publication.py`. Work: (a) `_validate_bayes` reads
  held_out_metrics.json when present — ece_held_out and AUC-vs-baseline become
  graded checks feeding validation_status; (b) add held-out reliability/ECE
  computation to the multinomial branch (E, D, C) mirroring the Bernoulli
  branch; (c) new HDI-coverage validator: for aggregate surfaces (run
  expectancy, transitions, park factors), empirical coverage of the 94% HDI on
  held-out season folds; (d) a runnable `just validate-gates` sweep over
  published artifacts replacing the phase-6 markdown checklist, so
  confidence_status can move from exploratory to passed on evidence.
- **Model A OOS wiring (beyond referee).** Mostly done in
  `training.py:2586` — the residual work is (a) above plus confirming all six
  obs targets ran with `inputs.held_out` set; fold into the calibration task.

## Beyond-referee additions (why they belong in this round)

1. Gate-wiring of existing held-out metrics (above) — the referee asked for
   calibration evidence; the repo already computes half of it and ignores it.
2. Publish validation artifacts as paper supplements — backtest metrics.json,
   ribbon parquets, calibration curves, gate results — answering referee 6
   (unverifiability) with artifacts, not prose.
3. justfile recipes for `sensitivity_ribbon` and `mnar_masked_backtest`
   (currently direct-invocation scripts) — reproducibility section stops
   describing manual commands.
4. 2020s trajectory NULL propensity cell QA (tables/obs_propensity_by_decade)
   — likely the saturated-season filter; diagnose, then either fix or document
   as intended behavior in the table and paper.
5. gamma_dl ablation table from existing zero/shrunk artifact pairs (referee
   4) — analysis only.

## Out of scope this round

Model H buildout (upstream SQL missing advancement_class), error-credit and DP
unblocks (no signal/truth columns), joint EM selection model (contradicts the
paper's stance), Statcast second-source ingestion (parser repo), DuckLake
publish path.

## DAG

```
plan (done)
  ├─ T2 mnar_anchor module ──┬─ T3 backtest designs (uses anchor for expectations)
  │                          └─ T4 joint ribbon (consumes anchor artifact)
  ├─ T5 linear-weights Dirichlet
  ├─ T6 calibration gates (absorbs T7 Model A wiring)
  ├─ T8 NULL-cell QA
  └─ T9 gamma_dl ablation analysis
T6 ─ T10 gate sweep → confidence_status
T3,T4,T5,T6,T9,T10 ─ T11 paper revision + response to reviewers + PDF
```

Agents: implementation via Sonnet/Opus subagents per task, code-review round
before merge, fits run detached where >10 min. No subagent runs mutating git.
