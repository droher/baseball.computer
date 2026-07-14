# Validation gates

`just validate-gates` (read-only) sweep over every published artifact pointer, resolved the way `publish` / `validate` do (branch root `artifacts/statistical/published-<slug>/` shadows the global `artifacts/statistical/published/`; the `paper-revision` branch has no branch root, so this table resolves the global pointers). Each row runs `validate_artifact` and reports the validation status plus the codes of the findings that fired at `warn`/`block` severity. `hdi_cov` / `hdi_cov_param` are the two aggregate-surface coverage numbers defined below; `-` means the model has no coverage hook. Snapshot generated 2026-07-14 on branch `paper-revision`, `state-transition-v4` / `re-full-eraregime-v2` operating points.

| model | artifact_id | status | hdi_cov | hdi_cov_param | findings |
|---|---|---|---:|---:|---|
| assist_count | assist-count-v1 | passed | - | - | - |
| assist_credit_allocation | full-10k-v3-cut1-prod | passed | - | - | - |
| ball_handler_imputation | d-noprop-10k-v2 | passed | - | - | - |
| ball_handler_position_observedness | 10k-v5-fullscore | passed | - | - | bayes_post_pred_bucket_dev(warn) |
| dl_proposal_location_depth | phase3-location-depth-v8 | missing | - | - | - |
| dl_proposal_location_edge | phase3-location-edge-v8 | missing | - | - | - |
| dl_proposal_location_side | phase3-location-side-v8 | missing | - | - | - |
| dl_proposal_trajectory | phase3-trajectory-v9-cv | missing | - | - | - |
| general_location_observedness | 10k-v5-fullscore | passed | - | - | bayes_post_pred_bucket_dev(warn) |
| geometry_general_location | e-v12-noprop-general_location-zero | passed | - | - | - |
| geometry_location_depth | e-v12-noprop-location_depth-shrunk | failed | - | - | bayes_held_out_top1_not_beating_baseline(block) |
| geometry_location_edge | e-v12-noprop-location_edge-shrunk | passed | - | - | - |
| geometry_location_side | e-v12-noprop-location_side-shrunk | passed | - | - | - |
| geometry_trajectory | e-v12-noprop-trajectory-shrunk | passed | - | - | - |
| location_depth_observedness | 10k-v5-fullscore | passed | - | - | bayes_post_pred_bucket_dev(warn) |
| location_edge_observedness | 10k-v5-fullscore | passed | - | - | bayes_post_pred_bucket_dev(warn) |
| location_side_observedness | 10k-v5-fullscore | passed | - | - | bayes_post_pred_bucket_dev(warn) |
| park_factor_runs | pf-full-ar1-v3 | passed | - | - | - |
| pitch_summary | ps-cut1-full-v4 | passed | - | - | - |
| putout_credit_allocation | full-10k-v15-tuned | passed | - | - | - |
| run_expectancy | re-full-eraregime-v2 | passed | 0.8774 | 0.3235 | predictive_coverage_out_of_band(warn) |
| state_transition | state-transition-v4 | passed | 0.9772 | 0.2123 | - |
| trajectory_observedness | 10k-v5-fullscore | passed | - | - | bayes_post_pred_bucket_dev(warn) |

## The two coverage columns

Both numbers hold out the same deterministic 10% of games (`game_hash_fold(game_id, fold_count=10) == 0`), recompute the held-out cell realization from the model's own dataset parquet, and grade against the acceptance band `[0.88, 0.99]` for a nominal 94% interval. They differ in what interval the held-out realization is checked against.

- **`hdi_cov_param` (parameter HDI coverage).** Fraction of held-out cells whose empirical realization lands inside the published 94% HDI of the *parameter* (the cell's `cell_class_prob` for `state_transition`, its `re_value` for `run_expectancy`). This is the number the prior gate reported. It is misleading for dense cells: the parameter HDI is the posterior interval of the *mean* probability/rate and shrinks toward a point as the cell's event count grows, while the held-out *frequency* carries irreducible finite-sample multinomial (or sampling) noise at that same count. On a corpus this dense the two scales are orders of magnitude apart, so parameter coverage collapses to ~0.21 / ~0.32 even when the model is well calibrated — it is comparing a frequency against an interval that was never meant to contain it.

- **`hdi_cov` (posterior-predictive coverage, primary).** Fraction of held-out cells whose empirical realization lands inside a *predictive* 94% interval that folds finite-sample noise into the parameter uncertainty at the cell's own event count `n`. For `state_transition` (multinomial): per cell, draw M = 2000 seeded (`seed = 20260714`) posterior-predictive frequency vectors — a per-class parameter draw followed by an exact size-`n` multinomial — and form the per-`(cell, end_class)` predictive interval. For `run_expectancy` (mean): the predictive interval of the held-out cell **mean** of `runs_to_end_of_inning` is the posterior of `re_value` plus the central-limit sampling noise of a mean of `n` draws whose per-event spread is the held-out empirical standard deviation. This is the primary gate number.

**Method note on the parameter layer of the predictive draw.** No per-draw class probabilities are persisted — the published `*_summary` exports carry only per-cell mean / sd / 94% HDI. The predictive simulation therefore *approximates* the parameter posterior per class as an independent Normal(`prob_mean`, `prob_sd`) truncated to `[0, 1]` and renormalized across the cell's classes; an all-zero-uncertainty degenerate cell falls back to the normalized mean. This is an approximation of the true (correlated Dirichlet-like) parameter posterior, adequate here because the finite-sample multinomial layer dominates the predictive width on dense cells. For `run_expectancy` the mean's predictive interval is a Normal-CLT interval for the sample mean; the underlying NB dispersion is *not* persisted in the published summary, so the sampling-noise layer is reconstructed from the held-out empirical standard deviation rather than the NB variance — a deliberate substitution (a Normal predictive around a single NB draw would be wrong, but a Normal-CLT predictive around a mean of `n` draws is well justified and sidesteps the missing dispersion parameter).

**Reading the results.** `state_transition` predictive coverage is 0.9772 (in band; the intervals are slightly conservative) against a parameter coverage of 0.2123 — the predictive form is what confirms the transition surface is calibrated. `run_expectancy` predictive coverage is 0.8774, just below the 0.88 band floor, so it fires `predictive_coverage_out_of_band` at `warn` (the artifact still passes overall — no block): the posterior HDIs for `re_value` slightly under-cover held-out cell means even after folding in sampling noise, a mild under-dispersion worth flagging but far from the parameter-coverage artifact of 0.3235.

## Held-out ECE availability caveat

The multinomial coverage models (E, D, C) write `validation/held_out_metrics.json` with a `distribution_calibration` block only for fits produced after the calibration-gate wiring landed; the number of held-out reliability/ECE fields on any given artifact depends on when it was fit. A multiclass **top-label ECE** for the geometry imputation models is *not* reconstructable read-only from `exports/` + dataset parquet alone: the published `geometry_probabilities.parquet` scores only the geometry-**unobserved** production slice, which carries no ground-truth label, while the observed held-out events that do carry truth have no persisted per-event prediction (verified: the observed-`location_depth` event set and the export event set have **0** overlapping `event_key`s). Producing a per-event top-label ECE would require reloading the posterior and re-running the DL-aware softmax reconstruction over the held-out observed slice, which is neither an `exports`+dataset computation nor cheap/robust. The disposition below therefore reports the held-out calibration that *is* persisted plus a marginal log-loss lift computed offline read-only.

## `geometry_location_depth` disposition

`geometry_location_depth` is the one `failed` row: its gate fires the pre-existing block `bayes_held_out_top1_not_beating_baseline` because held-out top-1 accuracy (0.5609) does not beat the majority-class baseline (0.5611) — `location_depth` is dominated by the `Default` class, so top-1 is a near-useless discriminator here. The block finding is left as-is in code. The open question is whether the model's *calibrated shares* are nonetheless good even though top-1 ties the majority baseline. The read-only evidence says yes: on the held-out observed slice (505,921 events) the predicted marginal class shares match the empirical shares to a total-variation distance of 0.0051 (max per-class absolute deviation 0.0044), and held-out log-loss is 1.0816 nats against a marginal-entropy baseline of 1.1219 nats — a +0.0403-nat lift, i.e. the per-event probability vectors carry real event-level information beyond the constant marginal predictor, even though that information is not enough to flip the arg-max off `Default`. Per-season slice calibration is stable (weighted TV 0.0234, worst-season TV 0.2345 on a thin early-era slice). Disposition: the top-1 block is a correct statement about arg-max utility on a majority-dominated dimension, not a calibration defect; the shares are well-calibrated and mildly informative, so the surface is fit for probabilistic (share-weighted) consumption while the block correctly warns against treating its arg-max as a point label.

## Regeneration

```
just validate-gates
```

Read-only. Add `--write` to persist each `validation_report.json` beside its artifact; pass model names to restrict the sweep. The `geometry_location_depth` disposition numbers come from that artifact's `validation/held_out_metrics.json` (`distribution_calibration` + `log_loss`) with the marginal-entropy baseline computed from the block's empirical shares.
