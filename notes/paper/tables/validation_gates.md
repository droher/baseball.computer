> Historical September 4, 2026 snapshot under gate version 2. Its `passed` labels are not current validation claims. See [current evidence](../EVIDENCE.md) and the revised manuscript.

# Validation gates

`just validate-gates` (read-only) sweep over every published artifact pointer, resolved the way `publish` / `validate` do (branch root `artifacts/statistical/published-<slug>/` shadows the global `artifacts/statistical/published/`; the `main` branch has no branch root, so this table resolves the global pointers). Each row runs `validate_artifact` and reports the validation status plus the codes of the findings that fired at `warn`/`block` severity. `manifest_status` is the `validation_status` now stamped on the artifact's manifest by `just validate-gates --write`, which is what a later re-publication copies into the table's `confidence_status`. `hdi_cov` / `hdi_cov_param` are the two aggregate-surface coverage numbers defined below; `-` means the model has no coverage hook. Snapshot generated 2026-09-04 on branch `modeling-review-fixes`, `state-transition-v5` / `re-full-eraregime-v4` operating points, gate version 2. Two columns are new since the last snapshot: `gate_v` is the gate version under which the manifest status was last stamped, and a table's `confidence_status` reads as `exploratory` unless that number matches the current `VALIDATION_GATE_VERSION`, which is 2. `weak_id` is a flag set when any group-level rhat exceeds 1.01 or any group-level bulk ESS is below 400, recomputed over every posterior variable in `inference/posterior.nc`.

| model | artifact_id | status | manifest_status | gate_v | hdi_cov | hdi_cov_param | weak_id | findings |
|---|---|---|---|---:|---:|---:|---|---|
| assist_count | assist-count-v1 | passed | passed | 2 | - | - | False | - |
| assist_credit_allocation | full-10k-v3-cut1-prod | passed | passed | 2 | - | - | True | - |
| ball_handler_imputation | d-noprop-10k-v2 | passed | passed | 2 | - | - | False | - |
| ball_handler_position_observedness | 10k-v6-unseen-fix | passed | passed | 2 | - | - | True | bayes_post_pred_bucket_dev(warn) |
| dl_proposal_location_depth | phase3-location-depth-v8 | missing | - | - | - | - | - | - |
| dl_proposal_location_edge | phase3-location-edge-v8 | missing | - | - | - | - | - | - |
| dl_proposal_location_side | phase3-location-side-v8 | missing | - | - | - | - | - | - |
| dl_proposal_trajectory | phase3-trajectory-v9-cv | missing | - | - | - | - | - | - |
| general_location_observedness | 10k-v6-unseen-fix | passed | passed | 2 | - | - | False | bayes_post_pred_bucket_dev(warn) |
| geometry_general_location | e-v12-noprop-general_location-zero | passed | passed | 2 | - | - | False | - |
| geometry_location_depth | e-v12-noprop-location_depth-zero | passed | passed | 2 | - | - | False | bayes_held_out_top1_not_beating_baseline(warn) |
| geometry_location_edge | e-v12-noprop-location_edge-zero | passed | passed | 2 | - | - | False | bayes_held_out_top1_not_beating_baseline(warn) |
| geometry_location_side | e-v12-noprop-location_side-zero | passed | passed | 2 | - | - | False | bayes_held_out_top1_not_beating_baseline(warn) |
| geometry_trajectory | e-v12-noprop-trajectory-shrunk | passed | passed | 2 | - | - | False | - |
| location_depth_observedness | 10k-v6-unseen-fix | passed | passed | 2 | - | - | True | bayes_post_pred_bucket_dev(warn) |
| location_edge_observedness | 10k-v6-unseen-fix | passed | passed | 2 | - | - | True | bayes_post_pred_bucket_dev(warn) |
| location_side_observedness | 10k-v6-unseen-fix | passed | passed | 2 | - | - | False | bayes_post_pred_bucket_dev(warn) |
| park_factor_runs | pf-full-ar1-v4 | passed | passed | 2 | - | - | False | - |
| pitch_summary | ps-cut1-full-v7 | passed | passed | 2 | - | - | False | - |
| putout_credit_allocation | full-10k-v16-production-export | passed | passed | 2 | - | - | False | - |
| run_expectancy | re-full-eraregime-v4 | passed | passed | 2 | 0.9062 | 0.3861 | False | - |
| state_transition | state-transition-v5 | passed | passed | 2 | 0.9780 | 0.2042 | True | - |
| trajectory_observedness | 10k-v6-unseen-fix | passed | passed | 2 | - | - | True | bayes_post_pred_bucket_dev(warn) |

## The two coverage columns

Both numbers hold out the same deterministic 10% of games (`game_hash_fold(game_id, fold_count=10) == 0`), recompute the held-out cell realization from the model's own dataset parquet, and grade against the acceptance band `[0.88, 0.99]` for a nominal 94% interval. They differ in what interval the held-out realization is checked against.

- **`hdi_cov_param` (parameter HDI coverage).** Fraction of held-out cells whose empirical realization lands inside the published 94% HDI of the *parameter* (the cell's `cell_class_prob` for `state_transition`, its `re_value` for `run_expectancy`). This is the number the prior gate reported. It is misleading for dense cells: the parameter HDI is the posterior interval of the *mean* probability/rate and shrinks toward a point as the cell's event count grows, while the held-out *frequency* carries irreducible finite-sample multinomial (or sampling) noise at that same count. On a corpus this dense the two scales are orders of magnitude apart, so parameter coverage collapses to ~0.21 / ~0.32 even when the model is well calibrated — it is comparing a frequency against an interval that was never meant to contain it.

- **`hdi_cov` (posterior-predictive coverage, primary).** Fraction of held-out cells whose empirical realization lands inside a *predictive* 94% interval that folds finite-sample noise into the parameter uncertainty at the cell's own event count `n`. For `state_transition` (multinomial): per cell, draw M = 2000 seeded (`seed = 20260714`) posterior-predictive frequency vectors — a per-class parameter draw followed by an exact size-`n` multinomial — and form the per-`(cell, end_class)` predictive interval. For `run_expectancy` (mean): the predictive interval of the held-out cell **mean** of `runs_to_end_of_inning` is the posterior of `re_value` plus the central-limit sampling noise of a mean of `n` draws whose per-event spread is the held-out empirical standard deviation. This is the primary gate number.

**Method note on the parameter layer of the predictive draw.** No per-draw class probabilities are persisted — the published `*_summary` exports carry only per-cell mean / sd / 94% HDI. The predictive simulation therefore *approximates* the parameter posterior per class as an independent Normal(`prob_mean`, `prob_sd`) truncated to `[0, 1]` and renormalized across the cell's classes; an all-zero-uncertainty degenerate cell falls back to the normalized mean. This is an approximation of the true (correlated Dirichlet-like) parameter posterior, adequate here because the finite-sample multinomial layer dominates the predictive width on dense cells. For `run_expectancy` the mean's predictive interval is a Normal-CLT interval for the sample mean; the underlying NB dispersion is *not* persisted in the published summary, so the sampling-noise layer is reconstructed from the held-out empirical standard deviation rather than the NB variance — a deliberate substitution (a Normal predictive around a single NB draw would be wrong, but a Normal-CLT predictive around a mean of `n` draws is well justified and sidesteps the missing dispersion parameter).

**Reading the results.** `state_transition` predictive coverage is 0.9780 (in band; the intervals are slightly conservative) against a parameter coverage of 0.2042, and the predictive form is what confirms the transition surface is calibrated. `run_expectancy` predictive coverage is now 0.9062, inside the 0.88 to 0.96 band, so `predictive_coverage_out_of_band` no longer fires, against a parameter coverage of 0.3861. The run-expectancy improvement came from two fit changes: restricting the fit population to real regular-season events in innings 1 through 8, and switching to a per-state negative-binomial dispersion instead of a single shared one.

Six of the models above also carry the new `weak_id` flag. For `state_transition` the flag traces to a single parameter, the `alpha_trans[1]` intercept, at a bulk ESS of 227 on 4,000 draws and an rhat of 1.03, with zero divergences; this is a mixing shortfall in one weakly identified intercept, not a divergence or geometry problem. For `assist_credit_allocation` and the four observedness models (`ball_handler_position`, `location_depth`, `location_edge`, `trajectory`), the flag comes from group-level bulk ESS below 400 on the 10K-event fit subsample; `location_side_observedness` and `general_location_observedness` clear the threshold and are not flagged. The four `dl_proposal_*` rows read `missing` because the sweep's manifest lookup builds a path the deep-learning artifacts do not actually use; that mismatch is recorded as an open item in `notes/followups.md` rather than fixed here.

## Held-out ECE availability caveat

The multinomial coverage models (E, D, C) write `validation/held_out_metrics.json` with a `distribution_calibration` block only for fits produced after the calibration-gate wiring landed; the number of held-out reliability/ECE fields on any given artifact depends on when it was fit. A multiclass **top-label ECE** for the geometry imputation models is *not* reconstructable read-only from `exports/` + dataset parquet alone: the published `geometry_probabilities.parquet` scores only the geometry-**unobserved** production slice, which carries no ground-truth label, while the observed held-out events that do carry truth have no persisted per-event prediction (verified: the observed-`location_depth` event set and the export event set have **0** overlapping `event_key`s). Producing a per-event top-label ECE would require reloading the posterior and re-running the DL-aware softmax reconstruction over the held-out observed slice, which is neither an `exports`+dataset computation nor cheap/robust. The disposition below therefore reports the held-out calibration that *is* persisted plus a marginal log-loss lift computed offline read-only.

## The multinomial acceptance metric: log-loss, not top-1

An earlier version of this table carried `geometry_location_depth` as a `failed` row, blocked by `bayes_held_out_top1_not_beating_baseline`: held-out top-1 accuracy, 0.5609, did not beat the majority-class baseline, 0.5611. That row is now `passed`, because the metric the multinomial gate blocks on changed, not because the fit did.

The evidence that prompted the change was already in the earlier disposition. On the held-out observed slice (505,921 events) `location_depth`'s predicted marginal class shares match the empirical shares to a total-variation distance of 0.0051 (max per-class absolute deviation 0.0044), and held-out log-loss is 1.0816 nats against a marginal-entropy baseline of 1.1219 — a +0.0403-nat lift, so the per-event probability vectors carry real event-level information beyond a constant marginal predictor. Per-season slice calibration is stable (weighted TV 0.0234, worst-season TV 0.2345 on a thin early-era slice). The earlier reading was that the block was a true statement about arg-max utility rather than a calibration defect, and it was left in place as a scoped warning.

That disposition does not survive contact with what the block actually does. `confidence_status` is a per-row column on the published table (§9), copied from the manifest's `validation_status`; a consumer reading `failed` there reads "this estimate did not clear its gate," not "do not take this distribution's arg-max." The gate blocked publication confidence on a statistic the publication policy explicitly bans from canonical consumption, while the statistic the policy *does* call canonical — the calibrated share vector — was never gated at all.

The rule was also unstable on this family. Under the top-1 rule `geometry_location_side` passed by 0.6976425173100148 against a baseline of 0.6976405407168312, a margin of 2e-6, or roughly one held-out event. Neighbouring dimensions of the same model, fit the same way, landed on opposite sides of a publication gate on sampling noise.

The gate now blocks a multinomial fit when held-out log-loss fails to beat the entropy of the empirical class marginal — the log-loss of the constant-marginal predictor, and the weakest distributional baseline worth clearing. Where the fit does not persist `baseline_log_loss`, it is derived from the `distribution_calibration` block's `empirical_share` field; a payload that is malformed, truncated, or degenerate (a single held-out class, whose entropy is zero) yields no baseline and fires `bayes_held_out_log_loss_ungradeable` rather than passing by default, at the severity the gate itself carries — `block` on a full-scale fit, `warn` on a smoke fit. Top-1 is retained as a `warn`-severity diagnostic. Every published geometry dimension clears the new gate:

The per-dimension numbers below are for the published fits. Since 2026-09-04 the three location dimensions publish the `gamma_dl_zero` flavor, because their production rows carry no deep proposal and a deep covariate fit on observed rows only would have been applied to a NULL covariate. The numbers quoted in the paragraphs above (1.0816 nats for `location_depth`, the 2e-6 top-1 margin for `location_side`) are from the earlier shrunk fits and are kept as the history of the gate change; the location lifts are smaller without the deep covariate but every dimension still clears the marginal-entropy baseline.

| dimension | classes | top-1 | top-1 baseline | log-loss | marginal entropy | lift (nats) | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| `geometry_location_depth` | 4 | 0.5611 | 0.5611 | 1.1113 | 1.1219 | +0.0106 | 505,921 |
| `geometry_location_side` | 6 | 0.6976 | 0.6976 | 1.0306 | 1.0546 | +0.0240 | 505,921 |
| `geometry_location_edge` | 4 | 0.6469 | 0.6472 | 0.9177 | 0.9291 | +0.0114 | 505,921 |
| `geometry_trajectory` | 5 | 0.4920 | 0.4147 | 1.1382 | 1.3529 | +0.2147 | 616,513 |
| `geometry_general_location` | 18 | 0.1856 | 0.1353 | 2.4456 | 2.5303 | +0.0847 | 505,921 |

The ordering the two metrics induce differs, which is the point. `location_side` is the worst dimension on top-1 lift (zero, to six decimals) and the second best on log-loss lift; the arg-max on a dimension whose majority class holds 70% of the mass is uninformative in a way that says nothing about whether the other 30% is distributed correctly.

## Regeneration

```
just validate-gates
```

Read-only. Add `--write` to persist each `validation_report.json` beside its artifact and stamp `validation_status` onto its manifest; pass model names to restrict the sweep. The per-dimension numbers above come from each artifact's `validation/held_out_metrics.json` (`distribution_calibration` + `log_loss`), with the marginal-entropy baseline computed from the block's empirical shares.
