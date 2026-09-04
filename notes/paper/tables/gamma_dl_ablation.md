gamma_dl (DL-covariate) ablation status for every Bayes model dimension documented as consuming a deep-learning proposal, cross-referenced against `artifacts/statistical/bayes/*/*/manifest.json` and `artifacts/statistical/published/*.json` on branch `paper-revision` (checked 2026-07-13). Prepared in response to referee major comment 4 / minor comment 6, which cites "the ablation 'moves posteriors < 0.25 SD'" as one of the paper's unverifiable inferential claims.

**Headline finding: the zero/shrunk artifact pair the design doc requires (`notes/data-coverage-implementation/03-hierarchical-models.md` §"Deep-Proposal Ablation Policy", `04-deep-learning-supplements.md`, `06-rollout-and-validation.md` §"Cross-Family Acceptance Gate") does not exist on disk for any of the four DL-consuming dimensions.** Every stored artifact for `geometry_trajectory`, `geometry_location_side`, `geometry_location_depth`, and `geometry_location_edge` — across every generation (`e-cut1-*`, `e-cut2b-trajectory-shrunk`, `e-prop-*`, `e-noprop-*`, `e-v12-noprop-*`) — carries `gamma_dl_flavor = "gamma_dl_shrunk"`. No `*-zero` counterpart was ever fit and stored for these four dimensions, despite `bayes/targets/geometry.py` declaring `default_flavors=("gamma_dl_zero", "gamma_dl_shrunk")` for exactly these targets and the CLI (`bc/python_models/statistical/cli.py fit-bayes --flavor`) defaulting to `gamma_dl_zero`. Consequently the `>0.25 SD` publication-tier switch rule was never evaluated — there is no artifact anywhere under `artifacts/statistical/` containing a computed RE-posterior-shift-in-SD number (confirmed by an exhaustive `grep -rn "0.25 SD"` across the artifact tree, zero hits).

**Update 2026-09-04: the published pointers changed.** The three location dimensions now publish the `gamma_dl_zero` flavor (`e-v12-noprop-location_side-zero`, `e-v12-noprop-location_depth-zero`, `e-v12-noprop-location_edge-zero`), because their production rows carry no deep proposal: the location deep specs score observed rows only, so a deep covariate fit on the observed slice would have been applied to a NULL on every imputed row. Trajectory still publishes `e-v12-noprop-trajectory-shrunk`, and `general_location` publishes `e-v12-noprop-general_location-zero` as before. The shift-diagnostic numbers in this file are unchanged; the "Published flavor" column of the ablation-pairs table and the "No published pointer was changed" sentence in the verdict describe the pointers as they stood on 2026-07-14.

**Update 2026-07-14: the four missing `gamma_dl_zero` fits have now been run and the RE-shift diagnostic computed — see "2026-07-14: gamma_dl_zero counterfactual results" at the end of this file. The trajectory dimension FAILS the "<0.25 SD" claim (70% of publication-tier cells shift >0.25 SD). The tables and interpretation below are preserved as the pre-fit record.**

## Ablation pairs

| Model / dimension | Published flavor | Held-out metric (`gamma_dl_zero`) | Held-out metric (`gamma_dl_shrunk`) | Delta | RE posterior shift (SD) | Artifact IDs (zero / shrunk) |
|---|---|---|---|---|---|---|
| Model E — `geometry_trajectory` | `gamma_dl_shrunk` (published `e-v12-noprop-trajectory-shrunk`) | not found in artifacts/manifests — no `gamma_dl_zero` artifact was ever fit for this target | top-1 0.4920, log-loss 1.1382, macro PR-AUC 0.4623, n=616,513 (baseline top-1 0.4147) | not computable — no zero-flavor counterpart exists | not recorded — no artifact anywhere records this diagnostic (see headline finding) | zero: **none** / shrunk: `e-v12-noprop-trajectory-shrunk` (`artifacts/statistical/bayes/geometry_trajectory/e-v12-noprop-trajectory-shrunk/`) |
| Model E — `geometry_location_side` | `gamma_dl_shrunk` (published `e-v12-noprop-location_side-shrunk`) | not found in artifacts/manifests | top-1 0.6976, log-loss 0.9863, macro PR-AUC 0.2188, n=505,921 (baseline top-1 0.6976 — model sits at the majority-class baseline on top-1; the deliverable is the calibrated distribution, not argmax) | not computable | not recorded | zero: **none** / shrunk: `e-v12-noprop-location_side-shrunk` |
| Model E — `geometry_location_depth` | `gamma_dl_shrunk` (published `e-v12-noprop-location_depth-shrunk`) | not found in artifacts/manifests | top-1 0.5609, log-loss 1.0816, macro PR-AUC 0.3233, n=505,921 (baseline top-1 0.5611 — again at/below baseline on top-1) | not computable | not recorded | zero: **none** / shrunk: `e-v12-noprop-location_depth-shrunk` |
| Model E — `geometry_location_edge` | `gamma_dl_shrunk` (published `e-v12-noprop-location_edge-shrunk`) | not found in artifacts/manifests | top-1 0.6541, log-loss 0.8979, macro PR-AUC 0.3208, n=505,921 (baseline top-1 0.6472) | not computable | not recorded | zero: **none** / shrunk: `e-v12-noprop-location_edge-shrunk` |
| Model E — `geometry_general_location` | `gamma_dl_zero` (structural — never had a DL proposal) | top-1 0.1856, log-loss 2.4456, macro PR-AUC 0.0882, n=505,921 (baseline top-1 0.1353) | not applicable — no DL artifact was ever produced for this dimension (`dl_proposal_dimension=None` in the spec) | n/a | n/a | zero: `e-v12-noprop-general_location-zero` / shrunk: **does not exist, by design** |
| Model D — `ball_handler_imputation` | `gamma_dl_zero` (`gamma_dl_flavor: null` in the published manifest) | top-1 0.2202, log-loss 1.9431, macro PR-AUC 0.1715, n=1,069,690 (baseline top-1 0.1709) | not applicable — the `batted_to_fielder_class` DL proposal is documented as deferred (fails the 1% production-coverage floor on the handler-unobserved slice) | n/a | n/a | Model D carries the `gamma_dl` hook in code but never activates it; `d-noprop-10k-v2` is the only relevant artifact |

Supplementary signal actually available in place of the missing zero-flavor comparison — the `gamma_dl` posterior coefficient *within* the shrunk fit (prior `Normal(0, 0.5)`), which measures how far the DL covariate's fitted weight sits from 0, not how much a DL-free refit would move the random effects:

| dimension | operating point | gamma_dl mean | gamma_dl sd | mean / sd |
|---|---|---:|---:|---:|
| trajectory | `e-v12-noprop-trajectory-shrunk` | 1.157 | 0.032 | 36.2 |
| trajectory (earlier `e-cut1`) | `e-cut1-trajectory-shrunk` | 1.394 | 0.038 | 36.7 |
| location_side | `e-v12-noprop-location_side-shrunk` | 0.836 | 0.043 | 19.4 |
| location_depth | `e-v12-noprop-location_depth-shrunk` | 0.899 | 0.042 | 21.4 |
| location_edge | `e-v12-noprop-location_edge-shrunk` | 0.890 | 0.042 | 21.2 |

These are all far from 0 in posterior-SD terms — the DL logit is a strongly weighted covariate wherever it's active — but a tight, non-zero coefficient on the DL term is not the same measurement as "including DL shifts the publication-tier scorer/park/era random-effect posteriors by more than 0.25 SD," which is the quantity the switch rule and the paper's "< 0.25 SD" claim actually require. That quantity needs the missing zero-flavor sibling fit; it cannot be derived from the shrunk fit alone.

## Missing pairs

All four DL-consuming dimensions are missing pairs, for the same reason:

- `geometry_trajectory`, `geometry_location_side`, `geometry_location_depth`, `geometry_location_edge` — `bayes/targets/geometry.py` declares `default_flavors=("gamma_dl_zero", "gamma_dl_shrunk")` and the fit CLI's `--flavor` default is `gamma_dl_zero`, so the tooling supports fitting both. In practice, every operator invocation across every generation recorded in `notes/data-coverage-implementation/implementation-review.md` / `implementation-checklist.md` explicitly passed `--flavor gamma_dl_shrunk` (or the shrunk flavor was the only one ever run), and the `gamma_dl_zero` counterpart was never executed. `implementation-checklist.md` §"Gamma_dl Ablation" still shows both checklist items unchecked: `- [ ] Fit gamma_dl_zero flavor of handler and geometry models.` The switch-rule criterion (`>0.25 SD` shift on publication-tier REs) could therefore never have been computed for these four dimensions; the shrunk flavor appears to have been published by default/assumption, not by the documented empirical procedure. Why the zero counterpart was skipped is not stated anywhere I could find in the notes — I could not determine the reason and am not guessing at one.

No other model in the {A, B, C, F, G, H, I, J, K} set carries an active gamma_dl covariate to ablate:
- Model A (`scorer_observation_propensities`, 6 dims) — DL covariate dropped from v1 per `03-hierarchical-models.md` line 101 ("The DL covariate has been dropped from v1; revisit only if posterior-predictive calibration shows residual gaps.").
- Model C (`putout_credit_allocation`, `assist_credit_allocation`, `assist_count`) — fielding-credit DL allocation is explicitly out of scope (`bc/python_models/statistical/CLAUDE.md`: "Fielding-credit DL allocation is out of scope. Spatial-allocation task that doesn't benefit from shared player embeddings.").
- Model D (`ball_handler_imputation`) — the `batted_to_fielder_class` DL proposal is deferred; `gamma_dl_flavor: null` on the published manifest confirms it is inactive, not merely unpublished.
- Model H (`imputed_advancement_probabilities`) — carries `gamma_dl_zero` only, `dl_active=False`, and the table ships zero rows in production regardless (Model H has no published fit at all yet, per `docs/estimated-models.md`).
- Models B, F, G, I, J, K — no `dl_proposal_dimension` registered; not applicable by design (B and I are blocked/unidentifiable, K is unbuilt, F/G/J have no DL proposal source in the current spec set).

## Regeneration

Not run as part of this task (read-only). To produce the missing `gamma_dl_zero` counterpart for, e.g., `geometry_trajectory` against the same frozen dataset snapshot the published shrunk fit used (`e-v12`):

```
just fit-bayes geometry_trajectory e-v12 e-v12-noprop-trajectory-zero --flavor gamma_dl_zero
just validate-artifact e-v12-noprop-trajectory-zero --model geometry_trajectory
```

Repeat with `geometry_location_side` / `geometry_location_depth` / `geometry_location_edge` against the same `e-v12` dataset artifact and matching `-zero` artifact ids. Each `fit-bayes` invocation resolves to:

```
python -m python_models.statistical.cli fit-bayes --model <model> --dataset-artifact e-v12 \
  --artifact-id <artifact-id> --flavor gamma_dl_zero
```

(`bc/python_models/statistical/cli.py`, `fit_bayes` subparser, `--flavor` choices `{gamma_dl_zero, gamma_dl_shrunk}`, default `gamma_dl_zero`). This does not by itself compute the `>0.25 SD` switch-rule diagnostic — that comparison (publication-tier random-effect posterior shift, in SD, between the zero and shrunk fits) is not implemented as an automated check anywhere in `bc/python_models/statistical/validate.py` or `bayes/training.py` as of this branch; it would need to be written (e.g., diffing `posterior_summary.rows` for the season/scorer/park random-effect variables between the two artifacts) before the switch rule described in the design docs could actually run.

## Interpretation

The paper's "<0.25 SD" ablation claim is not currently verifiable from the artifact store, because the fit it depends on — the `gamma_dl_zero` counterpart for each of the four DL-consuming geometry dimensions — was never executed or stored; only the shrunk flavor was ever published. What *is* available is the `gamma_dl` posterior coefficient inside each shrunk fit, which sits 19–37 posterior SDs from zero (trajectory 1.16±0.03, location_side 0.84±0.04, location_depth 0.90±0.04, location_edge 0.89±0.04) — strong evidence the model assigns real, precisely estimated weight to the DL logit, but not evidence about how much a DL-free refit's *random effects* would move, which is the quantity the referee is actually asking about. Separately, the one genuine zero-vs-nonzero A/B that does exist in the artifacts — the `gamma_propensity` (MNAR-offset) ablation, a different covariate axis fit as `e-prop-*` vs `e-noprop-*` pairs — shows the same pattern of a largely inert covariate: trajectory moves top-1/log-loss by 0.0001/0.002, location_edge by 0.0009/0.002, and only location_depth moves meaningfully (top-1 0.5705→0.5608, log-loss 1.0704→1.0818), a shift the implementation review attributes to in-sample leakage rather than genuine signal. If the DL covariate behaves similarly to how the propensity covariate behaved once actually ablated, the honest expectation is that the pretrain's reported "~6×" embedding-signal lift (a permutation-importance number on frozen embeddings, per `notes/data-coverage-implementation/implementation-review.md` and the referee's own characterization in M4) would produce a comparably small movement in the published posteriors — but this is an inference from an analogous ablation, not a demonstrated result, because the actual gamma_dl zero/shrunk comparison the paper claims was never run. The direct, honest answer to the referee is: the published estimates cannot currently be shown to have moved by any specific amount because of the DL covariate, in either direction, because the counterfactual fit needed to measure that movement does not exist — the paper should either run the four missing `gamma_dl_zero` fits and report the real SD shift, or retract the "<0.25 SD" claim and reframe the 6× pretrain lift as an unconnected pipeline diagnostic, exactly as referee comment M4 requests.

## 2026-07-14: gamma_dl_zero counterfactual results

The four missing `gamma_dl_zero` fits were run on 2026-07-14 (branch `paper-revision`), each against the same frozen `e-v12` dataset artifact and the same operating point as its published shrunk sibling: 9,988 training events (10K operating point), nutpie backend, 1000 draws x 1000 tune x 4 chains, `target_accept=0.95`, `max_treedepth=12`, seed 20260513, `gamma_propensity_zero` — every knob identical except `--flavor gamma_dl_zero` (verified against each pair's `manifest.json`). Fit logs: `logs/gamma_dl_zero/*.log`. No published pointer under `artifacts/statistical/published/` was touched; the zero fits are counterfactual diagnostics only.

### Fit diagnostics (zero fits)

| dimension | artifact | rhat_max | ess_bulk_min | ess_tail_min | divergences |
|---|---|---:|---:|---:|---:|
| trajectory | `e-v12-noprop-trajectory-zero` | 1.0042 | 2764 | 2439 | 0 |
| location_side | `e-v12-noprop-location_side-zero` | 1.0036 | 1387 | 2177 | 0 |
| location_depth | `e-v12-noprop-location_depth-zero` | 1.0035 | 2253 | 2457 | 0 |
| location_edge | `e-v12-noprop-location_edge-zero` | 1.0032 | 2065 | 2089 | 0 |

All four converged cleanly (the shrunk siblings sit at rhat_max 1.0036–1.0038, same ballpark).

### Held-out metric deltas (zero vs published shrunk, from `held_out_metrics.json`)

| dimension | top-1 zero | top-1 shrunk | Δ top-1 | log-loss zero | log-loss shrunk | Δ log-loss | macro PR-AUC zero | macro PR-AUC shrunk | baseline top-1 | n |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| trajectory | 0.4518 | 0.4920 | +0.0402 | 1.2327 | 1.1382 | -0.0945 | 0.3484 | 0.4623 | 0.4147 | 616,513 |
| location_side | 0.6976 | 0.6976 | 0.0000 | 1.0306 | 0.9863 | -0.0443 | 0.1888 | 0.2188 | 0.6976 | 505,921 |
| location_depth | 0.5611 | 0.5609 | -0.0002 | 1.1113 | 1.0816 | -0.0297 | 0.2825 | 0.3233 | 0.5611 | 505,921 |
| location_edge | 0.6469 | 0.6541 | +0.0072 | 0.9177 | 0.8979 | -0.0198 | 0.2846 | 0.3208 | 0.6472 | 505,921 |

Deltas are shrunk-minus-zero for top-1 and shrunk-minus-zero for log-loss (negative = shrunk better). The shrunk flavor is better on log-loss and macro PR-AUC for all four dimensions; trajectory is the only dimension where the DL covariate also moves top-1 materially (+4.0 points). Unlike the gamma_propensity ablation, the DL covariate is NOT inert.

### Publication-tier posterior shift (the ">0.25 SD" switch-rule quantity)

Computed by `scripts/gamma_dl_shift.py` / `bc/python_models/statistical/gamma_dl_shift.py`; per-pair JSON stored at `artifacts/statistical/bayes/geometry_*/e-v12-noprop-*-zero/validation/gamma_dl_shift.json`. One definitional note: after the canceling-RE removal, the geometry model carries NO season/scorer/park random-effect blocks — scalar-per-event terms enter every class logit equally and cancel inside the per-event softmax (`models/geometry.py`), so the paper's "scorer/park/era REs" do not exist in this model. The publication-tier effect blocks that survive and drive the published imputation distribution are the per-class intercept `alpha_class` and the per-fixed-effect interaction tensors `delta_result_family`, `delta_base_state_start`, `delta_outs_start`, `delta_alignment_regime`, `delta_batter_hand`. The shift statistic is per-cell `|mean_shrunk - mean_zero| / pooled posterior SD` (pooled SD = sqrt of the mean of the two variances).

| dimension | cells | mean shift (SD) | max shift (SD) | share >0.25 SD | most cells >0.25 SD? |
|---|---:|---:|---:|---:|---|
| trajectory | 115 | 0.735 | 3.27 | **0.696** | **YES — FLAG** |
| location_side | 138 | 0.343 | 3.22 | 0.217 | no |
| location_depth | 92 | 0.204 | 0.97 | 0.272 | no |
| location_edge | 92 | 0.295 | 1.58 | 0.391 | no |

Per-block detail for the flagged dimension (trajectory): `alpha_class` 5/5 cells >0.25 SD (mean 1.27 SD), `delta_alignment_regime` 17/20 (mean 1.33), `delta_base_state_start` 33/40 (mean 0.73), `delta_outs_start` 11/15 (mean 0.79), `delta_result_family` 12/25 (mean 0.35), `delta_batter_hand` 2/10 (mean 0.18). For the three location dimensions the bulk of cells sit under the threshold, but not uniformly: location_side concentrates its shift almost entirely in `alpha_class` (5/6 over) and `delta_alignment_regime` (23/24 over, mean 1.47 SD) while every other block is essentially unmoved (<0.09 SD mean).

### Verdict

**`geometry_trajectory` FAILS the paper's "<0.25 SD" claim: 69.6% of publication-tier effect cells shift by more than 0.25 posterior SD (mean 0.74 SD, max 3.27 SD) between the gamma_dl_zero and gamma_dl_shrunk fits. The claim as written cannot stand for this dimension.** The other three dimensions pass the most-cells rule (22%, 27%, 39% of cells over threshold), so the "<0.25 SD on most cells" characterization holds for them, though none is the near-null the propensity ablation produced, and location_edge at 39% is close to the line.

What this means for the published flavor: the >0.25-SD-on-most-cells trigger fires for trajectory, but it does not argue for switching the pointer to the zero flavor — the shrunk fit is strictly better on every held-out metric (log-loss -0.094, top-1 +4.0 points, macro PR-AUC +0.114 on 616K held-out events), so the DL covariate is carrying real signal, not distortion, and the published `e-v12-noprop-trajectory-shrunk` remains the better model. The casualty is the paper's claim, not the pointer: the text asserts the DL covariate "moves posteriors < 0.25 SD" and that is now demonstrated to be false for trajectory. The paper should (a) replace the "<0.25 SD" blanket claim with the measured per-dimension numbers above, (b) state that the DL logit materially reshapes the trajectory model's publication-tier effects and is retained because it wins on held-out prediction, and (c) keep the "<0.25 SD on most cells" characterization only for the three location dimensions. **No published pointer was changed as part of this analysis** (`artifacts/statistical/published/*.json` all still reference the `-shrunk` artifacts).
