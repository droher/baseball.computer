# MNAR correction redesign — per-class selection offset

Supersedes the refuted learned-`gamma_propensity_class` approach (implementation-review T1.1) and
the anchored per-era offset that followed it (modeling review H2, removed 2026-09-03).
Applies to the imputation softmax models that score an unobserved slice: geometry (E), ball-handler (D).

## The estimand

For an event on the **unobserved** slice (`observed_status != 'observed'`), the deliverable is the
imputed class distribution `P(class | x, R=0)` — the class mix among events whose class was *not*
recorded, conditioned on pre-event covariates `x`.

This is not the same quantity the observed-only fit learns. The fit learns `P(class | x, R=1)` (the
class mix among *recorded* events). Under MNAR these differ, and the gap is exactly what must be
corrected.

## The observation (selection) process

Recording is missing-not-at-random: the probability a class is recorded depends on the class itself.

```
class_i ~ P(class | x_i)                       # latent baseball event
R_i     ~ Bernoulli( s(class_i, x_i) )         # s = P(recorded | class, x), the selection
```

## Why the offset is the right form (and the learned coefficient was not)

Bayes on the selection model gives the two slice distributions:

```
P(c | x, R=1) ∝ P(c|x) · s(c,x)
P(c | x, R=0) ∝ P(c|x) · (1 − s(c,x))
```

So the production estimand relates to the observed-only fit by a **per-class multiplicative reweight**:

```
P(c | x, R=0) ∝ P(c | x, R=1) · (1−s(c,x))/s(c,x)
             =  P(c | x, R=1) · odds_mask(c,x)
```

Write `log odds_mask(c,x) = logit(P(masked|c,x))`. Decompose into a class-independent part (varies
with `x`) plus a per-class deviation `delta_c`. **The class-independent part cancels inside the
softmax**, so only `delta_c` matters:

```
corrected_c = softmax( eta_c + delta_c )      equivalently   shares_c · exp(delta_c) / Σ shares·exp(delta)
```

`delta_c` is a **fixed per-class offset** (the per-class selection log-odds), supplied as data — not a
coefficient on a covariate, and not learned from the observed-only likelihood.

The refuted approach added `gamma_c · z_prop(x)`: a *learned* coefficient on the *marginal* propensity
`z_prop = logit P(observed|x)`. The marginal `P(observed|x)` (all Model A gives) cannot encode
class-dependent selection, and on the masked backtest the learned arm left the masked-slice error
unchanged (relative reduction −0.001). Sign convention, because earlier notes got it backwards:
`z_prop` is the standardized logit of `propensity_p_observed`, so masked events have LOW `z`; a
negative GroundBall `gamma` therefore *raises* GroundBall on the masked slice, the direction a
correction needs. The arm is inert, not wrong-signed.

### Numerical proof of the mechanism, and its limit

On a synthetic replica of the masked-backtest mask (`P(masked)=clip(w_c·intensity,0,.98)`,
`overall_masked_target=0.70`, `focal_depletion_ratio=0.82`), applying the oracle per-class offset
`delta_c = logit(w_c · mean_intensity)` to a perfect observed-only fit recovers the masked-slice
truth: focal-share error 0.0697 → 0.0003, TV 0.070 → 0.009.

That recovery is an algebraic identity when selection depends on class alone: at the marginal level
`P(c | R=0) ∝ P(c | R=1) · odds_mask(c)` holds for any per-event shares, a constant model included,
so reweighting by the realized per-class masked rate reproduces the masked marginal by construction.
It proves the offset is the right *form*; it cannot fail, so it is not a test of anything else.

## Identification in production — where `delta_c` comes from

`delta_c` is **not identified from observed-only data** (that is the whole content of T1.1). It must be
supplied. What is available, honestly:

1. **Sensitivity ribbon (the published deliverable).** Treat `delta_c` as a sensitivity parameter on
   the grid `±{0.25, 0.5, 1.0}` nats, `delta=0` = the current MAR fit. Publish the imputed shares as
   a band across the grid. This quantifies how much the imputed class mix could move under MNAR of
   assumed strength without claiming a point correction. Cheap: a post-hoc reweight of the published
   `noprop` shares, no refit. The band is an assumption, not a data-identified interval.
2. **Derived-slice lower bound (trajectory only).** The unrecorded slice splits into `derived` rows
   (trajectory deduced from the fielding string — every one is GroundBall) and an `unknown_code`
   remainder. Every derived event is an unrecorded event of known class, so
   `P(GB | unrecorded) >= n_derived / n_unrecorded` per era is a hard floor: 0.292 (pre-1950),
   0.339 (1950–1987), 0.400 (1988+). The same rows give a known-truth subslice on which the MAR
   shares can be scored. `mnar_anchor.py` computes both; `sensitivity.py::offset_reaching_share`
   reports the GroundBall offset at which the ribbon's corrected marginal reaches the floor, so a
   reader can see whether the grid contains it (it does: −0.15 / −0.19 / +0.04 nats).

### What the derived slice does not identify (H2)

The earlier design used the derived slice as an *anchor*: `delta_GB = log(p_derived / p_obs)` per era.
Because the derived slice is 100% GroundBall, `p_derived = 1` and the offset was `-ln(p_obs_GB)` — a
function of the observed slice alone (1.241 / 0.917 / 0.835 nats = `-ln(0.289 / 0.400 / 0.434)`).
The "anchored" pre-1950 share of 0.58 was `1/(2 − p_obs)` up to renormalization and carried no
information about the unrecorded slice; the era trend read as scoring practice was `p_obs` rising.
That estimator, the `delta_raw` / centered-offset machinery, the "joint anchored direction" ribbon
mode and the test enshrining the identity were removed.

A point estimate that applies MAR only to the unknown remainder,
`(n_derived + Σ_unknown p_MAR(GB)) / n_unrecorded` (0.519 / 0.579 / 0.641), is also computed and
published as a partial-truth reference, but it is not a correction: deduction pulls every ground
ball with a deducible fielding string into the derived slice, so the remainder is depleted of ground
balls and MAR on it is doubtful in a known direction.

The selection strength is era-structured, so any offset would be per-`(era_cell, class)`; the ribbon
and the bound are both reported per paper era.

## Validation plan

- **Mechanism (masked backtest, oracle delta).** The backtest variant whose "corrected" arm is a
  `noprop` fit scored with the oracle offset clears the gate on the class-only designs by identity
  (see above). Only the `covariate_joint` design, whose selection depends on class × `batter_hand`
  (a covariate the model conditions on), is informative; it shows a partial correction (relative
  reduction 0.537). The recorded runs are `SMOKE_BUDGET=1000` with the convergence gates failing
  (ESS 20–47) and no run artifacts checked in (`notes/paper/tables/mnar_backtest_robustness.md`).
  The learned-form arm stays as the negative control.
- **Sensitivity coherence.** `delta=0` reproduces the published `noprop` shares exactly; the band is
  monotone in `delta`.
- **Bound coherence.** The offset at which the ribbon reaches the derived-slice floor lies inside
  the grid; where MAR sits below the floor (1988+, by 0.008) the floor is the binding statement.
- **MAR on the known-truth subslice.** The published export's mean p(GB) on derived rows (0.320 /
  0.402 / 0.375 against a truth of 1.0; log loss 1.36 / 1.09 / 1.57) is reported as a direct
  miscalibration measure. The export gives derived and unknown rows almost the same p(GB) because
  the fielding string that identifies the derived rows is not a model covariate.

## Build order

1. Scoring-time per-class `selection_offset` on the geometry/ball-handler export path — applied as
   `eta += offset[class]` in the softmax reconstruction (purely an export transform; the fit stays
   observed-only and unchanged). Flavor name `gamma_propensity_offset` to sit beside `_zero` / `_class`. **Built.**
2. Masked-backtest variant: corrected = `noprop` + oracle offset. **Built**; informative only on `covariate_joint`.
3. Sensitivity-ribbon export over the delta grid (post-hoc reweight; no refit). **Built.**
4. Derived-slice bound + MAR-on-derived diagnostic + offset-at-bound (`scripts/mnar_anchor.py`,
   `just sensitivity-ribbon --bound <run-dir>`). **Built**; replaces the anchored point estimate.

## Status

`python_models/statistical/sensitivity.py` reweights a published per-event class-share export over
the selection-log-odds grid (`DEFAULT_GRID = ±{0.25, 0.5, 1.0}` nats, sweeping each class's offset
alone) and returns the per-class marginal band. `scripts/sensitivity_ribbon.py` (no args) writes
`exports/sensitivity_ribbon.parquet` beside each published `geometry_*` fit; `delta=0` reproduces
the published MAR marginal exactly. `--bound <anchor-run-dir>` writes the per-era trajectory ribbon
and `trajectory_bound_offset.parquet` beside the trajectory fit.

The grid scale was checked against the masked backtest (`--validate-backtest <run>`): the
correction the mask induced (oracle per-class offset = centered `logit(class_mask_prob)`) lands inside
`±1.0` — GroundBall `+0.37`, others `−0.09` — and the focal-class band `[0.18, 0.59]` brackets the true
`0.43` share. A `±2`-SD-of-logit scale was rejected as uninformatively wide (`[0.09, 0.72]`). This
calibrates the grid's width against one synthetic selection process; it does not identify the real one.

MAR (`delta=0` = the `e-noprop-*` operating points) stays the published default.

## What this does not claim

It does not recover event-level truth on the unobserved slice, and it does not identify the selection
from the data. It supplies the correct *form* of the correction, an explicitly assumed range
(sensitivity), one hard floor (the derived-slice bound, GroundBall only), and a known-truth subslice
on which the MAR shares are scored. It does not supply an anchored point.
