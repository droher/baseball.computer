# MNAR correction redesign — per-class selection offset

Supersedes the refuted learned-`gamma_propensity_class` approach (implementation-review T1.1).
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

The pre-1988 ground-ball signature is the textbook case: observed ground share ~34% vs partial-true
~68%, and 78–95% of pre-1988 events are on the unobserved slice (implementation-review T1.1).

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
`z_prop = logit P(observed|x)`. On observed-only data, low `p_observed` correlates with *less* focal
class (survivor tilt — the mirror of the truth on the masked slice), so the likelihood learns
`gamma_c` with the wrong sign. The marginal `P(observed|x)` (all Model A gives) cannot encode
class-dependent selection. The offset sidesteps this: it is the per-class selection itself, not a
regression on an endogenous proxy.

### Numerical proof of the mechanism

On a synthetic replica of the masked-backtest mask (`P(masked)=clip(w_c·intensity,0,.98)`,
`overall_masked_target=0.70`, `focal_depletion_ratio=0.82`), applying the oracle per-class offset
`delta_c = logit(w_c · mean_intensity)` to a perfect observed-only fit recovers the masked-slice
truth: focal-share error 0.0697 → 0.0003 (relative reduction 0.996, gate ≥ 0.25), TV 0.070 → 0.009.
The refuted learned form scored relative reduction −0.001 on the real backtest.

## Identification in production — where `delta_c` comes from

`delta_c` is **not identified from observed-only data** (that is the whole content of T1.1). It must be
supplied. Two honest sources, in increasing ambition:

1. **Sensitivity ribbon (default deliverable).** Treat `delta_c` as a sensitivity parameter on the
   canonical grid (`02-eda-and-modeling-datasets.md`: `{-2,-1,-0.5,0,+0.5,+1,+2}` SD of latent class
   log-prob), `delta=0` = the current MAR fit. Publish the imputed shares as a band across the grid.
   This quantifies how much the imputed class mix could move under MNAR of plausible strength without
   claiming a point correction. Cheap: a post-hoc reweight of the published `noprop` shares, no refit.
2. **Anchored point estimate (extension).** Where an external partial-truth class share exists for a
   calibration slice (deduced-layer / box-derived ground-ball shares, or a plausibly-MAR modern era
   that transports via covariates), solve for the `delta_c` (per era × class) that reconciles the
   observed-slice share with the anchor. Publish the anchored point *with* the sensitivity band, and
   state the transportability assumption explicitly.

The selection strength is era-structured (strong pre-1988, ~0 after), so production `delta_c` is
per-`(era_cell, class)`, with `delta ≡ 0` outside the under-recorded eras.

## Validation plan

- **Mechanism (masked backtest, oracle delta).** Add a backtest variant whose "corrected" arm is a
  `noprop` fit scored with the oracle offset derived from the known `w_class`. It must clear the
  existing gate: focal-share relative reduction ≥ 0.25, TV reduced, held-out non-regression, clean
  diagnostics. This proves the offset form works where the learned form failed. (The learned-form
  arm stays as the negative control.)
- **Sensitivity coherence.** `delta=0` reproduces the published `noprop` shares exactly; the band is
  monotone in `delta`; the focal share moves toward the partial-truth as `delta` grows.
- **Anchored estimate (when built).** Hold out the anchor slice; check the anchored `delta_c`
  generalizes to a disjoint anchor slice (no overfitting to one calibration cell).

## Build order

1. Scoring-time per-class `selection_offset` on the geometry/ball-handler export path — applied as
   `eta += offset[class]` in the softmax reconstruction (purely an export transform; the fit stays
   observed-only and unchanged). Flavor name `gamma_propensity_offset` to sit beside `_zero` / `_class`.
2. Masked-backtest variant: corrected = `noprop` + oracle offset; assert it clears the gate.
3. Sensitivity-ribbon export over the delta grid (post-hoc reweight; no refit). **Built.**
4. (Deferred) anchored per-`(era, class)` delta from partial-truth + its holdout validation.

## Status — ribbon built (steps 1–3 done)

`python_models/statistical/sensitivity.py` reweights a published per-event class-share export over
the selection-log-odds grid (`DEFAULT_GRID = ±{0.25, 0.5, 1.0}` nats, sweeping each class's offset
alone) and returns the per-class marginal band. `scripts/sensitivity_ribbon.py` (no args) writes
`exports/sensitivity_ribbon.parquet` beside each published `geometry_*` fit (grain
`(geometry_dimension, class_label, delta_logodds)` plus `marginal_share` / `baseline_share`);
`delta=0` reproduces the published MAR marginal exactly.

The grid scale is validated against the masked backtest (`--validate-backtest <run>`): the *real*
correction the mask induced (oracle per-class offset = centered `logit(class_mask_prob)`) lands
inside `±1.0` — GroundBall `+0.37`, others `−0.09` — reduces TV-to-truth (`0.073 → 0.033`), and the
focal-class band `[0.18, 0.59]` brackets the true `0.43` share. So the published band is wide enough
to contain a realistic MNAR shift without being uninformatively wide. A `±2`-SD-of-logit scale was
rejected: it produced absurd `[0.09, 0.72]` bands because the one-vs-rest logit SD is large.

The ribbon is the honest deliverable for the unidentified `delta_c`: an as-published band, not a
point claim. The anchored per-`(era, class)` point estimate (step 4) is still deferred; MAR
(`delta=0` = the `e-noprop-*` operating points) stays the published default.

## What this does not claim

It does not recover event-level truth on the unobserved slice, and it does not identify the selection
from the data. It supplies the correct *form* of the correction and an honest range (sensitivity) or
an explicitly-assumption-anchored point. MAR (`delta=0`) remains the default published operating point
until an anchor is built and validated.
