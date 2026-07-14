## Selection that never recorded itself

The imputation models of the previous section share a hidden assumption, and it is
the assumption most likely to be wrong exactly where the models do the most work.
Each trains on the recorded slice, $P(c \mid x, R_i = 1)$, and scores the
unrecorded slice, $P(c \mid x, R_i = 0)$. If recording were unrelated to the class
being recorded, those two distributions would agree and training on one to predict
the other would be sound. In the early record they do not: §7's `groundball_mnar`
table measures the pre-1988 gap directly — the observed ground-ball share sits near
half the deduced share that folds in what scorers left unrecorded — the textbook
missing-not-at-random signature. It is decisive rather than marginal because the
unrecorded slice is 78–95% of all pre-1988 events across the geometry dimensions,
against under 8% from 1988 on. <!-- src: notes/data-coverage-implementation/implementation-review.md -->
Uncorrected, the models under-impute ground balls by tens of points in precisely
the era where nearly everything is imputed.

### What the correction has to be

Write the observation as a selection process: a latent class is drawn,
$\text{class}_i \sim P(c \mid x_i)$, and then it is recorded with a probability
that depends on the class, $R_i \sim \operatorname{Bernoulli}(s(c_i, x_i))$ with
$s \in (0,1]$. Bayes on this process gives the two slices in terms of the same
latent distribution,

$$P(c \mid x, R_i=1) \propto P(c \mid x)\, s(c,x), \qquad P(c \mid x, R_i=0) \propto P(c \mid x)\,\bigl(1 - s(c,x)\bigr),$$

so the estimand is a per-class reweight of the fit,

$$P(c \mid x, R_i=0) \propto P(c \mid x, R_i=1) \cdot \frac{1 - s(c,x)}{s(c,x)}.$$

Take logs and split the masking log-odds into a class-independent part that varies
with $x$ and a per-class deviation $\delta_c$. The class-independent part is an
equal shift on every logit, and by the same softmax invariance that killed the
scalar random effects — $\operatorname{softmax}(\eta + c\mathbf{1}) =
\operatorname{softmax}(\eta)$ — it cancels in the normalization. What remains is a
per-class offset applied to the fitted logits,

$$P(c \mid x, R_i=0) = \operatorname{softmax}_c(\eta_{i,c} + \delta_c).$$

<!-- src: notes/data-coverage-implementation/mnar-selection-offset-design.md --> The
correction is a fixed per-class number supplied as data — the per-class selection
log-odds — not a coefficient to be learned from the observed slice. And it cannot
be learned from that slice, because the observed slice is by definition the slice
where selection did not act.

### A clean fit of the wrong quantity

The design this replaced tried to learn the correction. It added a per-class
coefficient $\gamma_c$ on the marginal observation propensity from Model A — the one
signal available at every event — and fit it on the observed data. A masked
backtest, which hides a realistic scorer-and-era pattern of pre-1988 events and asks
the model to recover the held-out class mix, refuted it.
<!-- src: memory/mnar_learned_gamma_propensity_refuted.md --> Under class-dependent
masking, the focal class is removed preferentially from heavily masked contexts, so
among the events that survive into the observed slice a low propensity correlates
with *less* focal class — a survivor tilt that is the mirror image of the truth on
the masked slice, where the focal class is enriched. The likelihood learns the tilt
faithfully and extrapolates it in the wrong direction. In the backtest,
$\gamma_{\text{GroundBall}}$ came back at $-0.083 \pm 0.026$ — the wrong sign — and
the correction barely moved the recovered ground share, a relative error reduction
of $-0.001$ against a required 0.25. <!-- src: notes/data-coverage-implementation/implementation-review.md -->
The load-bearing detail is what the diagnostics did during all of this. The fit
converged — zero divergences, $\hat R$ and effective sample size within gate — and
the held-out non-regression check passed. <!-- src: notes/data-coverage-implementation/implementation-review.md -->
A clean trace certifies that the sampler explored the posterior of the model it was
given; it says nothing about whether that model targets the right quantity. A
marginal propensity cannot encode class-dependent selection, so no coefficient on it
identifies the reweight, and the sampler's good behavior is exactly what makes the
error dangerous. This is the paper's thesis in one experiment: convergence is
necessary and not sufficient, and only a held-out check built to know the truth
caught the wrong estimand.

### An anchor the record already contains

The offset is unidentified from observed-only data, but the record is not a single
slice. For a fraction of events the trajectory was never written as a label yet is
recoverable from the fielding string a scorer did write — a ball fielded by the
shortstop and thrown to first is a ground ball whether or not anyone recorded the
trajectory field. That deduced slice is a second view of the same era's batted
balls, produced by a different part of the record, and contrasting its class mix
against the observed slice yields an offset anchored to data rather than assumed.
Per era $e$, the anchor is $\delta_{\text{GroundBall}}(e) = \log\!\bigl(p_{\text{masked}} / p_{\text{obs}}\bigr)$,
the log-ratio of the derived-slice ground share to the observed-slice ground share.
<!-- src: notes/data-coverage-implementation/mnar-selection-offset-design.md -->

Deduction recovers *only* ground balls — the fielding string identifies a grounder
unambiguously but does not distinguish a fly from a line drive from a pop-up — so
the derived slice is entirely GroundBall and the anchor is one-dimensional. That is
a property of the record, not a modeling choice: the offset is a single-class
softmax direction with every other class offset held at zero. Because softmax
offsets are identified only up to an additive constant, the raw single-class offset
is a valid joint direction, and it is used directly rather than the degenerate
zero-mean centering. The anchored magnitudes fall where the coverage story predicts:
the observed GroundBall share is 0.289 pre-1950, 0.400 in 1950–1987, and 0.434 from
1988 on — measured on the five-class trajectory vocabulary, where bunts are their own
class; the broad ground/air binary in the results table pools bunts back in and reads
0.337 pre-1950 — giving raw offsets of $+1.241$, $+0.917$, and $+0.835$ nats — the
under-recording is worst in the earliest era and shrinks monotonically as scoring
practice fills in. <!-- src: notes/paper/tables/joint_ribbon_trajectory.md -->
The anchor is a partial-truth estimate: it characterizes selection for the one class
deduction can see, and events whose status is `unknown_code` or `missing` rather than
`derived` remain outside it.

### A joint ribbon, not a marginal grid

With a data-anchored direction in hand the sensitivity object becomes a sweep along
it rather than an unmoored grid. The published imputation shares stay at the
missing-at-random point, $\delta = 0$; beside them the ribbon reports the class mix
at $t \cdot \delta_{\text{anchor}}$ for $t \in \{0, 0.25, \dots, 1.5\}$, with the
per-event closed-form renormalization so it is a post-hoc reweight of the published
shares and costs no refit. <!-- src: notes/paper/tables/joint_ribbon_trajectory.md -->
Because a real MNAR shift moves the whole simplex at once, the sweep moves every
class jointly along the anchor, and at the full-anchor point it perturbs each
non-focal class offset by $\pm 0.25$ nats one at a time to trace a band around the
joint point. This is the referee's joint deliverable in place of the earlier
one-class-at-a-time grid, whose width was calibrated against a single simulation.

The headline reads directly off the sweep. On the pre-1950 unrecorded slice the MAR
default puts the GroundBall share at 0.320; the full anchor raises it to 0.582, with
a perturbation band of $[0.563, 0.598]$. <!-- src: notes/paper/tables/joint_ribbon_trajectory.md -->
The MAR default understates pre-1950 ground balls by nearly half, and the ribbon
states that as a graded surface across $t$ rather than a point — 0.448 at
half-anchor, 0.582 at full anchor, 0.705 past it — so a reader sees how the class mix
responds to selection of increasing strength without the paper claiming to know the
one true strength. MAR remains the published default; the ribbon publishes beside it.

### How far the correction reaches

The fixed-offset mechanism was validated against not one masking design but four,
each imposing a qualitatively different selection process and each corrected with its
own oracle offset, so the backtest measures where the per-class form holds and where
it breaks. <!-- src: notes/paper/tables/mnar_backtest_robustness.md --> When
selection is a pure function of class — the form the correction assumes — the oracle
offset removes essentially all of the bias: relative error reduction 0.995 on the
original class-intensity design and 0.958 when the same class-marginal selection is
graded by era, an era the geometry model does not condition on. When selection
depends on class *and* a covariate the model does condition on — batter handedness —
the marginal offset is genuinely misspecified and cuts the error only about in half,
0.537. When selection depends on a whole-game block and is independent of class, the
per-class offset comes out flat and the reweight is a near-no-op, relative reduction
0.015 — correctly so, because class-independent selection is already close to MAR
for the marginal shares and needs no correction. The correction's scope is therefore
stated by construction: exact when selection is class-only, partial when class ×
covariate, inapplicable to block absence.

These off-form designs meet the referee's circularity charge head on. The concern is
that supplying the oracle offset implied by an injected mask merely inverts the mask,
proving nothing about a real selection process. The three robustness designs were
built *after* the correction and *against* its assumptions — the covariate-joint and
scorer-blocked masks were constructed specifically to violate the per-class form the
offset presumes — and the correction degrades on exactly the designs that break its
assumption while holding on those that respect it. A mechanism that inverted any mix
would not degrade selectively; this one does.

### What the record still will not say

The anchor buys one class in one dimension. The per-class offsets for the non-ground
classes — whether the observed slice over- or under-represents fly balls, line
drives, and pop-ups relative to their unrecorded truth — have no analogue of the
fielding-string deduction and remain unidentified; the ribbon moves them only through
the softmax coupling to the anchored GroundBall direction and the $\pm 0.25$-nat
perturbation, not from data. The residual slice compounds this: events stamped
`unknown_code` or `missing` are recovered by neither the recorded label nor the
deduction, so they sit outside both the anchor and its partial-truth guarantee. The
shape of the whole treatment is unchanged by the anchor — the record supports a
per-event propensity to observe, a posterior over what was observed, and a bounded
sensitivity surface over what was not. The anchor sharpens the surface for the one
class the record can corroborate; it does not turn the sensitivity ribbon into a
point estimate, and for the classes and slices it cannot reach, MAR remains the
honest default.
