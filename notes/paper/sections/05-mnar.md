## Selection that never recorded itself

The imputation models of the previous section share a hidden assumption, and it is
the assumption most likely to be wrong exactly where the models do the most work.
Each trains on the recorded slice, $P(c \mid x, R_i = 1)$, and scores the
unrecorded slice, $P(c \mid x, R_i = 0)$. If recording were unrelated to the class
being recorded, those two distributions would agree and training on one to predict
the other would be sound. In the early record there is direct evidence that they do
not. The unrecorded trajectory slice is not uniformly unknown: for a large part of it
the fielding string a scorer did write identifies a ground ball unambiguously — a
ball fielded by the shortstop and thrown to first — even though the trajectory field
is empty. Those `derived` rows are 763,993 of the 2,615,579 unrecorded pre-1950
events, 1,057,776 of 3,117,160 in 1950–1987, and 53,922 of 134,970 from 1988 on,
and every one of them is a ground ball <!-- src: tables/groundball_mnar.md -->. The
scorer omitted the trajectory on a routine grounder often enough that the omitted
events the record can still classify outnumber the whole recorded slice before 1950
(712,469 events). It is decisive rather than marginal because the unrecorded slice is
78–95% of all pre-1988 events across the geometry dimensions, against under 8% from
1988 on. <!-- src: notes/data-coverage-implementation/implementation-review.md -->

### What the correction has to be

Write the observation as a selection process: a latent class is drawn,
$\text{class}_i \sim P(c \mid x_i)$, and then it is recorded with a probability
that depends on the class, $R_i \sim \operatorname{Bernoulli}(s(c_i, x_i))$ with
$s \in (0,1]$. Bayes on this process gives the two slices in terms of the same
latent distribution,

$$P(c \mid x, R_i=1) \propto P(c \mid x)\, s(c,x), \qquad P(c \mid x, R_i=0) \propto P(c \mid x)\,\bigl(1 - s(c,x)\bigr),$$

so the estimand is a per-class reweight of the fit,

$$P(c \mid x, R_i=0) \propto P(c \mid x, R_i=1) \cdot \frac{1 - s(c,x)}{s(c,x)}.$$

Take logs. The assumption the rest of the section rests on is that the masking
log-odds is additively separable — a class-independent part that varies with $x$
plus a per-class deviation $\delta_c$ that does not. That is an assumption about
the selection process, not a consequence of the algebra: selection that depends on
class × covariate jointly is not of this form, and the backtest below measures what
the offset loses when it is violated. Under separability the class-independent part
is an equal shift on every logit, and by the same softmax invariance that killed the
scalar random effects — $\operatorname{softmax}(\eta + c\mathbf{1}) =
\operatorname{softmax}(\eta)$ — it cancels in the normalization. What remains is a
per-class offset applied to the fitted logits,

$$P(c \mid x, R_i=0) = \operatorname{softmax}_c(\eta_{i,c} + \delta_c),$$

with $\delta_c$ a natural-log log-odds, in nats, the unit every offset and grid in
this section uses.
<!-- src: notes/data-coverage-implementation/mnar-selection-offset-design.md --> The
correction is a fixed per-class number supplied as data — the per-class selection
log-odds — not a coefficient to be learned from the observed slice. And it cannot
be learned from that slice, because the observed slice is by definition the slice
where selection did not act.

### A clean fit of an inert quantity

The design this replaced tried to learn the correction. It added a per-class
coefficient $\gamma_c$ on the marginal observation propensity from Model A — the one
signal available at every event — and fit it on the observed data. A masked
backtest, which hides a realistic scorer-and-era pattern of pre-1988 events and asks
the model to recover the held-out class mix, showed the term does nothing: the
relative reduction in the masked-slice focal-share error was $-0.001$ against a
required 0.25. <!-- src: notes/data-coverage-implementation/implementation-review.md -->
The fitted $\gamma_{\text{GroundBall}}$ was $-0.083 \pm 0.026$, and under the code's
convention that is the direction a correction would need, not the wrong sign: $z$
is the standardized logit of $P(R_i = 1 \mid x_i)$, so masked events sit at low $z$,
and a negative coefficient on $z$ raises the ground-ball logit exactly there.
<!-- src: notes/data-coverage-implementation/mnar-selection-offset-design.md --> An
earlier draft of this paper read the coefficient as wrong-signed; that reading was
an error in the sign convention, and the run's `metrics.json` is not checked into
the repository, so the coefficient itself cannot be re-checked without a rerun. The
correct reading is that a marginal propensity cannot encode class-dependent
selection, so no coefficient on it can identify the reweight, whatever its sign.
The load-bearing detail is what the diagnostics did during all of this. The fit
converged — zero divergences, $\hat R$ and effective sample size within gate — and
the held-out non-regression check passed.
<!-- src: notes/data-coverage-implementation/implementation-review.md --> A clean
trace certifies that the sampler explored the posterior of the model it was given;
it says nothing about whether that model targets the right quantity. This is the
paper's thesis in one experiment: convergence is necessary and not sufficient, and
only a held-out check built to know the truth showed the term was inert.

### What the derived slice does and does not identify

The previous revision of this paper used the derived slice as an anchor: per era it
set $\delta_{\text{GroundBall}} = \log(p_{\text{derived}} / p_{\text{obs}})$, the
log-ratio of the derived-slice ground share to the observed-slice ground share, and
reported offsets of $+1.241$, $+0.917$, and $+0.835$ nats for the three eras and a
pre-1950 unrecorded ground-ball share of 0.58 at the full anchor. Those numbers are
withdrawn. The derived slice is entirely GroundBall, so $p_{\text{derived}} = 1$ and
the "anchor" was $-\ln p_{\text{obs},\text{GroundBall}}$ — $-\ln 0.289$, $-\ln 0.400$,
$-\ln 0.434$ exactly — a function of the observed slice alone that carries no
information about the unrecorded slice; the 0.58 was $1/(2 - p_{\text{obs}})$ up to
renormalization, and the era trend the earlier draft read as scoring practice filling
in was $p_{\text{obs}}$ rising.
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md --> <!-- src: tables/trajectory_mnar_bound.md -->

What the derived slice does support is a bound and a diagnostic. Every derived event
is an unrecorded event whose class is known, so per era

$$P(\text{GroundBall} \mid R_i = 0) \;\geq\; \frac{n_{\text{derived}}}{n_{\text{unrecorded}}},$$

a hard floor of 0.292 before 1950, 0.339 in 1950–1987, and 0.400 from 1988 on
<!-- src: tables/groundball_mnar.md -->. The floor is weak by construction — it
counts only the ground balls a fielding string can name and says nothing about the
remaining `unknown_code` rows — and it binds from below only. The same rows are a
subslice of the unrecorded population on which the truth is known, and the published
missing-at-random export can be scored against it: the export's mean ground-ball
probability on the derived rows is 0.320 before 1950, 0.402 in 1950–1987, and
0.375 from 1988 on, against a truth of 1.0, with log loss 1.36, 1.09, and 1.57
<!-- src: tables/groundball_mnar.md -->. The export gives derived and unknown rows
almost the same probability (0.3203 against 0.3204 pre-1950) because the fielding
string that identifies the derived rows is not a model covariate; the diagnostic
measures how far the MAR shares sit from truth on the one unrecorded subslice where
truth exists. A partial-truth point estimate that takes truth on the derived rows and
MAR on the remainder reads 0.519, 0.579, and 0.641, but it is not a correction:
deduction pulls every ground ball with a deducible fielding string into the derived
slice, so the remainder is depleted of ground balls and MAR on it is doubtful in a
known direction <!-- src: tables/groundball_mnar.md -->.

### An assumed ribbon against a hard floor

The sensitivity object is therefore a marginal grid, not a data-anchored direction.
The published imputation shares stay at the missing-at-random point, $\delta = 0$;
beside them the ribbon reports the class mix as each class's offset sweeps
$\pm\{0.25, 0.5, 1.0\}$ nats with the other offsets held at zero, computed by the
per-event closed-form renormalization so it is a post-hoc reweight of the published
shares and costs no refit <!-- src: tables/trajectory_mnar_bound.md -->. The grid's
width was checked once against a synthetic mask — the correction that mask induced
landed inside $\pm 1.0$, and a $\pm 2$-SD-of-logit scale was rejected as
uninformatively wide — which calibrates the width against one synthetic selection
process and identifies nothing about the real one
<!-- src: notes/data-coverage-implementation/mnar-selection-offset-design.md -->. The
band is an assumption, published as one.

The floor is what the ribbon must respect, and the offset at which the corrected
ground-ball share reaches it is reported per era. Before 1988 the MAR share already
sits above the floor: the share could fall by 0.15 nats (pre-1950) or 0.19 nats
(1950–1987) before violating it. From 1988 on the MAR share, 0.392, sits 0.008 below
the floor of 0.400 and needs $+0.037$ nats to reach it, so the MAR default is
inconsistent with the record there by a small margin and the floor is the binding
statement <!-- src: tables/trajectory_mnar_bound.md -->. Across the $\pm 1.0$ grid
the pre-1950 unrecorded ground-ball share runs from 0.16 to 0.53; the floor sits
inside that band in every era, near its center, which is the honest summary of what
the record says — the band is where the share could be, the floor is the one point
it cannot be below.

### How far the correction reaches

The fixed-offset mechanism was run against four masking designs in the backtest
harness, each corrected with its own oracle offset, and reading that table needs two
caveats stated before any number. First, every run is a smoke-budget fit —
50 draws × 50 tune × 2 chains — whose convergence gates fail on all four designs
(bulk ESS 20–47 against a floor of 100), and no run artifacts are checked into the
repository, so the table is a check of the mechanism, not a publication-grade result
<!-- src: tables/mnar_backtest_robustness.md -->. Second, three of the four designs
cannot fail by construction. When selection depends on class alone, or on nothing,
$P(c \mid R=0) \propto P(c \mid R=1)\,\text{odds}_{\text{mask}}(c)$ holds at the
marginal level for any per-event shares, a constant model included, so reweighting
by the realized per-class masked rate reproduces the masked marginal as an algebraic
identity. The near-exact recoveries on the class-intensity and era-graded designs
(relative error reduction 0.995 and 0.958) and the near-no-op on the class-independent
scorer-blocked design (0.015) are properties of those designs, not evidence about the
offset <!-- src: tables/mnar_backtest_robustness.md -->.

Only the covariate-joint design is informative. There selection depends on class and
batter handedness jointly, a covariate the geometry model conditions on, so the
additive-separability assumption above is genuinely violated; the marginal per-class
offset cuts the focal-share error from 0.137 to 0.063, a relative reduction of
0.537 <!-- src: tables/mnar_backtest_robustness.md -->. That is the one measured
statement about the mechanism's reach: when selection is separable the offset is the
right form, and when it interacts with a conditioned covariate the offset recovers
about half the bias. Rerunning the four designs at the default sampler budget, with
artifacts checked in, is open work (§11).

### What the record still will not say

The offset $\delta_c$ is unidentified from the observed slice for every class. The
derived slice buys a floor for one class in one dimension and a known-truth subslice
on which the MAR fit can be scored; it does not supply a point, and for fly balls,
line drives, pop-ups, and bunts there is no analogue of the fielding-string deduction
at all. Events stamped `unknown_code` or `missing` are recovered by neither the
recorded label nor the deduction, so they sit outside the floor's reach. The shape of
the treatment is what it was — the record supports a per-event propensity to observe,
a posterior over what was observed, and an assumed sensitivity band over what was
not — with one hard constraint added to the band and one estimator removed from it.
MAR remains the published default, the ribbon publishes beside it, and the floor is
the only number in this section the unrecorded slice itself vouches for.
