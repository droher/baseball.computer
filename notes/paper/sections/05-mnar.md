## Selection that never recorded itself

The September 4 legacy geometry models learned
$P(c\mid x,R_i=1)$ from events whose class was recorded and applied that
distribution to events with $R_i=0$. Equality of the two conditional
distributions is a missing-at-random assumption. The acquired record cannot test
it directly because the class is absent on the target rows. Moreover, the chance
that a scorer records trajectory plausibly depends on event salience, source
practice, and the contact label itself. High held-out accuracy within the recorded
slice therefore does not establish accuracy on naturally unrecorded events.

Some unrecorded trajectory rows have fielding descriptions from which the project
rules derive a broad contact class. For example, certain shortstop-to-first plays
are classified as ground balls under the deterministic deduction rules. These are
rule-conditioned derivations, not independent physical measurements and not proof
that every superficially similar play was a ground ball. In the September 4
ledger, derived-ground rows numbered 763,993 of 2,615,579 unrecorded pre-1950
events, 1,057,776 of 3,117,160 in 1950--1987, and 53,922 of 134,970 from 1988
on. The counts show that the derivation is consequential; because eligibility for
deduction depends on the recorded fielding description, they do not by themselves
identify the class mix of the remaining unknown rows.
<!-- src: tables/groundball_mnar.md -->

### The selection model

Let a latent class be drawn from $P(c\mid x_i)$ and then recorded with probability
$s(c_i,x_i)$:

$$c_i\sim P(c\mid x_i), \qquad
R_i\sim\operatorname{Bernoulli}\{s(c_i,x_i)\}.$$

Both observed and unobserved slices are products of that selection process:

$$P(c\mid x,R_i=1)\propto P(c\mid x)s(c,x),$$

$$P(c\mid x,R_i=0)\propto P(c\mid x)\{1-s(c,x)\}.$$

For the classes under consideration, assume $0<s(c,x)<1$, so both selection
outcomes have positive support. Consequently,

$$P(c\mid x,R_i=0)\propto P(c\mid x,R_i=1)
  \frac{1-s(c,x)}{s(c,x)}.$$

Suppose, as a sensitivity assumption, that the nonselection-to-selection log
odds separate as

$$\log\frac{1-s(c,x)}{s(c,x)}=a(x)+\delta_c.$$

Here $a(x)$ is class independent and $\delta_c$ is a class-specific constant. The
class-independent component adds the same quantity to every class logit and
cancels under softmax. The unrecorded distribution can then be represented as

$$P(c\mid x,R_i=0)=
\operatorname{softmax}_c(\eta_{i,c}+\delta_c).$$

This is an assumed sensitivity form. Selection that interacts between class and
covariates cannot generally be reduced to one constant offset per class.
<!-- src: notes/data-coverage-implementation/mnar-selection-offset-design.md -->

Neither $P(c\mid x,R_i=1)$ nor the marginal propensity $P(R_i=1\mid x)$
identifies $\delta_c$. The observed slice was selected too; it identifies the
conditional distribution among selected rows, while the missing labels prevent a
direct estimate of how class composition changes when $R_i=0$. A former design
placed a learned class coefficient on Model A's marginal observation propensity.
The September 4 masked experiment reported essentially no reduction in its focal
share error. That result is consistent with the identification problem: a marginal
propensity cannot recover class-dependent selection without additional truth or a
specified selection model. Its old smoke artifact is not retained, so the quoted
coefficient and sampler diagnostics are historical notes rather than reproducible
current evidence.
<!-- src: notes/data-coverage-implementation/implementation-review.md -->
<!-- src: docs/modeling-evidence-contract.md -->

### What the derived slice supports

The previous paper revision set
$\delta_{\mathrm{GroundBall}}=\log(p_{\mathrm{derived}}/p_{\mathrm{obs}})$.
Because every row admitted by that derivation was labelled GroundBall,
$p_{\mathrm{derived}}=1$, so the supposed anchor reduced to
$-\log p_{\mathrm{obs}}$. It was a function of the observed slice and supplied no
new measurement of the unknown rows. The offsets and corrected shares derived from
that construction are withdrawn.
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->

If the deduction rule is accepted as correct on its admitted rows, it supplies a
conditional lower bound:

$$P(\mathrm{GroundBall}\mid R_i=0)
\geq\frac{n_{\mathrm{derived\ ground}}}{n_{\mathrm{unrecorded}}}.$$

The September 4 ledger gives floors of 0.292 before 1950, 0.339 in 1950--1987,
and 0.400 from 1988 onward. These are floors conditional on the rule's validity
and the ledger's population definition. They do not prove MNAR, identify a point
share, or validate the derivation on rows without independent labels. No analogous
deduction supplies a floor for fly balls, line drives, pop-ups, or bunts.
<!-- src: tables/groundball_mnar.md -->

The same derived rows can be used as a diagnostic subset, again conditional on the
deduction. The September 4 MAR export assigned mean GroundBall probabilities of
0.320, 0.402, and 0.375 across the three era groups to rows labelled GroundBall by
the rule. The gap is evidence that the model did not encode the deduction signal;
it is not an unbiased estimate of error on all naturally missing rows, because the
diagnostic subset is selected by the availability and content of fielding detail.

### Sensitivity rather than identification

The defensible use of $\delta_c$ is an assumption grid. The September 4 analysis
reweighted event probabilities as one class offset swept
$\pm\{0.25,0.5,1.0\}$ nats with other offsets fixed at zero. This operation is
a closed-form sensitivity calculation, not a fitted correction. Its width was
chosen from one synthetic masking exercise and does not bound the real historical
selection mechanism.

The old robustness table used 50 draws, 50 tuning steps, and two chains. All four
fits failed their convergence threshold, and the underlying run artifacts are not
retained. Three masking designs also made oracle marginal reweighting an algebraic
identity. The remaining covariate-interaction design reported that a marginal
offset removed about half the induced focal-share error. That is a historical
mechanism check under a constructed mask, not validation of historical MNAR
correction.
<!-- src: tables/mnar_backtest_robustness.md -->

### Current coverage policy

The legacy MAR geometry pointers are explicitly exploratory and fail gate version
3. In particular, transport and identification are unsupported. The September 13
full-history PBP candidate addresses a different objective: it supplies normalized
empirical distributions and broader declared fallbacks so every applicable target
on the acquired PBP spine has an estimate. It does not estimate $\delta_c$ or
claim that missingness is at random.

Those completed rows must therefore be interpreted as assumption-labelled rough
reconstructions. Their method, fallback, source status, and uncertainty fields are
part of the estimand. Conservation and complete row coverage show that the outputs
cohere with their declared rules; they do not establish the unobserved historical
class. No sealed confirmation labels were opened or rescored for this completion
work.
<!-- src: artifacts/imputation/legacy-publication-candidate-v1/migration_summary.json -->
<!-- src: docs/pbp-imputation.md -->
<!-- src: notes/full-history-imputation-plan.md -->
