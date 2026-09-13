## What the record cannot tell you

Complete output coverage and statistical identification are different goals. The
September 13 candidate supplies a reproducible estimate or declared fallback for
applicable targets on the acquired PBP spine. That policy does not make an
unobserved historical fact learnable from a single source. Three legacy model
letters illustrate three different limits.

### Model B — contact-label confusion

Model B proposed a latent contact class $Z_i$ and a scorer or era confusion
matrix $\Omega$ generating the recorded label $L_i$:

$$Z_i\sim P(Z\mid x_i), \qquad L_i\sim\Omega_{g_i}[Z_i,\cdot].$$

Estimating $\Omega$ requires repeated independent labels for the same event or a
second measurement that can disagree with $L_i$ without being constructed from
it. The acquired PBP surface usually provides one recorded contact label. The
project's deduced broad class is not a general second measurement: on many
recorded rows its rule uses or reproduces the recorded class, while on other rows
it depends on fielding outcomes that are themselves selectively recorded.

The September 4 audit found only a thin outcome-anchored subset with information
independent enough to challenge the recorded label. That subset can move a simple
aggregate confusion rate, but the retained evidence does not support a
scorer-by-era matrix or historical transport. It is therefore more precise to say
that this record is uninformative for the proposed structured $\Omega$ than to
claim a formal non-identification theorem or that scorer confusion is rare.
Unblocking the estimand requires an independently generated second contact label
at event grain, with its own provenance and observation model.
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->

### Model I — analytical fielder responsibility

Fielder responsibility asks who should have had a play given geometry and
alignment:

$$P(\text{responsible}=k\mid\text{geometry},\text{alignment},\ldots).$$

That differs from the handling fielder, who touched or retrieved the ball. Training
on `ball_handler_position` would simply reproduce Model D with a narrower
vocabulary and would be especially misleading on hits, where the retriever need
not be the defender with the opportunity.

A defensible model would need a zone or positioning kernel such as
$\pi(k\mid\text{location},\text{alignment})$. The current source does not
contain tracking-era defensive positions or an independent opportunity label from
which to estimate that kernel. This is a data-availability limit, not a claim that
responsibility is unidentifiable in principle. The September 13 fielding candidate
can allocate official residual credits when eligibility and integer box capacity
agree, but official credit reconciliation does not identify analytical
responsibility.
<!-- src: notes/data-coverage-implementation/responsibility-zone-design.md -->
<!-- src: docs/pbp-imputation.md -->

### Model K — shift propensity

Model K was scoped as

$$P(\text{shift}\mid\text{player},\text{batter hand},
\text{defending team},\text{count},\text{outs},\text{base state}).$$

It was designed but not fit. No retained result supports a claim about its
identification or accuracy. Geometry and responsibility work that would consume
it instead uses coarse alignment regimes or broader fallbacks with explicit
method labels. K is unfinished research scope, rather than an identified failure
of the source.

### Publication consequence

These cases share a reporting rule, not a statistical proof. A structured
confusion model without an independent label, a responsibility model without a
positioning source, and an unbuilt shift model cannot become validated facts
through a complete SQL table. A rough fallback may still be useful for an
authorized coverage interface if it is labelled as an assumption, retains its
method and provenance, and is kept separate from recorded facts. It must not be
described as if the historical record identified the latent quantity.
