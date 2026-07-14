## A family of coverage models

The models that fill the record share one statistical contract, and reading them
as a family — rather than as separate fits — is what makes the contract worth
stating. Each model names an estimand on the baseball scale, an observed slice it
trains on, an unobserved slice it scores, and a published table that carries the
posterior forward with provenance. Each is hierarchical and Bayesian, sampled with
the No-U-Turn sampler through numpyro or nutpie, and each partitions games by a
shared hash so that a game's events never straddle a split:
`HASH(game_id) % 100` sends whole games to `TRAIN`, `VALIDATE`, or `TEST`, and
every model reads the same partition. <!-- src: notes/paper/OUTLINE.md --> The
grouping is deliberate: events within a game share a scorer, a park, and a
missingness regime, so an event-level split would leak the very structure the
models are meant to estimate.

Three likelihood families cover the whole set. <!-- src: docs/estimated-models.md -->
An observation dimension that is either recorded or not is a Bernoulli logistic
model,

$$R_i \sim \operatorname{Bernoulli}(p_i), \qquad \operatorname{logit} p_i = \eta_i;$$

a categorical outcome — a batted-ball class, a handler position, a credited
fielder, an end state — is a reference-class softmax over $K$ classes,

$$Y_i \sim \operatorname{Multinomial}(n_i, \pi_i), \qquad \pi_{i,c} = \operatorname{softmax}_c(\eta_{i,c});$$

and a count or rate — park runs, run expectancy — is negative binomial,
$y \sim \operatorname{NB}(\lambda, \phi)$ with $\log \lambda = \eta$. Group effects
enter the linear predictor $\eta$ through non-centered partial pooling —
$\beta = \sigma z$, $z \sim \mathrm{N}(0,1)$, $\sigma \sim \mathrm{HalfNormal}(s)$
— so that a park with three season-games and a park with three thousand both
shrink toward the league mean by an amount the data, not the analyst, decides.
<!-- src: notes/data-coverage-implementation/03-hierarchical-models.md -->

Every softmax model can optionally read a deep-learning proposal as a per-class
covariate $\gamma_c \cdot \log \tilde p^{dl}_{i,c}$ with a shrinkage prior
$\gamma_c \sim \mathrm{N}(0, 0.5)$, and every such model is fit twice — once with
$\gamma_c$ fixed at zero, once with the shrunk prior. The design rule prefers the
flavor whose deep covariate moves the publication-tier effects — for the geometry
softmaxes, the per-class intercepts and fixed-effect interaction tensors, since
scorer, park, and era terms cancel under the softmax and carry no separate
posterior — by less than 0.25 SD on most cells; a larger shift means the deep
signal is doing work the hierarchy should carry itself.
<!-- src: notes/data-coverage-implementation/03-hierarchical-models.md --> Both
fits are stored, and the ablation is part of each model's validation report. The
rule guides the choice but does not bind it: the measured ablation (§6) shows the
trajectory covariate shifts most publication-tier cells past 0.25 SD yet ships
anyway, retained on held-out predictive grounds.
<!-- src: notes/paper/tables/gamma_dl_ablation.md -->

The acceptance gate is the same everywhere and it has two halves that must both
pass. Convergence — split-$\hat R$, bulk and tail effective sample size, and
divergence count — is necessary but not sufficient. A model that mixes cleanly
can still be estimating the wrong quantity, and the sharpest findings below are
exactly cases where the diagnostics were clean and the estimand was wrong. So
every fit is also scored against a held-out predictive metric on the shared
`TEST` games — ROC-AUC or PR-AUC for the Bernoulli and binary-decomposed
softmax models, total variation or log-loss against the held-out class mix for
the multinomials, RMSE for the counts. A fit ships only when it clears both.

### Model A — observation propensity

Model A estimates $P(R_i = 1 \mid x_i)$, the probability that a given batted-ball
dimension was actually observed and recorded for event $i$, and it is the model
the rest of the family leans on to know how selective the record is. It fits one
event-grain Bernoulli per dimension that has both observed and unobserved rows —
`trajectory`, the four location dimensions, and `ball_handler_position`; the
purely derived `pulled_opposite` is 0% observed and excluded.
<!-- src: memory/phase4_obs_propensity_findings.md --> The predictor pools season,
scorer, and park on the logit scale and carries game- and play-context fixed
effects:

$$\operatorname{logit} p_i = \alpha + \beta^{\text{season}}_{t_i} + \beta^{\text{scorer}}_{c_i} + \beta^{\text{park}}_{k_i} + \sum_j X^{(j)}_i \beta^{(j)},$$

with $\alpha \sim \mathrm{N}(0, 1.5)$, $\sigma_{\text{season}}, \sigma_{\text{scorer}} \sim \mathrm{HalfNormal}(1.5)$,
$\sigma_{\text{park}} \sim \mathrm{HalfNormal}(1.0)$, and each fixed-effect block
$\beta^{(j)} \sim \mathrm{ZeroSumNormal}(0, 1)$ over its full level set so no
reference level is dropped. <!-- src: notes/data-coverage-implementation/03-hierarchical-models.md -->
A source-family random effect is declared only when the training slice holds more
than one source; the production population is single-source by construction, so
the term would be unidentified and is omitted.
<!-- src: notes/data-coverage-implementation/03-hierarchical-models.md -->

Two operating decisions came out of a sample-size sweep on the trajectory
dimension and both generalize across dimensions. First, the fit runs on 10,000
rows per dimension. Held-out ROC-AUC has plateaued by 10K — trajectory moves only
from 0.910 to 0.912 as rows go from 10K to 1M — while mixing collapses past 500K,
with $\hat R$ climbing from 1.015 to 1.789 and minimum ESS falling from 269 to 6
at a million rows. <!-- src: memory/phase4_obs_propensity_findings.md --> More data
buys nothing the estimand needs and eventually breaks the sampler; 10K is the
budget. Second, seasons whose rare class is nearly saturated — fewer than 100
rare-class events, or a rare-class share below 0.5% — are dropped, because a
near-constant response has almost no logistic gradient and ridges with the season
effect, and downstream consumers read a missing propensity as $p = 1$ anyway.
<!-- src: memory/phase4_obs_propensity_findings.md --> On the shipped, held-out
path — Model A gained a proper holdout split only in a later fix wave — the six
retrained dimensions clear ROC-AUC from 0.898 on trajectory to 0.972–0.973 on the
four location dimensions, with `ball_handler_position` at 0.942.
<!-- src: notes/data-coverage-implementation/implementation-review.md --> A single
context feature carries `ball_handler_position`: adding the 13-level
plate-appearance-outcome fixed effect lifts its held-out PR-AUC from 0.569 to
0.755, while leaving the other five dimensions — already at a 0.97-plus PR-AUC
ceiling — untouched. <!-- src: memory/phase4_obs_propensity_findings.md -->

### Models E and D — geometry and ball handler

Model E imputes batted-ball geometry — trajectory, location side, depth, edge,
and the fielding region — for events whose class was never recorded, and Model D
imputes the handling fielder position. Both are per-event reference-class
softmaxes over their class sets, with a per-class intercept and a per-class
fixed-effect interaction on each context column (result family, start base-out
state, alignment regime, batter hand):

$$\pi_{i,c} = \operatorname{softmax}_c\!\left(\alpha_c + \sum_j \beta^{(j)}_{c}[x_i] + \gamma_c \log \tilde p^{dl}_{i,c}\right).$$

<!-- src: docs/estimated-models.md --> Model E's four deep-backed dimensions
publish the shrunk deep flavor; `general_location` publishes the deep-free fit.
<!-- src: docs/estimated-models.md --> Model D publishes here for the first time: its
held-out top-1 accuracy is 0.220 against a position-prior baseline of 0.171 —
real lift, but a reminder that the handler is genuinely hard to pin from pre-event
state, which is why the full distribution ships and the argmax never becomes a
fact. <!-- src: notes/data-coverage-implementation/implementation-review.md -->

Both models train on the observed-only slice and score the unobserved slice, and
both publish under a missing-at-random flavor — the `noprop` operating points —
because the correction for the fact that recording is correlated with the class
being recorded turned out to be unidentifiable from a marginal propensity.
That is the subject of the next section; the geometry and handler surfaces here
are the MAR points those sensitivity ribbons are built around.

### Model C — fielding credit

Model C allocates official credit — putouts and assists — over the
personnel-eligible fielders on events where the record left it unattributed. The
shipped putout arm is a dual-likelihood softmax: a supervised
$Y_e \sim \operatorname{Multinomial}(U_e, \pi_e)$ on well-attributed events, where
$U_e$ is the known unattributed-putout count, sharing its $\pi_e$ with an
aggregate arm $T_m \sim \operatorname{Normal}(\sum_e U_e \pi_{e,k}, \sigma_{\text{box}})$
that anchors a synthetically masked subset to box-cell totals.
<!-- src: docs/estimated-models.md --> The supervised arm is load-bearing: without
a per-event observed outcome the per-event effects cancel (see the closing
vignette), and it is the arm that lets scorers and eras shift the per-position
distribution.

Assists are harder and the result is instructive. Cut one folds assist allocation
and a `P(any assist)` decision into a single $K = 10$ softmax with a `NONE`
sentinel class, marginalizing the unknown putout position over the published
putout posterior at scoring time. <!-- src: notes/data-coverage-implementation/03-hierarchical-models.md -->
The fit is clean — $\hat R_{\max}$ 1.036, minimum bulk ESS 185, 0 divergences —
and on the held-out set the any-assist decision clears its baseline convincingly,
PR-AUC 0.516 against a 0.348 base rate, while per-fielder identification sits
essentially at the majority-class baseline, top-1 0.664 against 0.652.
<!-- src: notes/data-coverage-implementation/03-hierarchical-models.md --> The value
that survives is the part that does not depend on the missing clue — how many
assists, and their calibrated shares — not the part that names the specific
fielder. The multi-assist count itself is a separate cell-grain model, a
collapsed Dirichlet-multinomial over $M \in \{1,2,3,4\}$ per
`(result_family, base_state, outs)` cell; it converges at $\hat R$ 1.009 with 3,969
ESS and 0 divergences and cuts held-out total variation to the count mix by 33%.
<!-- src: notes/followups.md -->

### Model F — park factors

Model F estimates the multiplicative run effect of a park in a season-league, net
of the teams that played there. It is a team-game negative binomial with a
sum-to-zero park effect centered within each season-league — centering globally
would let era-level scoring leak into the park term — and an AR(1) persistence
prior across a park's consecutive seasons so a factor is pulled toward its own
recent history rather than fit independently cell by cell:

$$\theta^{\text{raw}}_{p,t} = \rho\, \theta^{\text{raw}}_{p,t-1} + \varepsilon_t, \quad \varepsilon_t \sim \mathrm{N}(0, \sigma_{\text{innov}}), \quad \rho \sim \mathrm{Beta}(2,1),$$

with $\theta^{\text{raw}}_{p,1} \sim \mathrm{N}(0, \sigma_{\text{init}})$ and
$\theta_{\text{park}}$ the group-centered raw effect.
<!-- src: docs/estimated-models.md --> The $\mathrm{Beta}(2,1)$ prior places more
mass near 1 than near 0, encoding the expectation that park effects are sticky
year to year absent a structural change. The full-history fit needed a
prior-predictive fix before it would sample — a
$\mathrm{Normal}(0, 1.5)$ season-league intercept against the $\log$-exposure
offset implied roughly 38 runs a game and overflowed the negative-binomial
forward sampler, and was replaced by an intercept centered on the empirical
log rate with tightened deviation scales.
<!-- src: memory/t2_spec_completions_landed.md -->

### Model G — run expectancy and the transition matrix

Model G carries two submodels. The run-expectancy arm is a cell-grain negative
binomial whose mean pools from a global level through a per-state level to the
cell, and it adds an `era_regime` level between them — a multi-hot indicator over
`pre_DH`, `DH_AL_only`, `full_DH`, `ghost_runner`, and the extra-inning overlap,
pruned to full rank. <!-- src: memory/t2_spec_completions_landed.md --> The regime
level is a correctness fix, not an ornament: the 2020 extra-inning ghost runner
and the designated hitter materially move run expectancy, so a table that pools
1901 through 2025 into one mean is wrong rather than merely coarse. The full fit
over 16.3M events converges at $\hat R$ 1.009 with 0 divergences, its era effects
are correctly signed with `DH_AL_only` the largest at +0.043 log, and its
held-out RMSE is 8% better than a state-only baseline.
<!-- src: memory/t2_spec_completions_landed.md -->

The transition arm is where a clean diagnostic failure taught a lesson worth its
own telling. The submodel is a Markov transition over base-out states,
$P(\text{end} \mid \text{start}, t, l, \text{regime}) = \operatorname{softmax}(\zeta)$,
across the 24 base-out states plus a half-inning-ending sentinel — 25 end classes.
The natural specification treats all $24 \times 25$ start-end pairs as reachable,
pins a single global reference class, and pools transition logits across the
corpus. It does not converge: $\hat R$ walls out at 4.04 with an effective sample
size around 5, and the chains sit in different modes.
<!-- src: memory/hierarchical_multinomial_structural_zeros.md --> The instinct is
to blame the sampler and reparameterize, and reparameterizing does nothing,
because the failure is specification, not geometry. Outs never decrease within an
event, so more than half of the start-end pairs are structurally impossible; their
free softmax logits have no data and are driven to $-\infty$ against the prior,
creating a likelihood floor that fights the hierarchy. The pinned global reference
— bases empty, no outs — is itself unreachable from any start with an out already
recorded, which removes the only anchor for those states' softmax and leaves the
level unidentified. <!-- src: memory/hierarchical_multinomial_structural_zeros.md -->

The fix is to make the model respect the domain's hard constraint. Reachable end
classes for a start state are those at out counts $\geq$ the start's outs, plus
inning-end; the impossible cells are pinned to a large negative logit and carry no
free parameter; each start state pins its own modal reachable class as the
reference; and the corpus pooling level is dropped, since start states with
disjoint supports and different references cannot meaningfully pool. The predictive
softmax is built in float64 so the masked $\exp(-30)$ entries stay representable
for the multinomial sampler. <!-- src: memory/hierarchical_multinomial_structural_zeros.md -->
The redesigned fit clears the strict gate at full scale — $\hat R_{\max}$ 1.016,
minimum bulk ESS 188, 0 divergences — and its held-out total variation to the
transition mix falls from 0.070 to 0.044, a 37% reduction.
<!-- src: notes/followups.md --> No amount of $\hat R$ inspection on the original
fit would have suggested the remedy; the reachability structure had to be read off
the baseball, and this is the concrete face of the paper's stance that convergence
is necessary but never sufficient.

### Model J — pitch coverage and summary

Model J estimates whether a plate appearance's final ball-strike count was recorded
and, where it was, the distribution over final counts. The coverage arm is an
event-grain Bernoulli in the same shape as Model A, reused against the pitch-count
population — about half of events lack an observed count — and it clears its
smoke gate at held-out ROC-AUC 0.995, though it is fit only at smoke scale and
not yet published: its table `pitch_count_coverage` materializes a typed zero-row
frame pending an artifact pointer (§7). <!-- src: docs/estimated-models.md --> The
summary arm is a cell-grain multinomial over the 12 `balls × strikes` final-count
classes per result family, season, and league, with a centered reference-class
softmax pinned at `b0_s0`. <!-- src: docs/estimated-models.md --> The model recovers
structural constraints it was never told — every strikeout ends on two strikes —
which is the reassurance a coverage model should provide before its imputations on
the unrecorded slice are trusted.

### A scalar random effect under softmax is worth nothing

One identification rule runs through every event-level softmax in the family, and
it began as a bug shared across four models. A season, scorer, or park random
effect summed into a single scalar per event and broadcast equally across the
class logits contributes exactly nothing to the prediction, because
$\operatorname{softmax}(x + c\,\mathbf 1) = \operatorname{softmax}(x)$ — an equal
shift on every class cancels in the normalization. <!-- src: notes/data-coverage-implementation/implementation-review.md -->
These terms were sampled anyway across the geometry, ball-handler, credit, and
responsibility builders — wasted compute, and a flat posterior direction on the
corresponding $\sigma$ hyperparameters that invites funnels and divergences.
Removing them and re-checking each fit at a matched seed moved held-out top-1 and
log-loss by at most 0.0015 and 0.00004 across four models, left zero divergences,
and improved convergence in three of the four; the effect the specification
wanted was per-class all along. <!-- src: notes/data-coverage-implementation/implementation-review.md -->
