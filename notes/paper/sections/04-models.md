## A family of coverage models

The models that fill the record share one statistical contract, and reading them
as a family — rather than as separate fits — is what makes the contract worth
stating. Each model names an estimand on the baseball scale, an observed slice it
trains on, an unobserved slice it scores, and a published table that carries the
posterior forward with provenance. Each is hierarchical and Bayesian, sampled with
the No-U-Turn sampler through numpyro or nutpie, and each holds out whole games
rather than events: every Bayes fit, and every coverage gate in §7, removes fold 0
of a ten-fold BLAKE2s hash of `game_id` before any subsampling and scores against
it. <!-- src: bc/python_models/statistical/splits.py --> <!-- src: bc/python_models/statistical/models/_event_data.py -->
The deep supplements of §6 use a different partition — the `HASH(game_id) % 100`
split into `TRAIN` (70%), `VALIDATE` (15%), and `TEST` (15%) that the modeling
datasets carry as `primary_fold` <!-- src: bc/models/intermediate/modeling_datasets/model_input_event_universe.sql -->.
The two hashes are unrelated and the partitions are not nested; an earlier draft
of this paper described the deep split as the one every model reads, which was
wrong, and §6 states the consequence. The game grouping is deliberate in both:
events within a game share a scorer, a park, and a missingness regime, so an
event-level split would leak the very structure the models are meant to estimate.

| Letter | Estimand | Published table | Tier | Status |
|---|---|---|---|---|
| A | P(dimension recorded) per event | `scorer_observation_propensities` | estimated | populated |
| B | scorer contact-label confusion Ω | — | withheld | data uninformative (§8) |
| C | fielding credit share per position | `imputed_fielding_credit`, `assist_count_distribution` | estimated | populated |
| D | handling fielder position | `imputed_ball_handler_probabilities` | estimated | populated |
| E | batted-ball geometry class | `imputed_batted_ball_geometry` | estimated | populated |
| F | park run effect per season-league | `park_factor_summary` | estimated | populated |
| G | run expectancy; base-out transition; run values | `run_expectancy_summary`, `state_transition_summary`, `linear_weights_estimated` | estimated | populated |
| H | runner advancement class | `imputed_advancement_probabilities` | estimated | schema only, zero rows (§11) |
| I | fielder responsibility | — | withheld | input absent from source (§8) |
| J | final-count recorded; final-count distribution | `pitch_count_coverage`, `pitch_summary_distribution` | estimated | coverage arm zero rows; summary populated |
| K | shift propensity | — | withheld | designed, not built (§8) |

<!-- src: docs/estimated-models.md --> <!-- src: bc/python_models/statistical/publication_tiers.py -->
Twelve tables, ten populated; seven letters published, three withheld, one
deferred. That is the counting the rest of the paper uses.

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

A softmax model that has a deep proposal (§6) can read it as a covariate
$\gamma \log \tilde p^{dl}_{i,c}$: one scalar $\gamma$ shared across classes, with
a $\mathrm{N}(0, 0.5)$ prior. The posterior standard deviation of $\gamma$ in the
published geometry fits is 0.03–0.04, so the prior contributes under 1% of the
posterior precision and is inert; the flavor name `gamma_dl_shrunk` is a label for
"deep term active," not a description of a mechanism.
<!-- src: tables/gamma_dl_ablation.md --> <!-- src: bc/python_models/statistical/models/geometry.py -->
Each such model is fit twice, with $\gamma$ fixed at zero and free, and both
artifacts are stored. The design rule was to prefer the flavor whose deep covariate
moves the publication-tier effects — the per-class intercepts and fixed-effect
interaction tensors — by less than 0.25 SD on most cells.
<!-- src: notes/data-coverage-implementation/03-hierarchical-models.md --> The rule
did not guide the published choice: the fits with the deep term active were
published first, and the zero flavors were fit afterwards, on 2026-07-14, as a
diagnostic <!-- src: tables/gamma_dl_ablation.md -->. This revision re-fits both
flavors for every dimension, and a dimension whose production rows carry no deep
prediction publishes the deep-free fit (§6).

The acceptance gate is the same everywhere and it has two halves that must both
pass. Convergence — split-$\hat R$, bulk and tail effective sample size, and
divergence count — is necessary but not sufficient. A model that mixes cleanly
can still be estimating the wrong quantity, and the sharpest findings below are
exactly cases where the diagnostics were clean and the estimand was wrong. So
every full-scale fit is also scored on the held-out game fold against a baseline —
ROC-AUC over chance or PR-AUC over the marginal for the Bernoulli models and the
binary-decomposed softmaxes, log-loss against the entropy of the pooled held-out
class marginal for the multinomials, log-likelihood lift and RMSE against a
state-only baseline for the counts — and a fit whose held-out evidence is missing,
or fails to beat its baseline, blocks. Held-out calibration error and
posterior-predictive interval coverage are computed and reported beside these but
only warn (§7). <!-- src: bc/python_models/statistical/validate.py -->

### Model A — observation propensity

Model A estimates $P(R_i = 1 \mid x_i)$, the probability that a given batted-ball
dimension was actually observed and recorded for event $i$, and it is the model
the rest of the family leans on to know how selective the record is. It fits one
event-grain Bernoulli per dimension that has both observed and unobserved rows —
`trajectory`, the four location dimensions, and `ball_handler_position`; the
purely derived `pulled_opposite` is 0% observed and excluded.
<!-- src: bc/python_models/statistical/CLAUDE.md --> The predictor pools season,
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
<!-- src: notes/data-coverage-implementation/03-hierarchical-models.md --> A scorer
or park level absent from the training sample receives a zero effect at scoring
time. Until this revision such levels — 12.8% of scored trajectory events, whose
empirical observed rate (0.28) is the lowest of any group — took the effect of the
null-scorer level, the group with the highest observed rate (0.61); the fix is
disclosed here and the six dimensions are refit under it.
<!-- src: bc/python_models/statistical/models/_event_data.py --> <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->

Two operating decisions came out of a sample-size sweep on the trajectory
dimension and both generalize across dimensions. First, the fit runs on 10,000
rows per dimension. Held-out ROC-AUC has plateaued by 10K — trajectory moves only
from 0.910 to 0.912 as rows go from 10K to 1M — while mixing collapses past 500K,
with $\hat R$ climbing from 1.015 to 1.789 and minimum ESS falling from 269 to 6
at a million rows. <!-- src: notes/data-coverage-implementation/implementation-checklist.md --> <!-- src: memory/bayes_sample_size_and_backend.md -->
More data buys nothing the estimand needs and eventually breaks the sampler; 10K
is the budget. Second, seasons whose rare class is nearly saturated — fewer than
100 rare-class events, or a rare-class share below 0.5% — are dropped, because a
near-constant response has almost no logistic gradient and ridges with the season
effect, and downstream consumers read a missing propensity as $p = 1$ anyway.
<!-- src: bc/python_models/statistical/models/_event_data.py --> On the previously
published held-out path the six dimensions cleared ROC-AUC from 0.898 on
trajectory to 0.972–0.973 on the four location dimensions, with
`ball_handler_position` at 0.942 <!-- src: notes/data-coverage-implementation/implementation-review.md -->;
the refit values land in the same range: 0.894 on trajectory, 0.971 to 0.973
across the four location dimensions, and 0.934 on `ball_handler_position`
<!-- src: artifacts/statistical/bayes/*_observedness/10k-v6-unseen-fix/validation/held_out_metrics.json -->.
A single context feature carries
`ball_handler_position`: adding the 13-level plate-appearance-outcome fixed effect
lifted its held-out PR-AUC from 0.569 to 0.755, while leaving the other five
dimensions — already at a 0.97-plus PR-AUC ceiling — untouched.
<!-- src: notes/data-coverage-implementation/implementation-checklist.md -->

### Models E and D — geometry and ball handler

Model E imputes batted-ball geometry — trajectory, location side, depth, edge,
and the fielding region — for events whose class was never recorded, and Model D
imputes the handling fielder position. Both are per-event reference-class
softmaxes over their class sets, with a per-class intercept and a per-class
fixed-effect interaction on each context column (result family, start base-out
state, alignment regime, batter hand):

$$\pi_{i,c} = \operatorname{softmax}_c\!\left(\alpha_c + \sum_j \beta^{(j)}_{c}[x_i] + \gamma \log \tilde p^{dl}_{i,c}\right).$$

<!-- src: docs/estimated-models.md --> Of Model E's four deep-backed dimensions,
trajectory publishes the fit with the deep term active; the three location
dimensions publish deep-free fits, because their production rows carry no deep
prediction (§6), as does `general_location`, which has no deep proposal.
<!-- src: docs/estimated-models.md --> Model D publishes here for the first time:
its held-out top-1 accuracy is 0.220 against a position-prior baseline of 0.171 —
real lift, but a reminder that the handler is genuinely hard to pin from pre-event
state, which is why the full distribution ships and the argmax never becomes a
fact. <!-- src: notes/data-coverage-implementation/implementation-review.md -->
Neither model carries a season term finer than the four-level alignment regime,
whose first level covers every season through 2009; a per-class era effect, if one
exists, is absent rather than cancelled
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->.

Both models train on the observed-only slice and score the unobserved slice, and
both publish under a missing-at-random flavor — the `noprop` operating points —
because the correction for the fact that recording is correlated with the class
being recorded turned out to be unidentifiable from a marginal propensity.
That is the subject of the next section; the geometry and handler surfaces here
are the MAR points those sensitivity ribbons are built around.

### Model C — fielding credit

Model C allocates official credit — putouts and assists — over the nine fielding
positions on events where the record left it unattributed. Each credit type is a
dual-likelihood softmax: a supervised
$Y_e \sim \operatorname{Multinomial}(U_e, \pi_e)$ on well-attributed events, where
$U_e$ is the known unattributed-credit count, sharing its $\pi_e$ with an
aggregate arm $T_m \sim \operatorname{Normal}(\sum_e U_e \pi_{e,k}, \sigma_{\text{box}})$
that anchors a synthetically masked subset to box-cell totals, with
$\sigma_{\text{box}}$ a fixed constant of 0.5 rather than a parameter.
<!-- src: docs/estimated-models.md --> <!-- src: bc/python_models/statistical/models/_credit_data.py -->
The supervised arm is load-bearing: without a per-event observed outcome the
per-event effects cancel (see the closing vignette), and it is the arm that lets
scorers and eras shift the per-position distribution. Both exports score the
production slice of events whose credit is unknown. Until this revision the putout
export scored the training grain instead — every putout row in the published table
was a well-attributed event with nothing to impute — so a consumer summing putout
shares double-counted recorded putouts; the export now scores the production
unknown slice, as the assist export already did, and the putout fit is re-run.
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md --> <!-- src: docs/estimated-models.md -->

Assists are harder and the result is instructive. Cut one folds assist allocation
and a `P(any assist)` decision into a single $K = 10$ softmax with a `NONE`
sentinel class, marginalizing the unknown putout position over the published
putout posterior at scoring time. <!-- src: notes/data-coverage-implementation/03-hierarchical-models.md -->
The fit converges — $\hat R_{\max}$ 1.036, minimum bulk ESS 185, 0 divergences —
and on the held-out set the any-assist decision clears its baseline convincingly,
PR-AUC 0.516 against a 0.348 base rate, while per-fielder identification sits
essentially at the majority-class baseline, top-1 0.664 against 0.652.
<!-- src: notes/data-coverage-implementation/03-hierarchical-models.md --> The value
that survives is the part that does not depend on the missing clue — how many
assists, and their calibrated shares — not the part that names the specific
fielder. The multi-assist count itself is a separate cell-grain model, a plain
multinomial over $M \in \{1,2,3,4\}$ per `(result_family, base_state, outs)` cell
with a centered reference-class softmax; it converges at $\hat R$ 1.009 with 3,969
ESS and 0 divergences and cuts held-out total variation to the count mix by 33%.
<!-- src: bc/python_models/statistical/CLAUDE.md --> <!-- src: notes/followups.md -->

### Model F — park factors

Model F estimates the multiplicative run effect of a park in a season-league, net
of the teams that played there and of home-field advantage. It is a team-game
negative binomial on runs with a $\log$ plate-appearance exposure offset, a
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
prior-predictive fix before it would sample — a $\mathrm{Normal}(0, 1.5)$
season-league intercept against the $\log$-exposure offset implied a run rate far
above any real one and overflowed the negative-binomial forward sampler, and was
replaced by an intercept centered on the empirical log rate with tightened
deviation scales. <!-- src: notes/followups.md --> Two properties of the
previously published fit were limitations rather than features: its AR steps
were consecutive fitted seasons within a park-league chain, not calendar
seasons, and its persistence hyperparameters $\rho$ and $\sigma_{\text{innov}}$
mixed poorly. The published refit is non-centered and gap-aware, with the
innovation over a gap of $d$ seasons following the stationary AR(1) bridge, and
both hyperparameters mix (§7); the chain still never resets at a park
reconfiguration, which §11 carries.
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md --> <!-- src: bc/python_models/statistical/models/park_factor.py -->

### Model G — run expectancy and the transition matrix

Model G carries two submodels over one population: regular-season events in
innings one through eight, with untruncated exposure, restricted to real events —
a plate appearance, a base-out change, or a run — so that substitutions and other
no-play rows are not counted. That is the population the deterministic
`run_expectancy_matrix` uses, and the cells are keyed on the dataset's own
`season` and `league`, so a Negro-league cell carries its real league code rather
than a pooled bucket. <!-- src: docs/estimated-models.md --> The population is
itself a correction disclosed in this revision. The previously published fits
carried no such filter: 29% of the rows in the 2015 NL bases-empty, no-out cell
were substitutions and no-play rows with no plate-appearance result, each carrying
the same runs-to-end-of-inning as the next real event, so the cell counts were
inflated by duplicates and the posterior variance understated; walk-off-censored
innings and non-regular-season games were in the population the deterministic
matrix excludes; the published transition surface put 0.31 of the mass from
bases-empty, no-out on the no-change self-transition, which is not a
plate-appearance law; and the run-expectancy cells were keyed on a league group
that collapsed every non-AL/NL/FL league to `Other`, so the Negro-league run
values borrowed a pooled cell. Both submodels are re-run on the corrected
population.
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->

The run-expectancy arm is a cell-grain negative binomial whose mean pools from a
global level through a per-state level to the cell, with an `era_regime` level
between them — a multi-hot indicator over `pre_DH`, `DH_AL_only`, `full_DH`,
`ghost_runner`, and the extra-inning overlap, pruned to full rank — and a
dispersion $\phi$ per base-out state, because bases-empty and two-out states are
about twice as over-dispersed as loaded zero-out states and a single global
$\phi$ under-covered the former. <!-- src: docs/estimated-models.md --> The regime
level is a correctness fix, not an ornament: the 2020 extra-inning ghost runner
and the designated hitter materially move run expectancy, so a table that pools
every season the source carries into one mean is wrong rather than merely
coarse. The previously published full fit converged at $\hat R$ 1.009 with 0
divergences and beat a state-only baseline by 8% on held-out RMSE
<!-- src: notes/followups.md -->; the corrected fit converges at $\hat R_{\max}$
1.008 with a minimum bulk ESS of 515 and 0 divergences, its era-regime scale
$\sigma_{\text{era}}$ has posterior mean 0.039 on the log scale with the largest
per-state regime offsets near 7%, and it beats the state-only baseline by 7% on
held-out RMSE, 0.0884 against 0.0951
<!-- src: artifacts/statistical/bayes/run_expectancy/re-full-eraregime-v4/validation/ -->.

The transition arm is where a clean diagnostic failure changed the specification.
The submodel is a Markov transition over base-out states,
$P(\text{end} \mid \text{start}, t, l) = \operatorname{softmax}(\zeta)$,
across the 24 base-out states plus a half-inning-ending sentinel — 25 end classes.
The natural specification treats all $24 \times 25$ start-end pairs as reachable,
pins a single global reference class, and pools transition logits across the
corpus. It does not converge: $\hat R$ walls out at 4.04 with an effective sample
size around 5, and the chains sit in different modes.
<!-- src: notes/followups.md --> The instinct is to blame the sampler and
reparameterize, and reparameterizing does nothing, because the failure is
specification, not geometry. Outs never decrease within an event, so more than
half of the start-end pairs are structurally impossible; their free softmax logits
have no data and are driven to $-\infty$ against the prior, creating a likelihood
floor that fights the hierarchy. The pinned global reference — bases empty, no
outs — is itself unreachable from any start with an out already recorded, which
removes the only anchor for those states' softmax and leaves the level
unidentified. <!-- src: notes/followups.md -->

The fix is to make the model respect the domain's hard constraint. Reachable end
classes for a start state are those at out counts $\geq$ the start's outs, plus
inning-end; the impossible cells are pinned to a large negative logit and carry no
free parameter; each start state pins its own modal reachable class as the
reference; and the corpus pooling level is dropped, since start states with
disjoint supports and different references cannot meaningfully pool. The mask
constrains outs only: pairs that are base-impossible in a single event at a legal
out count — bases empty to bases loaded with no out recorded, for instance — remain
free parameters, 115 of the 408 masked-reachable pairs, and the posterior puts
mass of order $10^{-5}$ on them; that is a cost in pure-prior parameters, not an
error in the published surface.
<!-- src: bc/python_models/statistical/models/state_transition.py --> <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->
The predictive softmax is built in float64 so the masked $\exp(-30)$ entries stay
representable for the multinomial sampler. <!-- src: notes/followups.md -->
The redesigned fit cleared the strict gate at full scale — $\hat R_{\max}$ 1.016,
minimum bulk ESS 188, 0 divergences — and its held-out total variation to the
transition mix fell from 0.070 to 0.044, a 37% reduction, on the population
before the correction above <!-- src: notes/followups.md -->; the published
successor of that fit carried $\hat R_{\max}$ 1.048 and a weak-identification
flag, and the corrected-population refit at 4,000 draws reaches $\hat R_{\max}$
1.026 with zero divergences but still carries the flag: one transition intercept,
`alpha_trans[1]`, sits at a bulk ESS of 227 against the 400 floor. A longer run
does not fit in memory, since the stored posterior of the 94,832 cell effects
alone is about 24 GB at 8,000 draws, so the flag ships with the table.
No amount of $\hat R$ inspection on the original fit would have suggested the
remedy; the reachability structure had to be read off the baseball, and this is
the concrete face of the paper's stance that convergence is necessary but never
sufficient.

### Model J — pitch coverage and summary

Model J estimates whether a plate appearance's final ball-strike count was recorded
and, where it was, the distribution over final counts. The coverage arm is an
event-grain Bernoulli in the same shape as Model A, reused against the pitch-count
population — about half of events lack an observed count — and it clears its
smoke gate at held-out ROC-AUC 0.995, though it is fit only at smoke scale and
not published: its table `pitch_count_coverage` materializes a typed zero-row
frame pending a full-scale artifact, and the publish path now refuses a smoke fit
outright (§9). <!-- src: docs/estimated-models.md --> The summary arm is a
cell-grain multinomial over the 12 `balls × strikes` final-count classes per result
family, season, and league, built the same way as the transition arm: a per-family
structural mask pins the classes a family cannot end on — a strikeout ends on two
strikes, a walk on three balls — to a large negative logit with no free parameter,
each family pins its own modal reachable class as reference, and the few hundred
recorded events per constrained family that sit in an impossible cell are dropped
as data errors before the fit. <!-- src: docs/estimated-models.md --> The
previously published fit had no such mask and pinned `b0_s0` — an impossible
final count for both constrained families — as the global reference; its
lowest-ESS parameters were exactly those impossible classes, they set the table's
weak-identification flag, and the published surface carried up to 0.06 of mass on
strikeouts ending with fewer than two strikes. The corrected fit is re-run.
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->

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
