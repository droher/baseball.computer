---
title: "Estimating the Unrecorded Game: Hierarchical Bayesian Coverage Models for a Century of Baseball Play-by-Play"
type: design-doc
status: draft
audience: sports-analytics researchers, applied Bayesian statisticians
last-verified: 2026-07-13
---

<!-- Structural deviation from tech-write templates: this is a research paper
(JQAS/AOAS register), not a design doc — sections follow the academic
convention (intro, data, methods, results, discussion) rather than a template.
<!-- src: ... --> comments are internal provenance scaffolding; strip before
external submission. Plan: notes/paper/OUTLINE.md. -->

# Estimating the Unrecorded Game: Hierarchical Bayesian Coverage Models for a Century of Baseball Play-by-Play

## Abstract

The play-by-play record of major-league baseball is nearly complete in
outcomes but selectively incomplete in detail: whether a scorer recorded a
ball's trajectory, location, or handler depends on era, scorer practice, and
the play's result, not on the play alone. Treating the record as the output of
two coupled processes — the game and its observation — we build hierarchical
Bayesian models over 18.1 million events (1910–2025) that estimate both the
propensity that each detail was recorded and posterior distributions over the
details themselves. Twelve probabilistic surfaces are defined — ten populated,
two deferred as typed zero-row frames — spanning observation propensity,
batted-ball geometry, fielding credit, park factors, run expectancy, base-out
transitions, and pitch summaries, each row carrying provenance and uncertainty.
Held-out calibration gates — expected calibration error ≤ 0.018 across
propensity dimensions — and posterior-predictive interval coverage gate
publication.

Three findings organize the paper. First, trajectory recording before 1988 is
missing not at random — ground balls appear at half their true share — and a
masked backtest refutes the natural learned correction: a coefficient on
observation propensity learns the selection effect with the wrong sign while
passing every convergence diagnostic. The identified fix is a fixed per-class
selection offset — a ground-ball selection log-odds of +1.24 before 1950,
anchored from the trajectory-deduction slice — published as a joint sensitivity
ribbon because its magnitude is unidentified: the pre-1950 unobserved
ground-ball share rises from 0.32 under MAR to 0.58 [0.56, 0.60] at the full
anchor. Masked backtests bound the correction — near-exact under class-only and
era-graded selection, partial under class-by-covariate selection, inert under
block absence. Second, shared entity-embedding pretraining over the full corpus
multiplies player-effect signal in downstream models by roughly 6×, under
cross-fitting and leakage gates that caught one real leak; an ablation shows the
deep covariate materially reshapes trajectory estimates yet is retained on
predictive grounds. Third, several natural estimands — scorer label confusion,
fielder responsibility, shift propensity — are unidentifiable from a
single-source record; we document why and publish nothing.



## Introduction

A century of major-league baseball play-by-play looks, at first, like a solved
data problem. Every game since 1910 that was recorded at the event level has a
complete account of outcomes: who batted, what the result was, how the base-out
state changed, which runs scored. The database this paper draws on holds
205,845 play-by-play games and 18,141,020 events across the 1910–2025 span
<!-- src: tables/corpus_by_decade.md -->. Outcomes are there.
What is not uniformly there is detail: the trajectory of a batted ball, its
location, which fielder handled it, the sequence of pitches, the fielder charged
with a putout. These fields are missing not at random across the game but
according to who was keeping score and when.

The distinction matters because detail completeness is a property of the
observation process, not of the game. A scorer in 1935 and a scorer in 2015 both
watched a ground ball to shortstop; only one of them reliably wrote down that it
was a ground ball. When the record omits the trajectory, the omission is
systematic. Using the deduced-trajectory rows — cases where fielding evidence
lets a rule recover the broad class that the scorer did not write down — as a
partial view of the unobserved population, the ground-ball share among recorded
trajectories before 1950 is 33.7 percent, against 68.0 percent once the deduced
rows are folded in <!-- src: notes/data-coverage-implementation/implementation-review.md -->.
Across 1950–1987 the gap persists: 44.1 percent recorded against 77.8 percent
including deduced <!-- src: notes/paper/tables/groundball_mnar.md -->.
From 1988 on the two figures converge to within a point and a half
<!-- src: notes/data-coverage-implementation/implementation-review.md -->. Scorers
before 1988 selectively omitted routine grounders, and the omitted population is
far ground-enriched. A model that treats the recorded trajectories as a random
sample of all trajectories will under-impute ground balls by tens of points in
exactly the era where nearly everything must be imputed — the unobserved slice
is 78 to 95 percent of all events before 1988 across the geometry and location
dimensions <!-- src: notes/data-coverage-implementation/implementation-review.md -->.

This is the record problem: the play-by-play archive is the joint output of two
coupled processes. One is the game — a ball is hit, it has a latent trajectory
and location, fielders handle it, runners advance, a scorer assigns official
credit. The other is the observation process — a source, a scorer, an inputter,
a translator, and a parser recorded or dropped parts of that event. Most
existing treatments of historical baseball data blur the two, filling missing
detail with deterministic rules, source-precedence orderings, and sample-size
thresholds that hide the assumption that complete cases stand in for missing
ones. Those rules are useful and this paper keeps the good ones as measurement
constraints, but they publish a point where the honest answer is a distribution,
and they say nothing about whether the complete cases are representative.

We take the other route. Modeling the observation process explicitly — who
recorded what, when, and why — turns missing data from a cleaning nuisance into
an estimand. It yields probability surfaces whose calibration is checked
against held-out data — reliability and interval coverage, not asserted —
where deterministic imputation would fabricate certainty. And it draws a line,
for each quantity,
between what a single-source record can identify and what it cannot: some
targets are unidentifiable from one scorer's account and must not be published
as facts, only bounded or withheld.

The paper makes five contributions.

First, a taxonomy of missingness that separates the game process from the
observation process. A single missing-completely-at-random / missing-at-random /
missing-not-at-random label per field is the wrong ontology for a record where
one row can be complete for batting, missing a trajectory, complete for team
fielding, and missing a putout fielder. We classify ten missingness mechanisms
and identify which are selection-biased on the unobserved value itself.

Second, a family of hierarchical Bayesian coverage models that share one
statistical contract — non-centered partial pooling, shared hash-based data
splits, a per-model deep-learning-covariate ablation, and acceptance gates that
pair convergence diagnostics with held-out predictive accuracy, held-out
calibration, and posterior-predictive interval coverage
<!-- src: notes/paper/tables/validation_gates.md -->. The family spans
observation propensity (Model A, published as `scorer_observation_propensities`),
fielding credit (Model C, `imputed_fielding_credit`), ball handler (Model D,
`imputed_ball_handler_probabilities`), batted-ball geometry (Model E,
`imputed_batted_ball_geometry`), park factors (Model F, `park_factor_summary`),
run expectancy and base-out transitions (Model G, `run_expectancy_summary` and
`state_transition_summary`), and pitch coverage and summary (Model J)
<!-- src: docs/estimated-models.md -->.

Third, a treatment of missingness that is not at random. For the trajectory and
location dimensions, whether a label was recorded is correlated with what the
label would have been. We report a learned correction that we refute — a
propensity coefficient that converges cleanly but estimates the wrong sign — a
fixed per-class selection offset δ_c that the observed-only likelihood cannot
identify by itself, an anchored per-era estimate of δ_c drawn from a
deduced-trajectory partial-truth slice, and a joint sensitivity ribbon over the
offset vector in place of a single point estimate
<!-- src: bc/python_models/statistical/mnar_anchor.py, bc/python_models/statistical/sensitivity.py -->. The offset
mechanism is checked across four masking designs spanning class-marginal,
covariate-joint, and block-structured selection; correction quality ranges from
near-exact recovery under class-marginal selection to a documented partial
correction when selection depends on a covariate the model already conditions
on <!-- src: notes/paper/tables/mnar_backtest_robustness.md -->.

Fourth, a deep-learning supplement that supplies shrunk proposal distributions
and shared entity embeddings to the Bayesian layer, never published facts. Its
term enters the softmax as γ_c · log p̃^dl_{i,c} with γ_c ~ N(0, 0.5), behind
calibration and leakage gates and a cross-fitting contract; argmax-to-fact is
banned.

Fifth, a publication policy. Estimated surfaces ship as posteriors with an
eight-column provenance contract — artifact_id, model_name, model_version,
source_snapshot_id, method, observed_status, confidence_status, and
weak_identification_flag <!-- src: notes/paper/OUTLINE.md --> — kept in a
separate namespace from recorded and deterministic facts. Three designed models
are withheld with their names reserved, for three different reasons: contact-
label confusion (B), where a single scorer per event and no independent second
label leave the data uninformative about the confusion matrix — a genuine
identification limit; fielder responsibility (I), where the positioning prior a
correct model needs does not exist anywhere in the source — a data-availability
limitation, not an identification proof; and shift propensity (K), designed but
not yet built in this pass — unfinished scope, not a claim about the record.
Publishing the propensity to observe, the posterior over what was observed, and
a sensitivity ribbon over what was not — and nothing else — is the discipline
the record demands.


## The record and its gaps

The corpus is a DuckDB database built from several historical baseball source
families, each recorded at a different grain and playing a different role
<!-- src: notes/data-coverage-implementation/README.md -->. Play-by-play sources
carry event and event-player detail — base-out state, batting, pitching,
fielding, baserunning, batted-ball clues, and pitch sequences — and are the only
source from which event-level quantities can be estimated. Box scores carry
official aggregate totals at the game-player and game-team grain: batting lines,
pitching lines, fielding lines, line scores, decisions, and earned runs. Gamelog
and schedule sources establish that a game happened and how it ended. Season
supplements carry season-player and season-team totals that fill in where event
or box coverage is coarser <!-- src: notes/data-coverage-implementation/README.md -->.
The invariant that organizes all of them is that a box-score putout, a
rule-derived batted-ball location, an estimated expected putout, and a
sampled synthetic event are different quantities and are never stored in one
column <!-- src: notes/data-coverage-implementation/README.md -->.

Within the 1910–2025 target span the source mix is dominated by play-by-play but
not exclusively so. The snapshot holds 205,845 play-by-play games, 1,953
box-score-only games, and 4 gamelog-only games <!-- src: notes/data-coverage-implementation/README.md -->.
The event table, `event_states_full`, contains 18,141,020 events, all of them
from the play-by-play games <!-- src: tables/corpus_by_decade.md -->.
Event-level estimation is scoped to those games; the box-score-only and
gamelog-only rows stay at aggregate grain and are not given fabricated event
records <!-- src: notes/data-coverage-implementation/README.md -->. At the
coarser season-team grain the same split appears as 2,929 play-by-play
team-seasons against 269 box-score team-seasons
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->,
but source absence is not uniform within a season, so the modeling layer works
from a game-and-dimension ledger rather than a season-level flag.

Having event rows is not the same as having every field on them. The gaps that
this paper's models target are field-level, and they are large. Of 12,038,982
batted-ball rows in the deterministic derivation table, 3,992,018 retain an
unknown final trajectory even after rule-based inference has recovered every
broad class the fielding evidence supports, and 6,989,832 have no recorded
location <!-- src: notes/data-coverage-implementation/README.md -->. On the
fielding side, 463,102 events carry unknown putouts within the span
<!-- src: notes/data-coverage-implementation/README.md -->. An unknown putout is
worse than a single missing field because it usually means the assist chain is
also unrecorded: the record may know that an out occurred without knowing who
recorded it or whether an assist was involved
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.
These counts are not acceptance thresholds; they size the first modeling targets
<!-- src: notes/data-coverage-implementation/README.md -->.

Missingness is also multi-dimensional within a single event, which is why no
one completeness flag can describe a row. The database already exposes coverage
along separate axes — trajectory, general location, batted-to-fielder,
depth, angle, and strength for batted balls; count, pitch sequence, pitch
results, and strike types for pitches; putouts, assists, and errors for fielding
credit <!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.
A row can be complete for basic batting, incomplete for batted-ball trajectory,
complete for team fielding, incomplete for player fielding credit, and unusable
for pitch-sequence metrics all at once
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.
The coverage of each axis, in turn, is stratified by result: batted-ball
trajectory and location are recorded more often on outs than on hits, and the
metric layer already tracks known-trajectory rates separately for the two
because the missingness mechanism is result-dependent
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.
Known location among hits is not a random sample of all hit locations; it is a
scorer-selected subset <!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.

The one dimension that dominates all of these is era. Coverage of trajectory,
location, pitch sequence, and fielding credit is sparse and heavily selected in
the early decades, improves through the middle of the century, and reaches
modern batted-ball and location fidelity only in the most recent seasons; the
defensive-shift era at the end of the span then changes the meaning of a
fielder's position as a proxy for location
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.
The full decade-by-dimension coverage structure — observation propensity by era
and dimension from Model A — is an empirical result rather than a fixed corpus
statistic, and we present it as a table in the Results section rather than
restating raw counts here. What the counts above establish is only the shape of
the problem: complete outcomes, field-level gaps that run into the millions, and
a missingness pattern that tracks the scorer and the era rather than the game.


## A taxonomy of missingness

The standard missing-data vocabulary assigns one mechanism — missing completely
at random, missing at random, or missing not at random — to a variable. That
granularity is wrong for this record. A single event can be complete for basic
batting, missing at random for trajectory given the scorer and result, missing
not at random for hit location, structurally absent for pitch sequence, and
merely aggregate-only for fielding credit, all at the same time. The unit that
carries a mechanism is the event-dimension, not the field and not the row, and
the mechanism is a statement about a process, not a flag.

Two coupled generative processes produce every value. The baseball process
generates the latent state: a ball has a latent contact class and geometry,
fielders handle it, runners advance, runs score, and an official scorer assigns
credit. The observation process generates the record: a source, scorer,
inputter, translator, and parser record or omit parts of that state
<!-- src: notes/data-coverage-implementation/statistical-modeling-coverage-design.md -->.
Written as a schema, the latent baseball state maps to official credit and
outcome, and the latent state together with the scorer-and-source process maps
to the recorded label or its absence
<!-- src: notes/data-coverage-implementation/statistical-modeling-coverage-design.md -->.
Most heuristics blur these two maps; separating them is the point of the
taxonomy.

Under that separation, ten missingness classes recur across the record, each
with its own detector and its own defensible imputation family
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.

| Class | What it means |
|---|---|
| Structural absence | The target grain does not exist though a coarser one does — no event rows for a gamelog-only game. |
| Source-family block absence | The grain exists for some games but a whole source family or file block was never acquired. |
| Aggregate-only coverage | An official or parser aggregate exists but event attribution does not. |
| Field-level unknown | The event exists but a parsed field is `Unknown`, `0`, `Default`, or null. |
| Selection-biased detail | Detail is recorded only for non-random subsets, keyed to result salience or scorer habit. |
| Cross-source disagreement | Event-derived and box-derived totals conflict. |
| State reconstruction gap | The event exists but the state needed to interpret it is derived indirectly. |
| Official scoring convention gap | The raw facts are known but official credit follows a scoring convention, not event logic. |
| Taxonomy collapse | Raw codes exist but the wanted category is a coarser, more stable recode. |
| Sparse-context estimation | Detail exists but the conditioning bucket for adjustment is thin. |

<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->

These classes call for different responses, and conflating them destroys the
model. Structural absence and aggregate-only coverage are not imputation
targets in the event namespace at all; they stay at aggregate grain with source
flags. Taxonomy collapse is deterministic recode, not inference. Cross-source
disagreement is resolved by an authority order, not a fill. Field-level unknowns
take deterministic inference first, empirical priors second, and model
predictions last, always preserving the raw and imputed values separately. The
sentinels themselves must stay distinct: null, `Unknown`, `Default`, `0`,
not-applicable, aggregate-only, and known-source-issue are different markers, and
flattening them into one missing indicator discards the missingness model
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.

Every imputed value therefore carries companion facts for source, method,
observed status, and confidence or weight; without them a downstream aggregation
cannot tell an official total from a deterministic derivation from a posterior
draw from a block-missing gap
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.

The default working assumption for the observation models is missingness at
random conditional on context — source, scorer, inputter, translator, result,
hit-or-out state, leverage, era, and base-out state
<!-- src: notes/data-coverage-implementation/statistical-modeling-coverage-design.md -->.
Two dimensions are flagged as not satisfying it. Hit location and detailed
contact type are treated as missing-not-at-random risks, because whether the
label was recorded is correlated with what the label would have been
<!-- src: notes/data-coverage-implementation/statistical-modeling-coverage-design.md -->.
These are the selection-biased-detail class in its sharpest form, and they are
handled not by an MAR fill but by pattern-mixture sensitivity — letting the
missing values take distributions shifted from the observed ones within bounded
plausibility — the subject of the section on selection that never recorded
itself. A hierarchical model cannot rescue an unidentified estimand: where
scorer, park, team, source, and era are inseparable in a slice, the output is
tagged weakly identified or withheld rather than reported as a fill
<!-- src: notes/data-coverage-implementation/statistical-modeling-coverage-design.md -->.


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


## Deep-learning supplements

### Role in the pipeline

Deep models in this system produce proposal distributions and entity embeddings. They never publish a fact. The invariant that governs every supplement registered under `bc/python_models/statistical/deep/targets/` is a fixed pipeline shape: deep model → out-of-fold calibration and leakage checks → Bayesian layer → published surface <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md -->. A deep classifier's argmax class is never written to a `main_models` table; SQL artifact views expose probability vectors, and the acceptance criteria explicitly forbid a downstream table that exposes only the top class of a probabilistic target <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md -->. Where a deep proposal does reach a Bayesian model, it enters as a covariate on the log scale — a per-class logit `γ_c · log p̃^dl_{i,c}` added to the model's own linear predictor, with `γ_c ~ N(0, 0.5)` shrinking the deep signal toward zero unless the data support it <!-- src: notes/paper/OUTLINE.md -->. Each downstream Bayesian model that consumes a proposal is fit twice — once with `γ_dl` fixed at zero, once with the shrinkage prior active — and the two artifacts are stored as distinct flavors (`gamma_dl_zero`, `gamma_dl_shrunk`). The design rule ships the flavor whose inclusion does not move publication-tier posteriors by more than 0.25 SD on most cells, since a larger shift means the deep model absorbed structural signal the hierarchy already carries rather than adding incremental lift <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md -->. In the geometry softmaxes the publication-tier blocks are not scorer, park, or era random effects — those enter every class logit equally, cancel inside the per-event softmax, and were removed — but the per-class intercept `alpha_class` and the fixed-effect interaction tensors (`delta_alignment_regime`, `delta_base_state_start`, `delta_outs_start`, `delta_result_family`, `delta_batter_hand`) that drive the imputation distribution <!-- src: notes/paper/tables/gamma_dl_ablation.md -->.

Running the four `gamma_dl_zero` counterparts at the published operating point — the `e-v12` dataset, 9,988 events, nutpie, four chains, seed 20260513, all converging at rhat_max ≤ 1.0042 with zero divergences — makes the rule measurable, and it splits the four dimensions <!-- src: notes/paper/tables/gamma_dl_ablation.md -->. The deep covariate is not inert: the shrunk flavor wins held-out log-loss and macro PR-AUC on every dimension — for trajectory, log-loss 1.1382 against 1.2327 and PR-AUC 0.4623 against 0.3484 <!-- src: notes/paper/tables/gamma_dl_ablation.md -->. On the shift diagnostic — per-cell |posterior-mean shift| in pooled posterior SD — `location_side` moves 21.7% of 138 cells past 0.25 SD, `location_depth` 27.2% of 92, and `location_edge` 39.1% of 92, so the "most cells under threshold" rule holds for the three location dimensions; `trajectory` moves 69.6% of 115 cells (mean 0.735 SD, max 3.27 SD) and fails it outright <!-- src: notes/paper/tables/gamma_dl_ablation.md -->.

The rule as written is false for trajectory, and the resolution is not to switch the pointer. All four dimensions publish the shrunk flavor on predictive grounds: for trajectory the shrunk fit is strictly better held-out — log-loss −0.094, top-1 +4.0 points, macro PR-AUC +0.114 on 616,513 events — so the deep covariate carries real signal, not distortion, and the published estimate is the better model <!-- src: notes/paper/tables/gamma_dl_ablation.md -->. The casualty is the claim, not the estimate: for trajectory the deep logit materially reshapes the publication-tier effect tensors, and that reshaping is disclosed as a caveat — the deep signal partially absorbs structure the class and covariate tensors would otherwise carry — rather than asserted away.

### Shared entity-embedding pretraining

Per-target deep models originally learned their own `batter_id` / `pitcher_id` embeddings from scratch, seeing only the row slice each target's label happens to cover — trajectory only sees observed batted balls, handler only sees recorded putout chains. That starves the embedding of the cross-context signal a player's behavior carries. The fix pretrains one shared player embedding once, over the event universe filtered to batted-ball plate appearances — roughly 12M of the 18.1M events, since non-batted-ball rows are NULL on every pretext head — against a multi-head pretext objective, and every downstream target warm-starts from the resulting artifact <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md --> <!-- src: memory/pretrain_architecture.md -->. The embedding group is batter and pitcher only; park and scorer carry their own separate embeddings, and fielders and runners are deliberately excluded — including them diluted the batter/pitcher signal in earlier iterations without downstream payoff <!-- src: memory/pretrain_architecture.md -->. Five pretext heads cover exactly the imputation targets that are genuinely missing in the record — `trajectory_remapped`, three batted-location facets, and `batted_to_fielder_class` — while the outcomes that are fully observed at imputation time (`pa_result`, outs and runs on the play, and the three runner-advancement fields) enter as inputs rather than heads, so the pretext conditional distribution matches the inference distribution and no trivial outcome head can crowd out the hard imputation heads under the multi-task weighting <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md --> <!-- src: memory/pretrain_architecture.md -->. Training is a residual two-stage decomposition: a stage-1 context-only fit caches its per-head pre-softmax logits, and stage-2 adds the entity embeddings on top of that frozen offset, so the embeddings learn residual and interaction structure rather than re-encoding player skill already carried by context; focal loss counters the severe class imbalance in the location heads <!-- src: memory/pretrain_architecture.md -->. The gate that justifies the machinery is a permutation-importance comparison against a no-pretrain baseline whose embeddings initialize from scratch: on the geometry-trajectory target, pretraining lifts batter permutation-importance 6.4× and pitcher permutation-importance 6.3× over that baseline <!-- src: notes/data-coverage-implementation/phase3-acceptance-gates-v6.md -->.

### Two negative results

Two failures in this supplement stack are worth stating plainly, because both would have shipped a contaminated posterior if the acceptance gates had not caught them.

**A proxy metric that pointed the wrong way.** During pretrain-architecture selection, a fast in-loop diagnostic — a linear probe (logistic regression) fit on the frozen pretrain embeddings to predict `pa_result` / trajectory / outs — was used to rank candidate pretrain configurations. One candidate won the probe by +0.0068 over the comparison baseline. When the same candidate was evaluated the way it actually matters — permutation importance on a real downstream Keras fit that fine-tunes the embeddings jointly with the trunk, on the held-out `time_forward_fold = 'VALIDATE'` slice — it came out worse than no pretraining at all, by −0.001 on batter permutation-importance <!-- src: memory:phase3_pretrain_proxy_misleads_downstream.md -->. The mechanism is that the linear probe measures signal remaining in the embeddings at the end of pretraining, while a fine-tuned downstream fit lets the trunk absorb or overwrite that signal during joint training; the two numbers answer different questions and can move in opposite directions. The rule that replaced the proxy: never ship a pretrain artifact on probe evidence alone. The gate is downstream permutation importance against a `BC_DEEP_DISABLE_PRETRAIN=1` baseline, evaluated on the same held-out fold every per-supplement gate uses <!-- src: memory:phase3_pretrain_proxy_misleads_downstream.md -->.

**An in-sample fold mislabeled as out-of-fold.** The trajectory geometry target was registered with `fold_count=1`. Because the cross-fitting code path only runs the out-of-fold loop when `fold_count>1`, a `fold_count=1` spec instead falls into a fallback that scores the full training set with the full-fit model and tags every row `OOF` <!-- src: notes/data-coverage-implementation/implementation-review.md -->. The result was 8.43M in-sample predictions carrying an out-of-fold label, flowing through `dl_proposal_manifest` into the geometry Bayesian model as a `gamma_dl_shrunk` covariate — a consumed, published posterior trained partly on leaked information. On an identical game-hash holdout, the leaked artifact scored 1.15 percentage points higher on trajectory top-1 accuracy than the refit: 0.5031 leak-inflated versus 0.4916 once the leak was removed <!-- src: notes/data-coverage-implementation/implementation-review.md -->. The fix was mechanical — refit with `fold_count=5` so every prediction is genuinely out-of-fold (8,434,463 OOF rows verified across 5 folds, zero nulls) — but the finding generalizes past this one target: the fallback path mislabels in-sample predictions as `OOF` with no warning, so any future spec left at `fold_count=1` inherits the same silent leak <!-- src: notes/data-coverage-implementation/implementation-review.md -->.

### The cross-fitting contract this enforces

Both failures motivate the same standing rule. Every deep proposal consumed by a Bayesian model must be produced by a model that never saw that row during training — trained on `fold_id != k`, predicted on `fold_id = k`, calibrated on a held-out slice, and exported with an explicit `prediction_scope` (`out_of_fold`, `validation`, `test`, `full_fit`) so downstream code can enforce the distinction rather than infer it <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md -->. Embeddings carry a parallel check: a source-probe classifier trained to predict source family or scorer from the embedding vector. An AUC at or above 0.75 marks the embedding diagnostic-only for that source family — it may not enter a Bayes covariate, only the `gamma_dl_zero` flavor is publishable; below 0.65 it is fully publication-eligible; the band between is a manual-review gray zone recorded in the fit manifest <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md -->. Both gates exist because convergence diagnostics cannot see either failure mode — a leaked or source-encoded proposal can sample cleanly and still bias what gets published.


## Published surfaces

This section reads `bc.db` rather than the model code that built it. Every
number below is a query against the published `main_models.*` estimated
tables; each table caption below names its source data table
(`notes/paper/tables/<name>.md`) and the exact SQL that produced it
(`notes/paper/queries/<name>.sql`), so every figure regenerates. The event
universe behind these tables is 18,141,020 play-by-play events across
205,845 games <!-- src: tables/corpus_by_decade.md -->, and the models
described in the preceding sections turn a fixed subset of that universe's
missing fields into posteriors.

### Twelve tables, two of them empty on purpose

`docs/estimated-models.md` names twelve published `main_models.*` estimated
tables, and a direct inventory query confirms all twelve exist with the
provenance contract populated:

| table_name | n_rows | n_artifacts | model_versions | confidence_statuses |
| --- | ---: | ---: | --- | --- |
| assist_count_distribution | 596 | 1 | 0.3.0 | exploratory |
| imputed_advancement_probabilities | 0 | 0 | NULL | NULL |
| imputed_ball_handler_probabilities | 11,712,096 | 1 | 0.3.0 | exploratory |
| imputed_batted_ball_geometry | 253,013,169 | 5 | 0.3.0 | exploratory |
| imputed_fielding_credit | 4,257,414 | 2 | 0.3.0 | exploratory |
| linear_weights_estimated | 5,099 | 1 | 0.3.0 | exploratory |
| park_factor_summary | 2,636 | 1 | 0.3.0 | exploratory |
| pitch_count_coverage | 0 | 0 | NULL | NULL |
| pitch_summary_distribution | 18,156 | 1 | 0.3.0 | exploratory |
| run_expectancy_summary | 5,891 | 1 | 0.3.0 | exploratory |
| scorer_observation_propensities | 67,397,468 | 1 | 0.3.0 | exploratory |
| state_transition_summary | 152,925 | 1 | 0.3.0 | exploratory |

<!-- src: tables/table_inventory.md --> `imputed_advancement_probabilities`
(Model H) and `pitch_count_coverage` (Model J's coverage arm) hold 0 rows
with NULL provenance aggregates. That is the documented deferred-publication
mechanism, not a query error: both `@model`s and their grain are wired, but
neither has a published Bayes artifact pointer to resolve, so each
materializes its typed zero-row frame rather than fabricate rows against a
model that never fit. <!-- src: docs/estimated-models.md --> Every populated
table is stamped `model_version 0.3.0` and `confidence_status exploratory` —
none has yet cleared the stricter `passed` gate. `imputed_batted_ball_geometry`
alone accounts for 253M of the roughly 337M total published rows — about
three-quarters — because it publishes a share per class per geometry dimension
per unobserved event; its
five `artifact_id`s are one per geometry dimension (trajectory, three
location facets, general location), and `imputed_fielding_credit`'s two are
one per credit type (putout, assist). <!-- src: tables/table_inventory.md -->

### Coverage collapses at the 1988 boundary

The record's completeness is not a slow trend; it is a step. The deterministic
share of batted-ball events with unknown trajectory or location, by decade:

| decade | share unknown trajectory | share unknown location |
| ---: | ---: | ---: |
| 1900 | 0.497 | 0.386 |
| 1920 | 0.479 | 0.388 |
| 1940 | 0.655 | 0.545 |
| 1960 | 0.526 | 0.382 |
| 1980 | 0.460 | 0.357 |
| 1990 | 0.017 | 0.012 |
| 2000 | 0.013 | 0.008 |
| 2020 | 0.00003 | 0.00002 |

<!-- src: tables/coverage_by_decade.md --> Unknown share drops by roughly two
orders of magnitude between the 1980s and 1990s, tracking Retrosheet's shift
to detailed batted-ball location strings in play-by-play files around 1990.
<!-- src: tables/coverage_by_decade.md --> Model A's posterior propensity to
observe tells the same story from the fitted side, decade means of
`p_observed_mean` by geometry dimension:

| decade | trajectory | location_side | location_depth | ball_handler_position |
| ---: | ---: | ---: | ---: | ---: |
| 1910 | 0.252 | 0.042 | 0.059 | 0.765 |
| 1940 | 0.152 | 0.035 | 0.029 | 0.705 |
| 1970 | 0.191 | 0.039 | 0.040 | 0.934 |
| 1980 | 0.305 | 0.216 | 0.205 | 0.897 |
| 1990 | 0.954 | 0.936 | 0.955 | 0.938 |
| 2010 | 0.975 | 0.958 | 0.948 | 0.948 |
| 2020 | NULL | 0.910 | 0.979 | 0.942 |

<!-- src: tables/obs_propensity_by_decade.md --> Trajectory and location
propensities move together, sitting mostly under 0.3 through the 1980s and
jumping past 0.93 from 1990 forward; `ball_handler_position` is the outlier,
already 0.70–0.93 propensity before 1990, because a handler is partially
recoverable from box-score fielding lines even when no play-by-play trajectory
was logged. The 2020-decade `trajectory` cell reads NULL rather than a number, and that is
a filter convention, not a coverage gap. `prepare_event_observation_inputs`
drops any season whose unobserved-row (rare-class) count falls under 100
events or under 0.5% of the season's rows, and only surviving seasons get
scored into `scorer_observation_propensities`. Every 2020-2025 season fails
that floor for `trajectory` — season-level unobserved-row counts run 6, 31, 2,
2, 1 across 2020-2024 — because trajectory recording is by then effectively
saturated, so none of the six 2020s seasons produces a row and the decade
average has nothing to average. <!-- src: tables/obs_propensity_by_decade.md -->
The four location dimensions saturate on the same convention from 2021
forward but keep exactly one 2020s season — season 2020 itself, rare count
1,444, rare rate 3.1%, clearing both floors — which is why their 2020-decade
cells above are single-season figures rather than six-season averages;
`ball_handler_position`'s rare class never saturates, so it keeps all six.
<!-- src: tables/obs_propensity_by_decade.md --> A missing `p_observed_mean`
is this pipeline's documented convention for "fully observed by construction"
($p \approx 1$), the opposite of a coverage gap: the row that would show a
number near 1.0 is absent because the filter that would have produced it
never ran on a fully-saturated season.

### A missing-not-at-random signature, cleanly measured

The `groundball_mnar` table compares two versions of the pre-1988 ground-ball
share: the share among events where trajectory was directly recorded by the
scorer, and the share once trajectory rows deduced from the fielding record
(assisted infield putouts, by construction) are folded in as a partial-truth
peek at what scorers left unrecorded:

| era | n_observed | n_derived | ground_share_observed | ground_share_obs_plus_derived | gap |
| --- | ---: | ---: | ---: | ---: | ---: |
| pre-1950 | 712,469 | 763,993 | 0.3368 | 0.6800 | 0.3432 |
| 1950-1987 | 699,353 | 1,057,776 | 0.4410 | 0.7777 | 0.3367 |
| 1988+ | 4,759,451 | 53,922 | 0.4495 | 0.4557 | 0.0062 |

<!-- src: tables/groundball_mnar.md --> Pre-1950, observed-only ground share is
34%; adding the deduced rows pulls it to 68% — a 34-point gap that direct
recording alone cannot see, because deduction fires only on assisted infield
putouts and every derived row is `GroundBall` by construction, so the gap is a
lower bound on how much pre-1988 scorers under-recorded routine grounders
relative to hits and fly balls. <!-- src: tables/groundball_mnar.md --> The
gap collapses to 0.6 points once Retrosheet-era play-by-play (1988+) records
trajectory comprehensively, which is the same boundary the coverage-by-decade
table shows independently. This comparison is deliberately built from
`model_input_geometry`'s `observed_status ∈ {observed, derived}` split, not
from any BSL semantic-layer rate such as `offense_events.ground_ball_rate`.
That metric is built on `calc_batted_ball_type`'s merged `trajectory` column,
which already substitutes the same deduced value whenever the recorded
trajectory is unknown <!-- src: bc/models/intermediate/event_level/calc_batted_ball_type.sql -->
— so a season-level ground-ball rate computed from it is pulled toward the
68%-side number in exactly the pre-1988 seasons where this section's point is
that the two numbers must be kept apart. Using it in place of the
observed-only share would launder the selection effect this table exists to
expose.

### Geometry marginals on the slice that was never recorded

`imputed_batted_ball_geometry` restricts its trajectory rows to events with no
recorded trajectory — the same slice `coverage_by_decade` sizes — and the
task's four assumed trajectory classes turn out to be five in the data
(`Bunt` is a fifth, distinct class alongside Fly, GroundBall, LineDrive,
PopUp). Posterior mean expected share by era bucket:

| era_bucket | Bunt | Fly | GroundBall | LineDrive | PopUp | n_rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| pre-1950 | 0.076 | 0.224 | 0.320 | 0.224 | 0.155 | 2,615,579 |
| 1950-1987 | 0.068 | 0.087 | 0.376 | 0.305 | 0.165 | 3,117,160 |
| 1988+ | 0.100 | 0.229 | 0.392 | 0.217 | 0.063 | 134,970 |

<!-- src: tables/geometry_marginals_unobserved.md --> Each row sums to 1
across the five classes, per-event shares averaged within the era bucket.
The unobserved slice itself shrinks by twenty-fold from 1950–1987 to 1988+
(3.1M rows to 135K), which is the geometry-side face of the same coverage
collapse: after 1988 there is barely any unrecorded trajectory left to impute.
Distinct event-key coverage confirms the restriction is real — geometry's
trajectory rows cover 5,867,709 of 18,141,020 total events (about 32%),
consistent with a slice bounded to what was not directly recorded rather than
all batted-ball events. <!-- src: tables/geometry_marginals_unobserved.md -->
The pre-1950 GroundBall posterior mean, 0.320, sits close to the
observed-only ground share from the previous table (0.337) and far from the
deduced-inclusive share (0.680). That is the published, concrete face of the
MNAR under-imputation described in §5: Model E ships under the missing-at-random
flavor, trained on the recorded slice and scored on the unrecorded one, so it
inherits the recorded slice's selection bias onto the era and dimension where
nearly everything is imputed. §5's joint anchored sensitivity ribbon — not
this table — is where that gap is bounded: for this same pre-1950 slice, the
ribbon's full-anchor point raises the GroundBall share from this table's 0.320
MAR posterior mean to 0.582, with a $\pm 0.25$-nat perturbation band of
$[0.563, 0.598]$. <!-- src: tables/joint_ribbon_trajectory.md -->

### Run expectancy and the state-transition matrix, 2015 NL

`run_expectancy_summary` publishes a posterior mean plus 94% HDI for each of
the 24 base-out states. The two extremes at 0 outs:

| state | base_state | outs | re_value_mean | hdi_lower | hdi_upper |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0_0 (empty) | 0 | 0 | 0.451 | 0.443 | 0.459 |
| 0_7 (loaded) | 7 | 0 | 2.202 | 2.020 | 2.377 |

<!-- src: tables/re_matrix_2015_nl.md --> Bases loaded, nobody out, is worth
2.20 expected runs against 0.45 with the bases empty — a nearly fivefold
spread the posterior interval tracks tightly at the common empty-base state
(width 0.016) and more loosely at the rare loaded-base state (width 0.357),
uncertainty scaling with how often the state occurs rather than being fixed
by construction.

`state_transition_summary` publishes the full end-state distribution for each
start state. From bases-empty, 0 outs, all 25 possible end classes:

| end_class | prob_mean | hdi_lower | hdi_upper |
| --- | ---: | ---: | ---: |
| 1_0 (out, still empty) | 0.4865 | 0.4810 | 0.4919 |
| 0_0 (still empty, 0 out) | 0.3091 | 0.3041 | 0.3143 |
| 0_1 (reaches first) | 0.1649 | 0.1608 | 0.1690 |
| 0_2 (reaches second) | 0.0352 | 0.0333 | 0.0372 |
| 0_4 (reaches third) | 0.0043 | 0.0037 | 0.0050 |
| all other 20 classes | 0.0000 | 0.0000 | 0.0000 |

<!-- src: tables/transition_example.md --> `prob_mean` sums to exactly 1.0000
across all 25 rows. Every nonzero end class has outs $\geq$ 0, the start
state's out count — no outs-decreasing transition exists, which is the
reachability mask from §4 visible directly in published output rather than
asserted in the model code. The zero mass on every 2-out end class from a
0-out, bases-empty start is not the mask pinning an unreachable cell; a
double play needs a runner to force, and bases-empty admits none, so the
posterior correctly assigns it none.

### Park-factor extremes and honest uncertainty under sparse data

`park_factor_summary`'s highest and lowest park-seasons by posterior mean:

| rank | park_id | season | league | park_factor | hdi_lower | hdi_upper |
| --- | --- | ---: | --- | ---: | ---: | ---: |
| top 1 | DEN02 | 1996 | NL | 1.392 | 1.321 | 1.461 |
| top 2 | DEN02 | 1995 | NL | 1.391 | 1.310 | 1.470 |
| bottom 2 | LOS03 | 1963 | NL | 0.847 | 0.804 | 0.891 |
| bottom 1 | LOS03 | 1965 | NL | 0.841 | 0.801 | 0.880 |

<!-- src: tables/park_factor_extremes.md --> Coors Field (DEN02) holds the top
of the distribution across eight straight NL seasons, 1995–2002, peaking at
1.392 in 1996 — a 39% run inflation from altitude. The bottom is split between
Dodger Stadium in the early 1960s and Cleveland Municipal Stadium in the early
1940s, both around 0.84–0.85. <!-- src: tables/park_factor_extremes.md -->

Interval width tracks data density, not league identity, and the model says so
plainly rather than reporting a false-precision point estimate for thin
leagues:

| league | n_park_seasons | avg_hdi_width |
| --- | ---: | ---: |
| NAL | 12 | 0.1582 |
| FL | 16 | 0.1472 |
| NN2 | 19 | 0.1445 |
| NL | 1,288 | 0.0917 |
| AL | 1,301 | 0.0909 |

<!-- src: tables/park_factor_extremes.md --> The NAL's average HDI width
(0.158, 12 park-seasons) is 73% wider than the NL's (0.092, 1,288
park-seasons) and 74% wider than the AL's (0.091, 1,301). The Federal League
and the second Negro National League show the same pattern at similar
magnitude. This is the model correctly reporting what it does not know: with
an order of magnitude fewer park-seasons to pool across, the sparse leagues'
posteriors are and should be wider. A model that returned NAL park factors as
tight as the NL's would be manufacturing confidence the data does not support.

### `linear_weights_estimated` against the deterministic point surface

Joined on `(season, league, play)` for the 2015 NL, the Bayesian run-value
posterior and the deterministic `linear_weights` point value:

| play | play_category | deterministic | estimated_mean | hdi_low | hdi_high | diff |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| HomeRun | BATTING | 1.393 | 1.387 | 1.382 | 1.392 | -0.006 |
| Single | BATTING | 0.436 | 0.436 | 0.428 | 0.444 | 0.000 |
| StrikeOut | BATTING | -0.256 | -0.254 | -0.257 | -0.252 | 0.002 |
| OtherAdvanceOut | BASERUNNING | -0.441 | -0.409 | -0.418 | -0.400 | 0.032 |
| DoublePlay | BATTING | -0.769 | -0.771 | -0.787 | -0.756 | -0.002 |

<!-- src: tables/linear_weights_compare.md --> Across all 20 published play
types, most differences are at or under 0.01 runs, and the deterministic
value falls inside or almost against the 94% HDI everywhere <!-- src: bc/python_models/statistical/linear_weights_estimated.py -->; the exception is
`OtherAdvanceOut`, the one row where the deterministic point (-0.441) sits
just outside the estimated HDI's lower bound (-0.418) — the largest deviation
in either direction, at +0.032, and the only case worth flagging rather than
treating the two surfaces as interchangeable. <!-- src: tables/linear_weights_compare.md -->
The general agreement is itself informative: the Bayesian run-expectancy
propagation reproduces standard linear weights closely while additionally
carrying an interval, rather than replacing a trusted number with a different
one.

The comparison above draws on the currently published `linear_weights_estimated`
rows, whose 94% HDI propagates only Model G's run-expectancy random-effect
posterior draws through `runs_on_play + RE_end - RE_start`; it does not carry
finite-sample uncertainty from how many observations back a
`(season, league, play)` cell, so a three-event Negro-league cell and a
45,000-event modern cell report bands of comparable tightness under that
propagation. A corrected propagation closes this gap: `propagate_linear_weights_draws`
now draws a per-cell Jeffreys Dirichlet($n$+0.5) combination-weight vector — one
draw per RE-posterior draw — from the cell's own transition counts, so a
sparse cell's finite-sample noise widens its band automatically while a dense
cell is left unchanged to first order. <!-- src: tables/linear_weights_width_comparison.md -->
Across all 5,099 published cells, the corrected band is at least as wide as
the original on 99.57%; the median width ratio is 1.56 and the 90th
percentile is 4.19. The largest widenings land on the smallest-n Negro-league
cells — 1941 NN2 `Double` (8 events, ratio 59.8x), 1921 NN1 `Triple` (3
events, 33.6x), 1924 NN1 `Double` (5 events, 26.4x), 1924 ECL `Double` (7
events, 21.9x) — while the remaining 0.43% of cells (22 of 5,099) narrow
slightly, a Monte Carlo artifact of the per-draw Dirichlet realization rather
than a systematic failure of the correction. <!-- src: tables/linear_weights_width_comparison.md -->
This resolves the asymmetry the park-factor section above raises without
saying so directly: an interval that stays as tight on 3 events as on 45,000
is not honest uncertainty, and the corrected propagation now widens
`linear_weights_estimated`'s bands the same way sparse-league park factors
already widen. Promoting the corrected propagation to the published artifact
— currently a validated re-derivation against the same posterior and the same
production transition counts, not yet the live `linear_weights_estimated`
rows — is the one step remaining (§11).

### Assist counts and pitch summaries

`assist_count_distribution` places the probability mass over how many assists
a play produced, conditional on at least one. Bases empty versus a runner on
first, both at 0 outs, both `out_in_play`:

| base_state_start | 1 assist | 2 assists | 3 assists |
| ---: | ---: | ---: | ---: |
| 0 (empty) | 0.992 | 0.008 | 0.000 |
| 1 (runner on first) | 0.631 | 0.366 | 0.002 |

<!-- src: tables/assist_pitch_examples.md --> A bases-empty groundout is a
single assist 99% of the time; put a runner on first and multi-assist mass
jumps from 0.8% to 36.6%, the double-play states carrying almost all of the
model's 2-assist probability, exactly where a fan would expect it.

`pitch_summary_distribution` gives the final ball-strike count distribution
per result family. For strikeouts, 2015 NL, all 12 final-count classes are
published and the eight non-two-strike classes carry zero mass:

| final_count_class | balls | strikes | prob_mean |
| --- | ---: | ---: | ---: |
| b1_s2 | 1 | 2 | 0.341 |
| b2_s2 | 2 | 2 | 0.283 |
| b0_s2 | 0 | 2 | 0.226 |
| b3_s2 | 3 | 2 | 0.150 |

<!-- src: tables/assist_pitch_examples.md --> Every strikeout ends on two
strikes, a structural constraint of the game that the multinomial recovers
from data rather than having it hard-coded — the same reassurance
`docs/estimated-models.md`'s own worked example reports, reproduced here
against the live published table.

### Validation

The preceding tables assert calibration and honest uncertainty as narrative;
this subsection is the evidence. `just validate-gates` sweeps every published
artifact pointer through `validate_artifact` and reports validation status
plus fired findings. Of 23 registered gate targets, 18 pass, 1 fails, and 4
are missing; the four missing rows are the DL-proposal propensity gates
(`dl_proposal_trajectory`, `dl_proposal_location_side`,
`dl_proposal_location_edge`, `dl_proposal_location_depth`) — diagnostic
components of the deep-learning supplement in §6, not any of the twelve
published `main_models.*` tables above — so no published estimated table is
missing a gate report. <!-- src: tables/validation_gates.md --> The sweep is
a snapshot on this branch, resolved against the global published pointers
because the branch carries no branch-local override.
<!-- src: tables/validation_gates.md -->

Held-out expected calibration error (ECE) — absent from the earlier draft
this paper revises, and the metric a calibration claim requires — is now
computed and gated on all six of Model A's Bernoulli propensity dimensions:
0.0154 (`ball_handler_position`), 0.0094 (`location_side`), 0.0179
(`trajectory`), 0.0071 (`location_depth`), 0.0082 (`general_location`), 0.0054
(`location_edge`). Every dimension clears the 0.05 acceptance band by a wide
margin; `trajectory`, the dimension carrying the most pre-1988 missingness, is
also the least calibrated of the six, and `location_edge` the most.
<!-- src: artifacts/statistical/bayes/ball_handler_position_observedness/10k-v5-fullscore/validation/held_out_metrics.json -->
<!-- src: artifacts/statistical/bayes/location_side_observedness/10k-v5-fullscore/validation/held_out_metrics.json -->
<!-- src: artifacts/statistical/bayes/trajectory_observedness/10k-v5-fullscore/validation/held_out_metrics.json -->
<!-- src: artifacts/statistical/bayes/location_depth_observedness/10k-v5-fullscore/validation/held_out_metrics.json -->
<!-- src: artifacts/statistical/bayes/general_location_observedness/10k-v5-fullscore/validation/held_out_metrics.json -->
<!-- src: artifacts/statistical/bayes/location_edge_observedness/10k-v5-fullscore/validation/held_out_metrics.json -->

For `state_transition` and `run_expectancy` — the two aggregate surfaces whose
published intervals cover a cell mean or probability rather than a single
event — held-out HDI coverage is checked two ways against the same held-out
10% game fold, and the two ways disagree sharply enough to be worth showing
both. Parameter coverage asks whether the held-out empirical realization
lands inside the published 94% HDI of the fitted mean; on a corpus this dense
the fitted mean's own interval shrinks toward a point well before the held-out
cell's finite-sample noise does, so parameter coverage collapses on both
surfaces — 0.2123 for `state_transition`, 0.3235 for `run_expectancy` —
comparing a noisy realized frequency against an interval that was never built
to contain it. <!-- src: tables/validation_gates.md --> Posterior-predictive
coverage folds that finite-sample noise into the parameter uncertainty before
checking containment and is the gate's primary number: `state_transition`
covers at 0.9772, confirming the published transition surface is calibrated
once the comparison is to the right object; `run_expectancy` covers at 0.8774,
just under the 0.88 acceptance floor, and the gate fires this as a `warn`
rather than suppressing it — the artifact still passes overall, but its HDIs
under-cover held-out cell means by a small, disclosed margin.
<!-- src: tables/validation_gates.md --> The predictive check's parameter
layer is itself an approximation: neither surface persists per-draw class
probabilities, so the simulation reconstructs the parameter posterior per
class as an independent Normal(`prob_mean`, `prob_sd`) truncated to $[0, 1]$
and renormalized across the cell's classes, rather than drawing from the true
correlated posterior stored at fit time — adequate here because the
finite-sample layer dominates predictive width on the corpus's dense cells,
but a stated approximation rather than an exact reconstruction.
<!-- src: tables/validation_gates.md -->

`geometry_location_depth` is the one `failed` row, and the failure is worth
reading precisely rather than as a blanket miscalibration. Held-out top-1
accuracy, 0.5609, does not beat the majority-class baseline, 0.5611 —
`location_depth` is dominated by a single `Default` class, so arg-max is
close to useless as a discriminator on this dimension and the gate correctly
blocks on it. <!-- src: tables/validation_gates.md --> The calibrated shares
underneath are a separate question from arg-max utility: on the held-out
observed slice the predicted marginal class shares match the empirical shares
to a total-variation distance of 0.0051, and held-out log-loss is 1.0816 nats
against a marginal-entropy baseline of 1.1219 nats, a +0.04-nat lift — the
per-event probability vector carries real information beyond a constant
marginal predictor, even though that information does not reach the strength
needed to flip arg-max off `Default`. <!-- src: tables/validation_gates.md -->
`geometry_location_depth`'s published `expected_share` columns are fit for
the probabilistic, share-weighted consumption §9's publication policy
specifies as canonical; its top-1 label is not, and the block finding is
reporting exactly that distinction rather than a general calibration defect.


## What the record cannot tell you

A single-source play-by-play record bounds what coverage modeling can do, and part of a modeling program's job is to say exactly where that bound sits rather than paper over it with a model fit on the wrong estimand. Three model letters in this family — B, I, and K — are withheld, but they are withheld for three different reasons and the difference matters. Only Model B carries a genuine identification argument: a formal statement, defended below, about what the likelihood can and cannot learn. Model I is withheld because a specific input its correct specification needs does not exist anywhere in the source data — a data-availability limitation, not a proof that the estimand resists identification in principle. Model K is withheld because it has not been built in this pass — unfinished scope, not a claim about the record at all. Presenting the three at the same rigor would overstate the weakest of them, so each subsection below states its claim type before making it.

### Model B — contact-label confusion: the data are uninformative about Ω

This is an identification claim, not a report that confusion happens to be rare. Model B was specified as a scorer confusion model: a latent true contact class `Z_i` generating a recorded label `L_i` through a scorer/decade confusion matrix `Ω`. Identifying `Ω` from data needs one of two things — repeated, independent labels for the same event, or outcome evidence that is itself independent of the recorded label and can disagree with it. Retrosheet-era play-by-play has one scorer per event (scorer is a game-level attribute in `game_scorekeeping`), so the first route is closed: there is no second, independently drawn label to compare against the first.

The obvious candidate for that second label — the "deduced" batted-ball class computed by `calc_batted_ball_type.sql` — is not independent either. When the recorded trajectory is known, the deduction rule sets the deduced broad class equal to the recorded broad class by construction, so comparing recorded against deduced on that majority of events measures the deduction rule, not scorer behavior; the comparison carries zero independent information about `Ω` there. The only place independent evidence exists is where an outcome, not a recode of `L_i`, can adjudicate the class on its own — home runs, outfield-depth putouts, unassisted putouts. Restricted to that outcome-anchored evidence, the recorded broad class and the deduced broad class disagree on 702 of 6.0M recorded-known, non-bunt batted-ball events, 0.0117% <!-- src: memory:model_b_contact_blocked.md -->, all one-directional (GroundBall → AirBall, never the reverse), with no era or scorer structure.

The formal conclusion follows from the shape of the evidence, not from the size of the disagreement rate. Over the overwhelming majority of the record, the likelihood for `Ω` is flat — unmoved by the data, because the only comparison available there is an identity by construction, not a measurement. Over the 0.0117% outcome-anchored sliver, the 702 disagreements do carry some information, but not enough to resolve a matrix indexed by scorer and decade; a likelihood that is flat almost everywhere and thinly informative on a sliver two orders of magnitude too small to identify a structured matrix is fitting a prior, not learning from data. The precise statement is that the data are uninformative about `Ω`, not that `Ω` is unidentifiable outright — the anchored sliver does move the posterior, just not by enough to matter — and it is a different statement again from "confusion is rare," which describes `Ω`'s value rather than what the data can say about it. Unblocking Model B needs a second, genuinely independent contact-label source per event — a different feed's trajectory call — which means ingesting a second parser source, out of scope for this repository.

### Model I — fielder responsibility: a data-availability limitation

This is not a claim that responsibility is unidentifiable in principle. It is a claim about what today's source data contains: Model I's correct specification needs an input that does not exist in the record, and if that input existed the identification picture would look nothing like Model B's — there would be no confusion-matrix-style argument to make at all.

Model I's estimand is an analytical opportunity, not a recorded fact: `P(responsible = k | geometry, alignment, …)`, who should have had a play given where the ball went, marginal over who actually got to it — explicitly distinct from the ball handler, the fielder who did get to it <!-- src: notes/data-coverage-implementation/implementation-review.md -->. A first implementation shipped anyway, trained on `ball_handler_position` restricted to range positions, which makes it Model D with a narrower vocabulary rather than a responsibility model at all. The confound is empirical: 28% of the training slice is hits, and on hits the "handler" is just whichever fielder retrieved the ball after it got past the defense — 89% of hit-handlers are outfielders <!-- src: notes/data-coverage-implementation/implementation-review.md -->. A model trained on that label can only relearn who touched the ball, which Model D already publishes; it cannot answer who should have.

The design that would actually estimate responsibility decomposes it into a geometry term the pipeline already has — Model E's location posterior — and a term it does not: `π(k | location, alignment)`, a Dirichlet zone-responsibility kernel giving the positional probability of coverage for a location bin under a given defensive alignment <!-- src: notes/data-coverage-implementation/responsibility-zone-design.md -->. No positioning prior of that shape exists anywhere in the source data; the handler is the only position-valued label a batted ball carries. That is the whole limitation: a positioning-prior source — tracking-era defensive alignment logs, for instance — would resolve `π(k | location, alignment)` directly and turn Model I into a straightforward fit. It is withheld because that source is not in this record, not because the estimand resists identification. The prototype was parked and removed, the `responsibility_artifact_id` column stays as a reserved NULL pointer, and nothing named responsibility ships until the zone kernel exists <!-- src: notes/data-coverage-implementation/responsibility-zone-design.md -->.

### Model K — shift propensity: descoped, not a claim

Model K makes no identification claim and no data-availability claim. It is unfinished scope: designed, not built.

Shift propensity was scoped as its own first-class model — `P(shift | player, batter_hand, defending_team, count, outs, base_state)` — rather than folded into a categorical `alignment_regime` covariate, on the reasoning that defensive shifting is too consequential to bucket into four eras and that post-2015 event data and post-2009 pitch data are rich enough to support a dedicated fit <!-- src: memory:data_coverage_shift_model.md -->. Nothing in the record prevents fitting it. Model K was designed to feed Model I's alignment input, and would plausibly feed geometry and run-value models as well, but the fit was not carried out in this pass. Every model that would consume its posterior — Model I above, and Model E's alignment-regime fixed effect — instead falls back to the era-normal alignment prior it was always meant to use for eras and cells where a shift model has no support; that fallback is the entire operating mode today, not a degraded corner case. K appears in this section for publication-transparency inventory — it records that the shift-propensity posterior does not exist yet — not because it belongs beside a genuine identification or data-availability limit.

### What the three share, and what they don't

These three withheld model letters do not share one failure mode. B's is the narrowest and strongest claim: given a single scorer per event and no independent second label, the likelihood cannot move on `Ω` over nearly the whole record — a formal identification limit. I's is a data-availability limitation: the input a correct model needs is absent from the source, not absent in principle, and a different data source would resolve it outright. K's is neither of those — it is scope not yet completed, and grouping it with B and I asserts nothing about the record at all.

What the three do share is a publication discipline, not a common statistical argument. A model that cannot learn what it claims to (B), a model that needs an input the record does not supply (I), or a model that has not yet been built (K) does not get a hedge column in a published table. Each is parked, its name reserved, and its status — a blocking condition for B and I, remaining work for K — written down until the situation changes.


## Publication policy

Every `main_models.*` table carries one of five publication tiers, and the tier
is a property of the table, assigned centrally rather than inferred by a
consumer from column names. `PublicationTier` enumerates `official`,
`deterministic`, `estimated`, `synthetic`, and `withheld`, and a single
`PUBLICATION_TIERS` registry maps each published model name to its tier.
<!-- src: notes/data-coverage-implementation/phase5-conventions.md --> The
tiers read, in the rollout design's own words: `official` is an authoritative
source value at the target grain; `deterministic` is a rule-based value
computed from canonical inputs; `estimated` is a posterior expected value,
probability, or interval; `synthetic` is a generated row or value drawn from
aggregate-only or synthetic inputs; `withheld` is a quantity that was
specified but did not clear validation or identification and is not published
at all. <!-- src: notes/data-coverage-implementation/06-rollout-and-validation.md -->
Models B, I, and K — discussed in the previous section — sit in `withheld`
today: designed, in two cases prototyped, and not published, because no
amount of additional modeling substitutes for an identifying source of
variation the record does not contain.

The `estimated` tier is the one this paper's results section draws from, and
every row in it carries the same eight-column contract: `artifact_id` (the
fit that produced the row), `model_name`, `model_version`, `source_snapshot_id`,
`method` (`hierarchical_logistic`, `hierarchical_bayes_softmax`, or
`hierarchical_bayes_nb`), `observed_status` (the constant `estimated`, a
namespace marker), `confidence_status` (the fit's validation status as of the
run that produced the row — see below), and `weak_identification_flag` — set
from the fit's own diagnostics when a convergent posterior is nonetheless
weakly identified in some slice: low group-level effective sample size, a
r-hat above the comfort band, any divergence, or a non-finite diagnostic.
<!-- src: notes/data-coverage-implementation/phase5-conventions.md --> <!-- src: docs/estimated-models.md -->
Diagnostic columns such as ESS and r-hat are deliberately absent from the
published rows — they belong in the validation reports that gate publication,
not in the table a downstream consumer joins against.
<!-- src: notes/data-coverage-implementation/phase5-conventions.md -->

`confidence_status` is stamped once, at fit time, from whatever gate suite was
current when the artifact was produced, and it does not move on its own after
that. Every populated table's rows read `exploratory` today because they were
all stamped before the gate suite in §7's Validation subsection existed.
<!-- src: docs/estimated-models.md --> That gate suite is now a runnable
check rather than a manual phase-6 checklist — `just validate-gates` sweeps
every published pointer and reports pass/fail/missing per artifact, and 18 of
23 registered targets pass today. <!-- src: tables/validation_gates.md -->
Passing the sweep is necessary but not sufficient for a row to say `passed`:
the stamp is written by the publish path that produced the row, not
retroactively by a later gate run, so moving a table's `confidence_status`
from `exploratory` to `passed` requires re-publishing it — restating the
`@model` under the current gate suite so the stamp is written fresh. That
re-publication has not happened for any table in this paper; the gate results
in §7 describe what a restated run would be stamped with, not what the
published rows are stamped with today.

Posteriors are published as distributions, and this is enforced structurally
rather than by convention. The event-level imputation tables — geometry,
ball-handler, fielding credit — ship a `class_label` and an `expected_share`
per class, with shares summing to one over an event's class set; none of them
persists a single predicted label. A consumer who wants a top class computes
it at query time, typically with `QUALIFY ROW_NUMBER() OVER (PARTITION BY
event_key ORDER BY expected_share DESC) = 1`, the pattern `docs/estimated-models.md`
documents for `imputed_batted_ball_geometry`.
<!-- src: docs/estimated-models.md --> An argmax convenience column may be
emitted for inspection, but it is never the canonical representation any
downstream consumer is meant to read, and the shipped coverage tables do not
persist one — the full posterior is the product.
<!-- src: notes/data-coverage-implementation/03-hierarchical-models.md -->

Estimated surfaces never overwrite a deterministic legacy table. Where a
legacy point surface already exists — `linear_weights`, `park_factors`,
`run_expectancy_matrix` — its columns are left untouched at the
`deterministic` tier, and the Bayesian counterpart is published as a sibling
model under an explicit `_estimated` suffix, carrying the point estimate and
its uncertainty side by side.
<!-- src: notes/data-coverage-implementation/phase5-conventions.md --> The two
are not merged into one view, because their grains can differ — `park_factor_summary`
carries an `outcome` dimension the deterministic `park_factors` does not — so
the compatibility rule is "keep the legacy table and add a sibling," not
"join and replace." <!-- src: notes/data-coverage-implementation/phase5-conventions.md -->
An official-only consumer reads the legacy table and notices nothing; an
estimated-aware consumer opts into the sibling and its HDI.

The last rule is about what happens to a quantity that is genuinely uncertain
but not unidentifiable. Fielding credit on a no-box-score event is always
published, never withheld for insufficient confidence — the design commits to
a confidence column carrying that uncertainty rather than a hard inclusion
cutoff that would silently drop the sparse, hard cases from the record.
<!-- src: notes/data-coverage-implementation/statistical-modeling-coverage-design.md -->
That is the difference between `estimated` and `withheld` in practice:
`estimated` is the tier for "we don't know for certain, and we can say how
much we don't know"; `withheld` is reserved for "no model of this quantity is
identified by the data at hand," and it is used sparingly — three model
letters out of eleven attempted, each with its blocking condition written
down rather than papered over with a wide interval.


## Related work

The framing rests on the missing-data literature. Rubin (1976) established the
distinction between missing completely at random, missing at random, and missing
not at random, and the conditions under which the missingness mechanism can be
ignored for likelihood and Bayesian inference. Our observation models are a
direct application: the default assumption is missingness at random conditional
on scorer, source, result, and era, and the contribution is to make that
conditioning set explicit rather than assumed. Little and Rubin (2019) give the
methodology this paper follows for handling incomplete data through models rather
than deletion or single imputation.

Where ignorability fails, two traditions divide the territory, and this paper
draws on both. The first is the selection-model tradition. Heckman (1976, 1979)
formalized sample selection as a two-equation system — an outcome process and a
selection process whose errors are correlated — and showed that selection bias
is a specification error correctable by modeling the selection equation jointly
with the outcome. §5's derivation is a selection model in exactly this sense: a
latent class is drawn, then recorded with probability s(c, x), and Bayes' rule
turns the selection process into a per-class reweight of the fitted
distribution — the offset δ_c is the per-class selection log-odds, a
selection-model parameter acting on the recorded class before the softmax
normalizes. The difference from Heckman's program is what we ask of the
parameter. Heckman buys identification with parametric structure — joint
normality of the two equations' errors, or an exclusion restriction that moves
selection without moving the outcome. Our record offers no credible exclusion
restriction — no covariate moves a 1930s scorer's recording behavior without
also proxying the play itself — and we decline the distributional route: δ_c is
treated as unidentified from the observed slice, estimated only where an
external partial-truth anchor exists, and otherwise swept, not solved for.

That sweep places the paper in the second tradition: pattern-mixture models and
delta-adjustment sensitivity analysis. Little (1993) specified the distribution
of missing values directly through identifying restrictions rather than a
mechanism, and observed that such models are chronically underidentified —
there is no information in the observed data about the distribution of the
missing stratum, so the honest output is inference across a range of
restrictions rather than a point. Scharfstein, Rotnitzky, and Robins (1999)
made the same move inside the selection formulation: fix a nonidentified
selection-bias parameter at each value in a plausible range, estimate under
each, and report the trajectory — sensitivity analysis in place of an
untestable assumption. That template became standard practice in the
clinical-trials missing-data literature as delta adjustment and tipping-point
analysis, where a fixed offset δ is added to values imputed under a
missing-at-random fit and swept until the conclusion changes; Molenberghs and
Kenward (2007) treat the framework at book length, and Cro, Morris, Kenward,
and Carpenter (2020) give the current practical guide to controlled multiple
imputation in this style. The published object of §5 is this device
transplanted: δ_c is a delta adjustment on the class logits, the
missing-at-random fit is the δ = 0 point, and the ribbon is the tipping-point
trajectory, reported per class and jointly along an anchored direction. What
the paper adds to the template is the anchor itself — a deduced-trajectory
partial-truth slice internal to the record that yields a per-era estimate of
δ_c, so the sweep is centered by evidence rather than convention — and the
masked backtests that test the offset's functional form against selection
processes it does not nest. The structure of the treatment — a selection-model
parameterization with pattern-mixture-style sensitivity reporting — reflects
the standard observation that the two factorizations describe the same joint
distribution and can be mixed rather than chosen between.

The observation process studied here is informative nonresponse in the survey
statistician's sense: the probability an item is recorded depends on the value
the item would have taken. Rubin (1977) formalized subjective assumptions about
nonrespondents in exactly the spirit adopted here — a parameter the data cannot
estimate, elicited and varied rather than ignored. The survey-methodology
literature collected in Groves, Dillman, Eltinge, and Little (2002) treats
nonresponse as a process with its own covariates and propensities, which is
Model A's stance toward scorers: the observation-propensity surfaces are
response-propensity models with scorers in the role of respondents, and the
event-dimension grain of §3's taxonomy is item nonresponse rather than unit
nonresponse — one event can respond on batting and not on trajectory.

The fitting methodology follows the applied Bayesian workflow. Gelman et al.
(2013) is the reference for the hierarchical models, partial pooling, prior and
posterior predictive checking, and the practice of treating a model as a
hypothesis to be checked against held-out data. Betancourt and Girolami (2013)
document the pathologies that hierarchical models present to Hamiltonian Monte
Carlo and show that a non-centered parameterization restores efficient sampling
when data are sparse — the regime of most of our early-era cells — which is why
every model in the family is written non-centered.

In sports analytics, the play-by-play data itself comes from the Retrosheet
archive, and the closest methodological predecessor is openWAR (Baumer, Jensen,
and Matthews, 2015), which built an open, reproducible player-value system and
carried uncertainty through to its estimates rather than reporting point values —
the same commitment this paper makes for coverage surfaces. Marchi and Albert
(2014) is the standard applied treatment of baseball data analysis in this
register. Measurement problems in the baseball record itself have drawn less
attention, but not none. Kalist and Spurr (2006) document a home-team bias in
official scorers' hit-versus-error calls — direct evidence that the human
observation layer this paper models imprints itself on the record, on a field
this paper treats as official rather than corrects. Acharya et al. (2008)
treat park-factor estimation as a small-sample shrinkage problem, the
instability that Model F's hierarchical pooling addresses. A peer-reviewed
literature on measurement error in modern tracking data has not yet formed,
and we cite none. Our work differs from
all of these in target: rather than estimating player value or correcting a
single statistic, we model the process that decided which event details were
recorded at all, and publish the resulting uncertainty as the estimand.

### References

Acharya, R. A., Ahmed, A. J., D'Amour, A. N., Lu, H., Morris, C. N., Oglevee,
B. D., Peterson, A. W., & Swift, R. N. (2008). Improving Major League Baseball
park factor estimates. *Journal of Quantitative Analysis in Sports*, 4(2),
Article 4.

Baumer, B. S., Jensen, S. T., & Matthews, G. J. (2015). openWAR: An open source
system for evaluating overall player performance in Major League Baseball.
*Journal of Quantitative Analysis in Sports*, 11(2), 69–84.

Betancourt, M., & Girolami, M. (2013). Hamiltonian Monte Carlo for hierarchical
models. arXiv:1312.0906.

Cro, S., Morris, T. P., Kenward, M. G., & Carpenter, J. R. (2020). Sensitivity
analysis for clinical trials with missing continuous outcome data using
controlled multiple imputation: A practical guide. *Statistics in Medicine*,
39(21), 2815–2842.

Gelman, A., Carlin, J. B., Stern, H. S., Dunson, D. B., Vehtari, A., & Rubin,
D. B. (2013). *Bayesian Data Analysis* (3rd ed.). Chapman & Hall/CRC.

Groves, R. M., Dillman, D. A., Eltinge, J. L., & Little, R. J. A. (Eds.)
(2002). *Survey Nonresponse*. Wiley.

Heckman, J. J. (1976). The common structure of statistical models of
truncation, sample selection and limited dependent variables and a simple
estimator for such models. *Annals of Economic and Social Measurement*, 5(4),
475–492.

Heckman, J. J. (1979). Sample selection bias as a specification error.
*Econometrica*, 47(1), 153–161.

Kalist, D. E., & Spurr, S. J. (2006). Baseball errors. *Journal of
Quantitative Analysis in Sports*, 2(4), Article 3.

Little, R. J. A. (1993). Pattern-mixture models for multivariate incomplete
data. *Journal of the American Statistical Association*, 88(421), 125–134.

Little, R. J. A., & Rubin, D. B. (2019). *Statistical Analysis with Missing
Data* (3rd ed.). Wiley.

Marchi, M., & Albert, J. (2014). *Analyzing Baseball Data with R*. Chapman &
Hall/CRC.

Molenberghs, G., & Kenward, M. G. (2007). *Missing Data in Clinical Studies*.
Wiley.

Rubin, D. B. (1976). Inference and missing data. *Biometrika*, 63(3), 581–592.

Rubin, D. B. (1977). Formalizing subjective notions about the effect of
nonrespondents in sample surveys. *Journal of the American Statistical
Association*, 72(359), 538–543.

Scharfstein, D. O., Rotnitzky, A., & Robins, J. M. (1999). Adjusting for
nonignorable drop-out using semiparametric nonresponse models. *Journal of the
American Statistical Association*, 94(448), 1096–1120.


## Limitations and open problems

This section lists what is still open, named against the specific artifact it blocks and the specific condition that would unblock it. None of these are hedges against unknown risk; each is a concrete, already-diagnosed gap. Two items resolved since the paper's previous draft are stated here as delivered, with the narrower gap that remains after each fix.

**The MNAR offset `δ_c` is anchored for GroundBall; the other trajectory classes and the unknown-status residual are not.** §5 now publishes a per-era offset anchored to data rather than assumed: the derived slice (fielding-string deduction, GroundBall-only) contrasted against the observed slice yields raw offsets of +1.241 nats (pre-1950), +0.917 (1950-1987), and +0.835 (1988+), feeding a joint sensitivity ribbon alongside the published MAR default. <!-- src: tables/joint_ribbon_trajectory.md --> This closes the gap the previous draft flagged as open for that one class. What remains open: the four non-GroundBall trajectory classes (Fly, LineDrive, PopUp, Bunt) have no analogous deduction path, so their selection offsets are unidentified — the ribbon moves them only through the softmax coupling to the anchored GroundBall direction and the $\pm0.25$-nat perturbation, never from data. Events whose `observed_status` is `unknown_code` or `missing`, rather than `derived`, sit outside the anchor's partial-truth guarantee entirely. Unblock condition: an independent partial-truth source for the non-ground classes — a second scorer stream, a tracking-era backfill, or a comparable deduction rule — to extend the anchor beyond its current one-dimensional reach; and a disposition for the `unknown_code`/`missing` residual, which no anchor currently characterizes.

**`linear_weights_estimated`'s finite-sample band is corrected in code but not yet in the published artifact.** The propagation now draws a per-cell Jeffreys Dirichlet($n$+0.5) combination-weight vector from each cell's own transition counts — one draw per RE-posterior draw — instead of fixing them at the observed count, so sparse cells widen automatically while dense cells are unchanged to first order: median band-width ratio 1.56 against the original propagation, p90 4.19, up to 59.8x on the sparsest Negro-league cells (as few as 3 events), and only 22 of 5,099 cells (0.43%) narrow, a Monte Carlo artifact of the per-draw realization rather than a systematic failure. <!-- src: tables/linear_weights_width_comparison.md --> The mechanism is built and passes its width-monotonicity acceptance check across all published cells. What remains: the live `main_models.linear_weights_estimated` rows still carry the original, narrower bands — the corrected propagation has been run as a validated re-derivation against the same RE posterior and the same production transition counts, confirmed identical in every respect but weight uncertainty, but has not been promoted into the published artifact. Unblock condition: restate `linear_weights_estimated` so the published HDIs are produced by the corrected (default) Dirichlet propagation rather than the fixed-count one.

**`run_expectancy`'s held-out posterior-predictive coverage sits below the acceptance floor.** Checked against the same held-out 10% game fold used throughout §7, `run_expectancy`'s posterior-predictive HDI coverage is 0.8774, under the 0.88 floor for a nominal 94% interval; the gate fires `predictive_coverage_out_of_band` at `warn`, and the artifact still passes overall. <!-- src: tables/validation_gates.md --> The published HDIs mildly under-cover held-out cell means even after folding in finite-sample sampling noise. `state_transition`'s equivalent check covers at 0.9772, so the two aggregate surfaces are not symmetric on this measure, and the gap is disclosed here rather than smoothed over in §7's presentation. Unblock condition: revisit the run-expectancy submodel's dispersion assumption or era-regime random-effect scale and re-check predictive coverage; the 0.8774 figure stands as reported until then.

**The posterior-predictive coverage check's parameter layer is a reconstruction, not the stored posterior.** Neither `state_transition` nor `run_expectancy` persists per-draw class probabilities in its published summary export, so the predictive-coverage simulation approximates each cell's parameter posterior as an independent Normal(`prob_mean`, `prob_sd`) truncated to $[0, 1]$ and renormalized across classes, rather than drawing from the true correlated posterior (Dirichlet-like, for the multinomial surface) captured at fit time. <!-- src: tables/validation_gates.md --> This is adequate where the finite-sample multinomial or sampling-noise layer dominates predictive width, which holds for both surfaces on this corpus's dense cells, but it is a stated approximation and not an exact reconstruction. Unblock condition: persist a per-draw class-probability export, or a compact sufficient summary of the posterior's correlation structure, for at least a validation-scoped sample of cells, so the predictive check can draw from the stored posterior directly.

**`confidence_status` has not moved off `exploratory` for any published table.** The stamp is written once, at publish time, from the manifest's `validation_status`, and does not update retroactively when a later gate run passes — every populated `main_models.*` estimated table was stamped before the gate suite in §7's Validation subsection existed. <!-- src: bc/python_models/statistical/bayes/manifest_ingest.py --> 18 of 23 registered gate targets pass today, but moving a table's `confidence_status` from `exploratory` to `passed` requires re-publishing it under the current gate suite, and that re-publication has not happened for any table in this paper (§9). Unblock condition: restate each passing model's `@model` so its manifest is re-validated and its stamp is written fresh under the gate suite that now exists.

**Model H (advancement) is schema-only.** `model_input_advancement.sql` does not yet emit the dependent variable `advancement_class` or a `time_forward_fold` column, so the advancement DL and Bayes specs no longer register on import <!-- src: notes/followups.md -->. Unblock condition: close the SQL gap upstream (apply the same seven-class advancement derivation the pretrain heads already use for `r1/r2/r3_advancement`), then re-register the specs and restore the target's test suite.

**Model C's error-credit arm is code-complete but data-blocked, and its double-play submodel has no truth column.** The error model (`models/error_credit.py`) fits fine on the 356K attributed error events but produces an empty production export: every one of the 16.3M error rows in `model_input_fielding_credit` already carries `unknown_credit_need = 0` — the upstream parser emits no unknown-error allocation signal, so there is nothing left to impute <!-- src: notes/followups.md -->. Separately, the double-play submodel has no DP truth to train against: `double_plays` exists upstream in `event_player_fielding_stats` but is not surfaced into the modeling dataset, and there is no `outs_on_play` column to gate "two outs recorded on this play" <!-- src: notes/followups.md -->. Unblock condition for errors: an upstream parser or SQL change that emits an unknown-error allocation need, or a team-game box-residual anchor built from `aggregate_residual_errors`. Unblock condition for double plays: surface `double_plays` and an `outs_on_play` column into `model_input_fielding_credit`.

**Model G's context-neutral linear weights are deferred on an estimand ambiguity, not a modeling gap.** The spec's headline context-neutral `P_LW` integrates the end-state value against the modeled marginal transition `P_LW(end | start)` rather than the realized end state. That estimand is ambiguous at the per-play-type grain, because `P_LW` is keyed on the start state alone and so cannot distinguish play types that share a start state <!-- src: notes/followups.md -->. The Markov submodel that would feed `P_LW` is built; only the estimand decision is open. Unblock condition: pin the context-neutral estimand precisely (what varies within a shared start state and how it enters the integral), or confirm the standard marginal linear-weights surface — already built, published, and the subject of the corrected finite-sample band above — is the intended deliverable and drop the context-neutral variant from scope.

**Model F's AR(1) park chains never reset at park reconfigurations.** `park_episode_status` is 100% NULL in both dataset artifacts, so there is no stable `park_episode_id` to key the persistence chain on; it currently keys on `(park, league)` alone, which means a park's factor history runs continuously across a mid-history reconfiguration that should have broken the chain <!-- src: notes/followups.md -->. Unblock condition: the upstream park-history dimension populates a non-null `park_episode_id`, and `model_input_park_factors.sql` selects it so the AR(1) prep can key on `(park_episode_id, league)`.

**Model J's cell-grain summary model has a degenerate source-partial-pooling level.** `model_input_pitch_summary` is single-source — `source_family` and `source_type` each take exactly one distinct value in the modeling dataset — so a partial-pooling level over source is mathematically present in the model but carries no information <!-- src: notes/followups.md -->. Unblock condition: a dataset or SQL change that surfaces more than one source family into the pitch-summary modeling dataset.


## Reproducibility

Most tables in this paper are queries against `bc.db`, a DuckDB database built
entirely through SQLMesh — `MODEL` blocks and Python `@model` decorators, no
ad hoc writes — from 45 source parquet files and the coverage pipeline's
`model_input_*` datasets. <!-- src: CLAUDE.md --> <!-- src: .claude/rules/sqlmesh.md -->
The queries and outputs behind every number are checked in alongside the
prose: `notes/paper/queries/<name>.sql` is the exact query, and
`notes/paper/tables/<name>.md` is its output against the published database,
for each of the eleven SQL-query-backed tables cited in this section. A
further set of tables — the ones behind §7's Validation subsection — are
recipe-driven rather than single-query outputs, and each carries its own
regeneration command inline in `notes/paper/tables/<name>.md`: `just
validate-gates` (read-only; sweeps every published artifact pointer through
`validate_artifact` and reports the gate-status table, the state-transition
and run-expectancy predictive-coverage numbers, and the
`geometry_location_depth` disposition), `just mnar-backtest --mask-design
{w_class_intensity,covariate_joint,scorer_blocked,era_graded}` (the masked
backtest across the four robustness designs), `just sensitivity-ribbon
--joint <anchor-run-dir>` (the joint anchored sensitivity ribbon), and the
anchor driver that feeds it, `uv run --group stats python
scripts/mnar_anchor.py --run-id <id>` (reads `bc.db` read-only and writes the
per-era GroundBall selection offset). None of these four writes to `bc.db` or
mutates any published artifact; they are read-only sweeps and re-derivations
against the current published pointers, and together with the queries above
they reproduce every number in §7's Validation subsection.

Provenance runs deeper than the query. Every `estimated`-tier row carries an
`artifact_id` that resolves to an `ArtifactManifest` — a JSON document
recording the source snapshot, package versions, and, for Bayesian fits, a
`sampler_config` with draw count, tuning steps, chain count, target-accept
rate, maximum tree depth, backend, and the integer random seed the fit ran
on. <!-- src: bc/python_models/statistical/schemas.py --> Fits are
reproducible by seed and sampler settings, not by re-running with whatever
configuration happens to be current. This provenance layer is deliberately
not MLflow: the deep-learning training pipeline under `bc/python_models/ml/`
does use MLflow, but the statistical and Bayesian layer that publishes the
tables in this paper excludes it by construction — an artifact-id directory
plus `manifest.json` plus structured logs is the whole backend, and a test
asserts that importing the Bayes/deep-supplement modules never pulls MLflow
into the process. <!-- src: bc/tests/statistical/deep/test_no_mlflow_import.py -->

`bc.db` itself is not the distribution artifact end users query. The publish
pipeline copies `main_models.*` and `main_seeds.*` into a DuckLake catalog,
uploads catalog and data to object storage, and mirrors the result as a
standalone `bc_remote.db` plus per-table parquet — the artifact a downstream
consumer without SQLMesh actually reads. <!-- src: scripts/CLAUDE.md -->
Reproducing a published number end to end means three things: the SQLMesh
plan that built the table, the manifest the `artifact_id` points to, and the
query in `notes/paper/queries/` that reads it back — all three are version
controlled, and none of them is this paper's private copy of the truth.


