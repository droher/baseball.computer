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
Publication is gated on convergence and on held-out predictive lift over a
baseline; held-out calibration error and posterior-predictive interval coverage
are reported beside the gate as diagnostics.

Three findings organize the paper. First, trajectory recording before 1988 is
missing not at random: among pre-1950 batted balls whose trajectory the scorer
did not write, 763,993 are ground balls the fielding string alone identifies —
more than the whole recorded slice — putting a floor of 0.29 under the
unrecorded ground-ball share, while the missing-at-random fit scores those known
ground balls at 0.32. A masked backtest shows the natural learned correction — a
coefficient on observation propensity — is inert while passing every convergence
diagnostic; the correct form is a fixed per-class selection offset the observed
slice cannot identify, published as an assumed sensitivity band that the floor
constrains from below, not as a point. An earlier revision's data-anchored offset
is withdrawn as an identity of the observed slice. Second, shared
entity-embedding pretraining multiplies player-effect permutation importance in
the trajectory supplement by roughly 6× under a cross-fitting contract that
caught one real leak, and the deep proposal beats a class-prior baseline on
held-out log-loss for every dimension it feeds; a defect disclosed in this
revision — three location dimensions whose production rows carried no deep
prediction and were shifted by its absence — is corrected by publishing their
deep-free fits. Third, several natural estimands — scorer label confusion,
fielder responsibility, shift propensity — cannot be learned from a
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
systematic. Before 1950 the scorer wrote a trajectory on 712,469 batted balls
and omitted it on 2.6 million; of the omitted, 763,993 are ground balls whose
class the fielding string alone recovers — a ball fielded by the shortstop and
thrown to first — more events than the whole recorded slice, and a floor of
0.29 under the unrecorded ground-ball share before a single genuinely unknown
event is counted <!-- src: notes/paper/tables/groundball_mnar.md -->. A model
trained on the recorded slice assigns those known ground balls a mean
ground-ball probability of 0.32 <!-- src: notes/paper/tables/groundball_mnar.md -->.
The pattern holds through 1987 and collapses after 1988, when the unrecorded
slice shrinks to 135 thousand events against 4.8 million recorded
<!-- src: notes/paper/tables/groundball_mnar.md -->. Scorers before 1988 omitted
the trajectory on routine grounders the fielding string made redundant, and a
model that treats the recorded trajectories as a random sample of all
trajectories misstates the unrecorded class mix by an amount the recorded data
cannot bound from above, in exactly the era where nearly everything must be
imputed — the unobserved slice is 78 to 95 percent of all events before 1988
across the geometry and location dimensions
<!-- src: notes/data-coverage-implementation/implementation-review.md -->.

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
statistical contract — non-centered partial pooling, game-grouped holdouts, a
per-model deep-learning-covariate ablation, and acceptance gates that pair
convergence diagnostics with held-out predictive lift over a baseline, with
held-out calibration and posterior-predictive interval coverage reported
alongside as diagnostics <!-- src: bc/python_models/statistical/validate.py -->. The family spans
observation propensity (Model A, published as `scorer_observation_propensities`),
fielding credit (Model C, `imputed_fielding_credit`), ball handler (Model D,
`imputed_ball_handler_probabilities`), batted-ball geometry (Model E,
`imputed_batted_ball_geometry`), park factors (Model F, `park_factor_summary`),
run expectancy and base-out transitions (Model G, `run_expectancy_summary` and
`state_transition_summary`), and pitch coverage and summary (Model J)
<!-- src: docs/estimated-models.md -->.

Third, a treatment of missingness that is not at random. For the trajectory and
location dimensions, whether a label was recorded is correlated with what the
label would have been. We report a learned correction that is inert — a
propensity coefficient that converges cleanly and leaves the masked-slice error
unchanged — a fixed per-class selection offset δ_c that the observed-only
likelihood cannot identify, a hard lower bound on the unrecorded ground-ball
share from the deduced-trajectory slice together with a known-truth diagnostic
of the missing-at-random fit on that slice, and a sensitivity ribbon over the
offset published as an assumed band that the bound constrains from below
<!-- src: bc/python_models/statistical/mnar_anchor.py, bc/python_models/statistical/sensitivity.py -->.
An earlier revision's data-anchored offset is withdrawn as an identity of the
observed slice. The offset mechanism is checked in a masked backtest whose one
informative design — selection on class and a covariate the model conditions
on — shows the per-class offset recovering about half the bias
<!-- src: notes/paper/tables/mnar_backtest_robustness.md -->.

Fourth, a deep-learning supplement that supplies proposal distributions and
shared entity embeddings to the Bayesian layer, never published facts. Its term
enters the softmax as γ · log p̃^dl_{i,c} with one scalar γ ~ N(0, 0.5) whose
posterior the data dominate, behind leakage gates and a cross-fitting contract;
argmax-to-fact is banned. A defect in how the term handled a missing prediction
shifted three published location surfaces and is disclosed and corrected in
this revision.

Fifth, a publication policy. Estimated surfaces ship as posteriors with an
eight-column provenance contract — artifact_id, model_name, model_version,
source_snapshot_id, method, observed_status, confidence_status, and
weak_identification_flag <!-- src: notes/paper/OUTLINE.md --> — kept in a
separate namespace from recorded and deterministic facts. Three designed models
are withheld with their names reserved, for three different reasons: contact-
label confusion (B), where a single scorer per event and no independent second
label leave the data uninformative about the confusion matrix, so any fit
returns its prior; fielder responsibility (I), where the positioning prior a
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
not exclusively so. The snapshot holds 205,845 play-by-play games in the span, 1,953
box-score-only games, and 4 gamelog-only games <!-- src: notes/data-coverage-implementation/README.md -->,
plus 41 play-by-play games from the 1900s decade that lie before the target
span and surface as a `1900` row wherever a table is keyed by decade
<!-- src: tables/corpus_by_decade.md -->. The event table, `event_states_full`,
contains 18,141,020 events, all of them from play-by-play games; that count is
the whole table and so includes whatever the 41 early games contribute
<!-- src: tables/corpus_by_decade.md -->. Surfaces keyed by season — park
factors and Model G's cells — cover every season the source carries, which is
why a few descriptive rows fall before 1910.
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

The modeling datasets carry two game-level partitions, and the paper's
held-out numbers come from one or the other, never both. The datasets stamp
`primary_fold` from `HASH(game_id) % 100` — buckets 0–69 `TRAIN`, 70–84
`VALIDATE`, 85–99 `TEST` — and the deep supplements of §6 train, early-stop,
and report on it <!-- src: bc/models/intermediate/modeling_datasets/model_input_event_universe.sql -->.
Every Bayesian fit, and every coverage gate in §7, instead holds out fold 0
of a ten-fold BLAKE2s hash of `game_id`, removed before any subsampling
<!-- src: bc/python_models/statistical/splits.py -->. The two hashes are
unrelated, so the Bayes holdout is a 10% sample of games that cuts across all
three deep partitions; §6 states what that means for the deep covariate.


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


## Deep-learning supplements

### Role in the pipeline

Deep models in this system produce proposal distributions and entity embeddings. They never publish a fact. The invariant that governs every supplement registered under `bc/python_models/statistical/deep/targets/` is a fixed pipeline shape: deep model → out-of-fold prediction and leakage checks → Bayesian layer → published surface <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md -->. A deep classifier's argmax class is never written to a `main_models` table; SQL artifact views expose probability vectors, and the acceptance criteria explicitly forbid a downstream table that exposes only the top class of a probabilistic target <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md -->. Where a deep proposal does reach a Bayesian model, it enters as a covariate on the log scale — `γ · log p̃^dl_{i,c}`, the per-class log-probability centered on the training-slice class means and weighted by one scalar `γ` shared across classes, with a `N(0, 0.5)` prior whose posterior standard deviation is 0.03–0.04 in every published fit, so the prior is inert and the data set the weight <!-- src: tables/gamma_dl_ablation.md --> <!-- src: bc/python_models/statistical/models/geometry.py -->. Each downstream Bayesian model that consumes a proposal is fit twice — once with `γ` fixed at zero, once free — and the two artifacts are stored as distinct flavors (`gamma_dl_zero`, `gamma_dl_shrunk`). The design rule was to ship the flavor whose inclusion does not move publication-tier posteriors by more than 0.25 SD on most cells, since a larger shift means the deep model absorbed structural signal the hierarchy already carries rather than adding incremental lift <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md -->. In the geometry softmaxes the publication-tier blocks are not scorer, park, or era random effects — those enter every class logit equally, cancel inside the per-event softmax, and were removed — but the per-class intercept `alpha_class` and the fixed-effect interaction tensors (`delta_alignment_regime`, `delta_base_state_start`, `delta_outs_start`, `delta_result_family`, `delta_batter_hand`) that drive the imputation distribution <!-- src: tables/gamma_dl_ablation.md -->.

### Three location dimensions were shifted by a missing covariate

A defect in how the covariate handled a missing prediction is disclosed here because it changed three published surfaces. The location deep specs score only rows whose class was observed, so on the frozen dataset every production row for `location_side`, `location_depth`, and `location_edge` — the unrecorded events those tables exist to impute — had no deep prediction; the trajectory spec's filter includes derived and unknown rows and was unaffected. A missing prediction was mapped to a zero log-probability vector before the training-slice class means were subtracted, so every production row carried the covariate value `−mean_c`, and `γ · (−mean_c)` added a large constant per-class shift to every imputed logit. The published production shares diverged from the training shares accordingly: `location_edge`'s `All` class at 0.173 against a training share of 0.009, `location_side`'s `Default` at 0.24 against 0.70, `location_depth`'s `ExtraDeep` at 0.22 against 0.06 <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->. No gate could see it: the held-out rows are observed rows with real predictions, and the tests covered the two halves separately. The fix has two parts. A row with no deep prediction now contributes exactly zero to its logits on every slice, and a fit whose production slice carries no deep prediction at all must publish the `gamma_dl_zero` flavor — the prep raises rather than scoring a covariate that exists on no production row <!-- src: bc/python_models/statistical/bayes/dl_covariate.py --> <!-- src: bc/python_models/statistical/models/_geometry_data.py -->. The three location dimensions are refit and published deep-free in this revision; the finding underneath is that a deep spec that scores only observed rows cannot supply a covariate to an imputation model, and until the location specs score the unrecorded slice the deep supplement reaches one geometry dimension, not four.

### What the deep proposals add, measured

No published deep artifact had been gated against a baseline before this revision: the validator compares held-out log-loss only when a `baseline_predictions.parquet` sits beside the artifact, and nothing wrote one <!-- src: bc/python_models/statistical/validate.py -->. Measured for the modeling review on the `TEST` partition, deep log-loss against a per-`result_family` class prior is 1.229 against 1.258 for trajectory, 0.996 against 1.035 for `location_side`, 1.094 against 1.114 for `location_depth`, and 0.899 against 0.922 for `location_edge` — real, modest, and previously unmeasured <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->.

The ablation the previous revision reported — the four `gamma_dl_zero` counterparts fit at the published operating point and compared cell by cell against the `gamma_dl_shrunk` fits — is stated here as what the record supports. The zero fits ran on 2026-07-14, after the shrunk fits had been published, and their logs record convergence and top-1 accuracy; the artifacts themselves, and the `gamma_dl_shift.json` per-cell shift statistics the previous draft quoted, are not on disk, so the log-loss deltas and the share-of-cells-over-0.25-SD figures it reported are unreproducible <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md --> <!-- src: tables/gamma_dl_ablation.md -->. This revision re-fits both flavors for every dimension. On trajectory's 616,513 held-out events the shrunk flavor wins every metric: log-loss 1.1382 against the zero flavor's 1.2327, macro PR-AUC 0.4623 against 0.3484, and top-1 0.4920 against 0.4518, with the marginal-entropy baseline at 1.3529 nats and the majority-class top-1 at 0.4147 <!-- src: artifacts/statistical/bayes/geometry_trajectory/e-v12-noprop-trajectory-{shrunk,zero}/validation/held_out_metrics.json -->. The per-cell shift diagnostic, computed over the publication-tier blocks at the 0.25-SD threshold, is largest on trajectory, where 69.6% of the 115 cells move past the threshold and the maximum shift is 3.27 SD; on the location dimensions it is 21.7% of 138 cells at a maximum of 3.22 SD for `location_side`, 27.2% of 92 cells at 0.97 SD for `location_depth`, and 39.1% of 92 cells at 1.58 SD for `location_edge` <!-- src: artifacts/statistical/bayes/geometry_*/e-v12-noprop-*-zero/validation/gamma_dl_shift.json -->. What is already decided by the defect above is the pointer: the three location dimensions publish the zero flavor because the covariate does not exist on their production rows, and trajectory publishes whichever flavor wins held-out log-loss, with the shift diagnostic disclosed as a caveat rather than a switch rule.

### Shared entity-embedding pretraining

Per-target deep models originally learned their own `batter_id` / `pitcher_id` embeddings from scratch, seeing only the row slice each target's label happens to cover — trajectory only sees observed batted balls, handler only sees recorded putout chains. That starves the embedding of the cross-context signal a player's behavior carries. The fix pretrains one shared player embedding once, over the event universe filtered to batted-ball plate appearances — roughly 12M of the 18.1M events, since non-batted-ball rows are NULL on every pretext head — against a multi-head pretext objective, and every downstream target warm-starts from the resulting artifact <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md --> <!-- src: notes/data-coverage-implementation/phase3-acceptance-gates-v6.md -->. The embedding group is batter and pitcher; park and scorer carry their own separate, ungrouped embeddings <!-- src: notes/data-coverage-implementation/phase3-acceptance-gates-v6.md -->. Five pretext heads cover exactly the imputation targets that are genuinely missing in the record — `trajectory_remapped`, three batted-location facets, and `batted_to_fielder_class` — while the outcomes that are fully observed at imputation time (`pa_result`, outs and runs on the play, and the three runner-advancement fields) enter as inputs rather than heads, so the pretext conditional distribution matches the inference distribution and no trivial outcome head can crowd out the hard imputation heads under the multi-task weighting <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md --> <!-- src: notes/data-coverage-implementation/phase3-acceptance-gates-v6.md -->. Training is a residual two-stage decomposition: a stage-1 context-only fit caches its per-head pre-softmax logits, and stage-2 adds the entity embeddings on top of that frozen offset, so the embeddings learn residual and interaction structure rather than re-encoding player skill already carried by context; focal loss counters the severe class imbalance in the location heads <!-- src: notes/data-coverage-implementation/phase3-acceptance-gates-v6.md -->. The gate that justifies the machinery is a permutation-importance comparison against a no-pretrain baseline whose embeddings initialize from scratch, on the time-forward 2023 validation slice: on the geometry-trajectory target, pretraining lifts batter permutation-importance 6.4× and pitcher permutation-importance 6.3× over that baseline <!-- src: notes/data-coverage-implementation/phase3-acceptance-gates-v6.md -->.

The pretraining carries a leak the cross-fitting contract below does not close, stated here as a limitation. The pretrain artifact is fit on the whole `TRAIN` partition with heads on the same geometry labels the downstream targets predict, and every fold model warm-starts its embeddings from it; an out-of-fold prediction is therefore made by a model whose embeddings have seen that row's label through the pretext heads. The Bayes holdout compounds it, because the two partitions of §4 are unrelated: 70.1% of the Bayes held-out trajectory rows — 432,301 of 616,513 — lie inside the pretrain's labeled `TRAIN` set, and about 15% of Bayes held-out games are `VALIDATE` games whose deep logits came from the full fit that early-stopped on them, so the Bayes held-out metrics of §7 carry the same contamination. Which pretrain artifact produced the published proposals is not recorded in their manifests, and the magnitude of the leak is unmeasured <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->.

### Two negative results

Two failures in this supplement stack are worth stating plainly, because both would have shipped a contaminated posterior if the acceptance gates had not caught them. Both are ML-workflow quality findings about how the supplement was built, not statistical results about the record.

**A proxy metric that pointed the wrong way.** During pretrain-architecture selection, a fast in-loop diagnostic — a linear probe fit on the frozen pretrain embeddings to predict `pa_result`, trajectory, and outs — was used to rank candidate pretrain configurations. One candidate won the probe and, when evaluated the way it actually matters — permutation importance on a real downstream fit that fine-tunes the embeddings jointly with the trunk, on the held-out `time_forward_fold = 'VALIDATE'` slice — came out worse than no pretraining at all <!-- TODO: unverified: the probe and permutation-importance margins the previous draft quoted (+0.0068, −0.001) have no repository source; the logs under logs/permimp_gates/ are the place to recover them -->. The mechanism is that the linear probe measures signal remaining in the embeddings at the end of pretraining, while a fine-tuned downstream fit lets the trunk absorb or overwrite that signal during joint training; the two numbers answer different questions and can move in opposite directions. The rule that replaced the proxy: never ship a pretrain artifact on probe evidence alone. The gate is downstream permutation importance against a `BC_DEEP_DISABLE_PRETRAIN=1` baseline, evaluated on the same held-out fold every per-supplement gate uses <!-- src: notes/data-coverage-implementation/phase3-acceptance-gates-v6.md --> <!-- src: bc/python_models/statistical/deep/training.py -->.

**An in-sample fold mislabeled as out-of-fold.** The trajectory geometry target was registered with `fold_count=1`. Because the cross-fitting code path only runs the out-of-fold loop when `fold_count>1`, a `fold_count=1` spec instead falls into a fallback that scores the full training set with the full-fit model and tags every row `OOF` <!-- src: notes/data-coverage-implementation/implementation-review.md -->. The result was 8.43M in-sample predictions carrying an out-of-fold label, flowing through `dl_proposal_manifest` into the geometry Bayesian model as a covariate — a consumed, published posterior trained partly on leaked information. On an identical game-hash holdout, the leaked artifact scored 1.15 percentage points higher on trajectory top-1 accuracy than the refit: 0.5031 leak-inflated versus 0.4916 once the leak was removed <!-- src: notes/data-coverage-implementation/implementation-review.md -->. The fix was mechanical — refit with `fold_count=5` so every prediction is genuinely out-of-fold (8,434,463 OOF rows verified across 5 folds, zero nulls) — but the finding generalizes past this one target: the fallback path mislabels in-sample predictions as `OOF` with no warning, so any future spec left at `fold_count=1` inherits the same silent leak <!-- src: notes/data-coverage-implementation/implementation-review.md -->.

### The cross-fitting contract this enforces

Both failures motivate the same standing rule. Every deep proposal consumed by a Bayesian model must be produced by a model that never saw that row during training — trained on `fold_id != k`, predicted on `fold_id = k`, with folds assigned by `blake2s(game_id) % 5` inside the `TRAIN` partition — and exported with an explicit `prediction_scope` (`out_of_fold`, `validation`, `test`, `full_fit`) so downstream code can enforce the distinction rather than infer it <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md --> <!-- src: bc/python_models/statistical/deep/training.py -->. The proposals are not calibrated: no post-hoc calibration has ever run on a published deep artifact, and the `calibration_method: temperature` the published manifests carry was a copied default — the calibrator module was deleted without a call site, the trajectory proposal is a raw focal-loss softmax, and new manifests record `none` <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md --> <!-- src: bc/python_models/statistical/deep/training.py -->. The Bayes layer's scalar `γ` absorbs a global temperature but not a per-class one, and the previous draft's claim that each proposal was "calibrated on a held-out slice" is withdrawn. Embeddings carry a parallel check: a source-probe classifier trained to predict source family or scorer from the embedding vector. An AUC at or above 0.75 marks the embedding diagnostic-only for that source family — it may not enter a Bayes covariate, only the `gamma_dl_zero` flavor is publishable; below 0.65 it is fully publication-eligible; the band between is a manual-review gray zone recorded in the fit manifest <!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md -->. The probe is implemented as a diagnostic and is not wired into the publish path, so it is a rule the operator applies, not a gate the pipeline enforces <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->. Both checks exist because convergence diagnostics cannot see either failure mode — a leaked or source-encoded proposal can sample cleanly and still bias what gets published.


## Published surfaces

This section reads `bc.db` rather than the model code that built it. Every
number below is a query against the published `main_models.*` estimated
tables; each table caption names its source data table
(`notes/paper/tables/<name>.md`) and the exact SQL that produced it
(`notes/paper/queries/<name>.sql`), so every figure regenerates. The event
universe behind these tables is 18,141,020 play-by-play events across
205,845 games <!-- src: tables/corpus_by_decade.md -->, and the models
described in the preceding sections turn a fixed subset of that universe's
missing fields into posteriors. Every interval in this section is a 94%
highest-density interval: 0.94 is the ArviZ default the fits summarize with,
kept unchanged so that every published HDI is the sampler library's native
summary rather than a probability re-chosen per table, and its unfamiliar width
is a standing reminder that the interval probability is a convention.
<!-- src: bc/python_models/statistical/bayes/training.py --> Tables that show
only extremes or a single cell are labeled illustrative; a claim that depends
on a whole surface cites the full table under `notes/paper/tables/`.

Eight of the twelve tables were re-published on 2026-09-04 from the refits the
corrections of §4 and §6 required: the five geometry dimensions, fielding
credit, run expectancy, state transition, linear weights, pitch summary, park
factors, and the six observation-propensity dimensions. Ball handler, assist
count, and the two placeholders were not refit. Every number in this section
is read from the restated tables.

### Twelve tables, two of them empty on purpose

`docs/estimated-models.md` names twelve published `main_models.*` estimated
tables, and a direct inventory query confirms all twelve exist with the
provenance contract populated:

| table_name | n_rows | n_artifacts | model_versions | confidence_statuses |
| --- | ---: | ---: | --- | --- |
| assist_count_distribution | 596 | 1 | 0.3.0 | passed |
| imputed_advancement_probabilities | 0 | 0 | NULL | NULL |
| imputed_ball_handler_probabilities | 11,712,096 | 1 | 0.3.0 | passed |
| imputed_batted_ball_geometry | 253,013,169 | 5 | 0.3.0 | passed |
| imputed_fielding_credit | 8,335,674 | 2 | 0.3.0 | passed |
| linear_weights_estimated | 5,036 | 1 | 0.3.0 | passed |
| park_factor_summary | 2,636 | 1 | 0.3.0 | passed |
| pitch_count_coverage | 0 | 0 | NULL | NULL |
| pitch_summary_distribution | 18,156 | 1 | 0.3.0 | passed |
| run_expectancy_summary | 5,940 | 1 | 0.3.0 | passed |
| scorer_observation_propensities | 67,397,468 | 1 | 0.3.0 | passed |
| state_transition_summary | 148,500 | 1 | 0.3.0 | passed |

<!-- src: tables/table_inventory.md --> `imputed_advancement_probabilities`
(Model H) and `pitch_count_coverage` (Model J's coverage arm) hold 0 rows
with NULL provenance aggregates. That is the documented deferred-publication
mechanism, not a query error: both `@model`s and their grain are wired, but
neither has a published Bayes artifact pointer to resolve, so each
materializes its typed zero-row frame rather than fabricate rows against a
model that never fit. <!-- src: docs/estimated-models.md --> Every populated
table reads `passed`: the version-2 gate sweep stamped each manifest before the
restate, and §9 gives the column's history. `imputed_batted_ball_geometry`
alone holds 74% of the 340.6M published rows because it publishes a share per
class per geometry dimension per unobserved event, 5,867,709 events for
trajectory and 6,989,832 for each of the four location dimensions, every row on
the unrecorded slice; its five `artifact_id`s are one per geometry dimension,
and `imputed_fielding_credit`'s two are one per credit type.
<!-- src: tables/table_inventory.md --> The putout rows of the latter change
meaning in this revision: they now score the production slice of events whose
putout is unattributed, the same event set the assist rows cover, so the two
credit types carry identical row counts (4,167,837 each) rather than the
well-attributed training grain (§4).

### Coverage collapses at the 1988 boundary

The record's completeness is not a slow trend; it is a step. The deterministic
share of batted-ball events with unknown trajectory or location, by decade
(the 1900 row is the 41 play-by-play games the snapshot holds before the
1910–2025 target span, §2):

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
| 1910 | 0.247 | 0.040 | 0.056 | 0.819 |
| 1940 | 0.150 | 0.034 | 0.028 | 0.741 |
| 1970 | 0.187 | 0.037 | 0.039 | 0.943 |
| 1980 | 0.300 | 0.210 | 0.199 | 0.912 |
| 1990 | 0.953 | 0.935 | 0.955 | 0.938 |
| 2010 | 0.975 | 0.958 | 0.948 | 0.948 |
| 2020 | NULL | 0.910 | 0.979 | 0.943 |

<!-- src: tables/obs_propensity_by_decade.md --> Trajectory and location
propensities move together, sitting at or under 0.3 through the 1980s and
jumping past 0.93 from 1990 forward; `ball_handler_position` is the outlier,
already 0.74–0.94 propensity before 1990, because a handler is partially
recoverable from box-score fielding lines even when no play-by-play trajectory
was logged. The refit moves the trajectory and location decade means by at
most 0.007 against the previously published fits. The 2020-decade `trajectory`
cell reads NULL because the prep drops any season with fewer than 100
unobserved rows or an unobserved rate under 0.5% and scores only the survivors;
every 2020–2025 season fails that floor for `trajectory` (6, 31, 2, 2, 1
unobserved rows across 2020–2024), so nothing is averaged. A missing
`p_observed_mean` is the pipeline's documented convention for "fully observed
by construction" ($p \approx 1$), the opposite of a coverage gap.
<!-- src: tables/obs_propensity_by_decade.md -->

### A missing-not-at-random signature, cleanly measured

The `groundball_mnar` table splits the trajectory slice by `observed_status`
— `observed` (the scorer recorded it), `derived` (deduced from the fielding
string; every such row is `GroundBall`), and `unknown_code` — and reports, per
era, the floor the derived rows place under the unrecorded ground-ball share,
the missing-at-random export's share on the same slice, and the export's
mean ground-ball probability on the derived rows, where the truth is 1:

| era | n_observed | observed GB share | n_unrecorded | n_derived (all GB) | floor P(GB \| unrecorded) | MAR share, unrecorded | MAR mean p(GB) on derived rows |
|---|---:|---:|---:|---:|---:|---:|---:|
| pre-1950 | 712,469 | 0.2892 | 2,615,579 | 763,993 | 0.2921 | 0.3204 | 0.3203 |
| 1950-1987 | 699,353 | 0.3997 | 3,117,160 | 1,057,776 | 0.3393 | 0.3760 | 0.4020 |
| 1988+ | 4,759,451 | 0.4337 | 134,970 | 53,922 | 0.3995 | 0.3918 | 0.3751 |

<!-- src: tables/groundball_mnar.md --> Before 1950 the fielding string alone
recovers more unrecorded ground balls than there are recorded trajectories of
any class, and the MAR export — trained on the recorded slice — scores those
known ground balls at 0.32. From 1988 on the unrecorded slice is 135K events
against 4.8M recorded, and the MAR share sits 0.008 below the floor. An earlier
version of this table pooled the observed and derived slices into an
"observed + derived" ground share (0.680 pre-1950) and read the gap over the
observed share as the size of the under-recording; that pooled share mixes two
slices with different selection and is not an estimate of any population
quantity, and it is withdrawn with the anchored offset of §5.
<!-- src: tables/groundball_mnar.md --> The comparison is built from
`model_input_geometry`'s `observed_status` split rather than from the BSL
`offense_events.ground_ball_rate`, whose merged `trajectory` column already
substitutes the deduced value wherever the recorded one is unknown and so
launders the selection effect this table exposes.
<!-- src: bc/models/intermediate/event_level/calc_batted_ball_type.sql -->

### Geometry marginals on the slice that was never recorded

`imputed_batted_ball_geometry` restricts its trajectory rows to events with no
recorded trajectory — the same slice `coverage_by_decade` sizes — and the
trajectory vocabulary has five classes (`Bunt` is distinct from Fly,
GroundBall, LineDrive, PopUp). Posterior mean expected share by era bucket:

| era_bucket | Bunt | Fly | GroundBall | LineDrive | PopUp | n_rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| pre-1950 | 0.076 | 0.224 | 0.320 | 0.224 | 0.155 | 2,615,579 |
| 1950-1987 | 0.068 | 0.087 | 0.376 | 0.305 | 0.165 | 3,117,160 |
| 1988+ | 0.100 | 0.229 | 0.392 | 0.217 | 0.063 | 134,970 |

<!-- src: tables/geometry_marginals_unobserved.md --> Each row sums to 1
across the five classes, per-event shares averaged within the era bucket.
The unobserved slice shrinks 23-fold from 1950–1987 to 1988+ (3.1M rows to
135K), the geometry-side face of the same coverage collapse: after 1988 there
is barely any unrecorded trajectory left to impute. Distinct event-key coverage
confirms the restriction is real — geometry's trajectory rows cover 5,867,709
of 18,141,020 total events (about 32%). <!-- src: tables/geometry_marginals_unobserved.md -->
The pre-1950 GroundBall posterior mean, 0.320, is the published face of the
MNAR problem of §5: Model E ships under the missing-at-random flavor, trained
on the recorded slice and scored on the unrecorded one, so it inherits the
recorded slice's selection onto the era where nearly everything is imputed.
§5's ribbon and floor — not this table — are where that is bounded: across the
$\pm 1.0$-nat grid the pre-1950 share runs from 0.162 to 0.530, with the floor
of 0.292 inside the band. <!-- src: tables/trajectory_mnar_bound.md --> The
three location dimensions' previously published marginals are not shown because
they were wrong (§6); the deep-free refit's pooled marginals over the 6,989,832
imputed events per dimension are:

| dimension | class shares |
| --- | --- |
| location_side | Default 0.692, Middle 0.135, FoulLine 0.054, Left 0.052, Right 0.045, Foul 0.022 |
| location_depth | Default 0.604, Deep 0.185, Shallow 0.161, ExtraDeep 0.050 |
| location_edge | Middle 0.623, Left 0.195, Right 0.173, All 0.010 |

<!-- src: tables/geometry_marginals_unobserved.md --> Era-bucket means differ
from these pooled values by at most 0.02, and the three classes §6 names as the
defect's signature (`location_edge` `All`, `location_side` `Default`,
`location_depth` `ExtraDeep`) now sit at 0.010, 0.692, and 0.050 against
training shares of 0.009, 0.70, and 0.06, where the previous surface published
0.173, 0.24, and 0.22.

### Run expectancy and the state-transition matrix, 2015 NL

`run_expectancy_summary` publishes a posterior mean plus 94% HDI for each of
the 24 base-out states on the corrected population of §4. The two extremes at
0 outs (illustrative; the full 24-state surface is `tables/re_matrix_2015_nl.md`):

| state | base_state | outs | re_value_mean | hdi_lower | hdi_upper | width |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0_0 (empty) | 0 | 0 | 0.482 | 0.443 | 0.519 | 0.076 |
| 0_7 (loaded) | 7 | 0 | 2.323 | 2.169 | 2.478 | 0.309 |

<!-- src: tables/re_matrix_2015_nl.md --> Bases loaded, nobody out, is worth
several times the bases-empty value, and the posterior interval tracks the
common empty-base state tightly and the rare loaded-base state loosely,
uncertainty scaling with how often the state occurs rather than being fixed by
construction. The previously published values (0.451 and 2.202) sat 1.1–1.4%
below the deterministic matrix in every era because the population included
walk-off-censored innings and no-play rows; on the corrected population that
shortfall is gone. Over the 5,424 cells the estimated and deterministic
surfaces share, the median offset is −0.01% and the mean +0.34%, and the two
2015 NL extremes above sit 0.012 and 0.123 runs above a deterministic value
that carries two decimals (0.470, 2.200).
<!-- src: tables/re_matrix_2015_nl.md --> <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->

`state_transition_summary` publishes the full end-state distribution for each
start state. The example start is a runner on first with one out (`1_1`),
chosen because a one-out start is where the reachability mask of §4 is visible:
every 0-out end class is pinned by the mask, and the double play that ends the
inning is a legal end from this start. All 25 end classes (illustrative):

| end_class | prob_mean | hdi_lower | hdi_upper |
| --- | ---: | ---: | ---: |
| 2_1 (batter out, runner holds) | 0.3898 | 0.3779 | 0.4017 |
| 1_3 (batter reaches, runner to second) | 0.1873 | 0.1776 | 0.1977 |
| inning_end (double play) | 0.1204 | 0.1118 | 0.1286 |
| 1_2 (runner to second, batter still up) | 0.0910 | 0.0836 | 0.0982 |
| 2_2 (batter out, runner to second) | 0.0853 | 0.0780 | 0.0925 |
| 1_5 (batter reaches, runner to third) | 0.0400 | 0.0350 | 0.0447 |
| 2_0 (runner out on the bases) | 0.0277 | 0.0236 | 0.0318 |
| 1_0 (home run) | 0.0232 | 0.0196 | 0.0269 |
| 1_6 (double, runner to third) | 0.0183 | 0.0150 | 0.0214 |
| 1_4 (triple, runner scores) | 0.0140 | 0.0113 | 0.0169 |
| 2_4 (batter out, runner to third) | 0.0028 | 0.0019 | 0.0038 |
| 1_1 (batter reaches, runner scores) | 0.0003 | 0.0002 | 0.0005 |
| five base-impossible classes (1_7, 2_3, 2_5, 2_6, 2_7) | 0.0000 | 0.0000 | 0.0000 |
| eight 0-out classes | 0.0000 | 0.0000 | 0.0000 |

<!-- src: tables/transition_example.md --> `prob_mean` sums to 1 across the
25 rows. The eight 0-out end classes carry exactly zero because the mask pins
them with no free parameter; the five classes that are reachable by out count
but impossible from this base state in one event — a runner on first cannot
become bases loaded with one out on a single play — are free parameters that
the posterior leaves near $1.5 \times 10^{-6}$ (§4). The self-transition
`1_1 → 1_1`, which needs the batter to reach first while the runner scores from
first, carries 0.0003; in the previously published surface the corresponding
`0_0 → 0_0` row carried 0.31 of the mass, most of it substitutions and no-play
rows the corrected population excludes.
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->

### Park-factor extremes and uncertainty under sparse data

`park_factor_summary`'s highest and lowest park-seasons by posterior mean
(illustrative; the full surface is the table):

| rank | park_id | season | league | park_factor | hdi_lower | hdi_upper |
| --- | --- | ---: | --- | ---: | ---: | ---: |
| top 1 | DEN02 | 1996 | NL | 1.392 | 1.323 | 1.461 |
| top 2 | DEN02 | 1995 | NL | 1.391 | 1.311 | 1.472 |
| bottom 2 | CLE07 | 1942 | AL | 0.838 | 0.797 | 0.881 |
| bottom 1 | CLE07 | 1940 | AL | 0.835 | 0.793 | 0.883 |

<!-- src: tables/park_factor_extremes.md --> Coors Field (DEN02) holds the top
of the distribution across eight straight NL seasons, 1995–2002, peaking at
1.392 in 1996, as it did in the previous fit; the bottom eight are split between
Cleveland Municipal Stadium (1939–1943) and Dodger Stadium (1963–1965) at
0.835–0.847, with Cleveland's 1940 season now last where the previous fit put
Dodger Stadium's 1965. <!-- src: tables/park_factor_extremes.md --> Two
properties of the fit temper the reading. The factor is runs per plate
appearance with a near-unit-root persistence prior (posterior $\rho$ 0.956,
innovation sd 0.019), so it sits below a runs-per-inning definition — 1.265
against 1.395 for Coors in 2015 — and single-season events such as the 2002
humidor smear over five or more seasons. The persistence hyperparameters that
mixed poorly in the previous fit mix in this one: the AR(1) is now
non-centered and gap-aware, and $\rho$ and $\sigma_{\text{innov}}$ carry
bulk ESS of 3,261 and 1,891 at r-hat under 1.001 (§11).
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md --> <!-- src: tables/park_factor_extremes.md -->

Interval width tracks data density, not league identity:

| league | n_park_seasons | avg_hdi_width |
| --- | ---: | ---: |
| NAL | 12 | 0.159 |
| FL | 16 | 0.148 |
| NN2 | 19 | 0.148 |
| NL | 1,288 | 0.093 |
| AL | 1,301 | 0.092 |

<!-- src: tables/park_factor_extremes.md --> The NAL's average HDI width over
12 park-seasons is 71% wider than the NL's over 1,288 (73% in the previous
fit). With an order of magnitude fewer park-seasons to pool across, the sparse
leagues' posteriors are and should be wider. That is a description of the
posterior, not a validated coverage claim: park factors have no held-out
coverage hook, and the predictive-coverage check below runs only on run
expectancy and transitions (§11).

### `linear_weights_estimated` against the deterministic point surface

Joined on `(season, league, play)` for the 2015 NL, the Bayesian run-value
posterior and the deterministic `linear_weights` point value (illustrative;
the full 20-play comparison is `tables/linear_weights_compare.md`):

| play | play_category | deterministic | estimated_mean | hdi_low | hdi_high | diff |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| HomeRun | BATTING | 1.393 | 1.395 | 1.370 | 1.420 | 0.002 |
| Single | BATTING | 0.436 | 0.440 | 0.426 | 0.456 | 0.004 |
| StrikeOut | BATTING | -0.256 | -0.259 | -0.264 | -0.254 | -0.003 |
| OtherAdvanceOut | BASERUNNING | -0.441 | -0.442 | -0.445 | -0.440 | -0.001 |
| DoublePlay | BATTING | -0.769 | -0.781 | -0.810 | -0.752 | -0.012 |

<!-- src: tables/linear_weights_compare.md --> The deterministic value falls
inside the 94% HDI on all 20 play types, and the offsets are centered near zero
(eight positive, eleven negative, one zero; the largest is `ReachedOnError` at
+0.021) where the previously published surface's offsets were uniformly
negative, the run-expectancy population's censoring rather than posterior
uncertainty <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->.
An earlier draft of this paper reported `OtherAdvanceOut` as the one play
outside the band, which was not true of the table it cited. The agreement is
itself informative: the Bayesian run-expectancy propagation reproduces standard
linear weights closely while additionally carrying an interval, rather than
replacing a trusted number with a different one.

The published band propagates two sources of uncertainty. Each run-expectancy
posterior draw flows through `runs_on_play + RE_end − RE_start`, and the
per-`(season, league, play)` combination weights are drawn per posterior draw
from a Jeffreys Dirichlet($n$+0.5) over the cell's own transition counts, so a
sparse cell's finite-sample noise widens its band automatically while a dense
cell is unchanged to first order. <!-- src: bc/python_models/statistical/linear_weights_estimated.py -->
Measured when the propagation was introduced, against the run-expectancy
posterior published at the time, the Dirichlet band was at least as wide as the
fixed-count band on 99.57% of 5,099 cells, with a median width ratio of 1.56
and the largest widenings on the smallest-n Negro-league cells, 1941 NN2
`Double` (59.8×) and 1921 NN1 `Triple` (33.6×).
<!-- src: tables/linear_weights_width_comparison.md -->
The Dirichlet propagation has been the published one since 2026-07-14, and this
revision adds the deterministic sibling's occurrence floor and drops
transitions whose start state has no posterior cell instead of substituting a
run expectancy of zero. <!-- src: bc/python_models/statistical/CLAUDE.md -->
On the restated table, 827 of 5,036 cells sit at or below the 100-occurrence
floor and share one corpus-pooled value per play, with HDI widths of 0.001 to
0.009 (median 0.0045) against 0.009 to 0.216 (median 0.061) for the 4,209
cells above it, the two Negro-league cells above among them. Two of the 20 plays in the 2015 NL comparison, `PassedBall` (96 events)
and `OtherAdvanceOut` (23 events), are floor cells on both sides of the join,
which is why their intervals are far narrower than their neighbours'. The
table marks those floor cells with `is_imputed = True`, so a consumer can
exclude or downweight them without re-deriving the floor.
<!-- src: tables/linear_weights_compare.md --> <!-- src: bc/models/intermediate/coverage/linear_weights_estimated.py -->

### Assist counts and pitch summaries

`assist_count_distribution` places the probability mass over how many assists
a play produced, conditional on at least one. Bases empty versus a runner on
first, both at 0 outs, both `out_in_play` (illustrative):

| base_state_start | 1 assist | 2 assists | 3 assists |
| ---: | ---: | ---: | ---: |
| 0 (empty) | 0.992 | 0.008 | 0.000 |
| 1 (runner on first) | 0.631 | 0.366 | 0.002 |

<!-- src: tables/assist_pitch_examples.md --> A bases-empty groundout is a
single assist 99% of the time; put a runner on first and multi-assist mass
rises from 0.8% to 36.6%, the double-play states carrying almost all of the
model's 2-assist probability.

`pitch_summary_distribution` gives the final ball-strike count distribution
per result family. For strikeouts, 2015 NL, all 12 final-count classes are
published; the eight classes with fewer than two strikes carry exactly zero
mass because the per-family structural mask of §4 pins them:

| final_count_class | balls | strikes | prob_mean |
| --- | ---: | ---: | ---: |
| b1_s2 | 1 | 2 | 0.341 |
| b2_s2 | 2 | 2 | 0.283 |
| b0_s2 | 0 | 2 | 0.226 |
| b3_s2 | 3 | 2 | 0.150 |

<!-- src: tables/assist_pitch_examples.md --> The zeros are a constraint the
model is told, not one it recovers: the previously published fit had no mask
and carried up to 0.06 of strikeout mass on counts with fewer than two strikes
in some season-leagues, while this paper's earlier draft reported the
constraint as recovered from data. The four two-strike shares in this dense
cell are unchanged to three decimals by the mask.
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->

### Validation

The preceding tables assert calibration and honest uncertainty as narrative;
this subsection is the evidence, and it describes the gate suite as it now is.
`just validate-gates` sweeps every published artifact pointer through
`validate_artifact` and reports validation status plus fired findings. A fit
blocks on convergence — r-hat, bulk ESS, divergences — and on held-out
evidence: a full-scale fit whose `held_out_metrics.json` is missing blocks, as
does one whose held-out metric fails to beat its baseline. For the multinomial
targets the blocking metric is held-out log-loss against the entropy of the
pooled held-out class marginal, the log-loss of the constant predictor that
emits the empirical class shares, with top-1 accuracy retained as a
`warn`-only diagnostic. Held-out expected calibration error and
posterior-predictive interval coverage are computed and reported but only
warn; they do not gate publication. The publish-path refusal of smoke fits,
the version-stamped `confidence_status`, and the per-sweep recomputation of
the weak-identification flag are §9's.
<!-- src: bc/python_models/statistical/validate.py --> <!-- src: bc/python_models/statistical/cli.py -->
Of the 23 registered gate targets, 19 pass; the four
deep-proposal pointers are not validated by the sweep — it resolves them under
a layout they do not use and reports them `missing` — so the deep supplement's
own held-out comparison is the one §6 reports from the review, not a sweep
result. <!-- src: tables/validation_gates.md --> <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->

Held-out expected calibration error is computed on all six of Model A's
Bernoulli propensity dimensions. On the refit it runs from 0.0049
(`location_edge`) to 0.0181 (`trajectory`), every dimension inside the 0.05
warn band, with `trajectory` — the dimension carrying the most pre-1988
missingness — the least calibrated of the six, the same two dimensions at the
ends of the same band as on the previous fits (0.0054 to 0.0179)
<!-- src: tables/validation_gates.md --> <!-- src: artifacts/statistical/bayes/*_observedness/10k-v6-unseen-fix/validation/held_out_metrics.json -->.
The number is a diagnostic: a dimension outside the band would fire a `warn`
and still publish.

For `state_transition` and `run_expectancy` — the two aggregate surfaces whose
published intervals cover a cell mean or probability rather than a single
event — held-out HDI coverage is checked two ways against the same held-out
game fold, and the two ways disagree sharply enough to be worth showing both.
Parameter coverage asks whether the held-out empirical realization lands
inside the published 94% HDI of the fitted mean; on a corpus this dense the
fitted mean's own interval shrinks toward a point well before the held-out
cell's finite-sample noise does, so parameter coverage collapses on both
surfaces — 0.2042 for `state_transition`, 0.3861 for `run_expectancy` —
comparing a noisy realized frequency against an interval that was never built
to contain it. <!-- src: tables/validation_gates.md --> Posterior-predictive
coverage folds that finite-sample noise into the parameter uncertainty before
checking containment and is the number the gate reports: `state_transition`
covers at 0.9780, inside the band and slightly conservative, and
`run_expectancy` at 0.9062, inside the 0.88 to 0.99 band. On the previous
run-expectancy fit the same number was 0.8774, under the 0.88 floor and fired
as a `warn`; the corrected population and the per-state dispersion of §4 are
what moved it <!-- src: tables/validation_gates.md -->.
The predictive check's parameter layer is itself an approximation: neither
surface persists per-draw class probabilities, so the simulation reconstructs
the parameter posterior per class as an independent Normal(`prob_mean`,
`prob_sd`) truncated to $[0, 1]$ and renormalized across the cell's classes
(§11). <!-- src: tables/validation_gates.md -->

One gate correction is worth recording because it is a case of the acceptance
suite measuring the wrong quantity. `geometry_location_depth` was blocked for
a held-out top-1 accuracy of 0.5609 against a majority-class baseline of
0.5611: `location_depth` is dominated by a single `Default` class, so arg-max
is close to useless as a discriminator there, while the calibrated shares
underneath matched the held-out empirical shares to a total-variation distance
of 0.0051 and beat the marginal-entropy baseline on log-loss by 0.040 nats.
<!-- src: tables/validation_gates.md --> The gate was blocking publication
confidence on the arg-max, which §9's policy bans from canonical consumption,
while leaving the share vector that policy calls canonical ungated; two
dimensions of the same model separated by 2e-6 of top-1 accuracy had landed on
opposite sides of it on sampling noise. The multinomial gate now blocks on
log-loss, as §4 specified. The published fits' lifts over the distributional
baseline, with the three location dimensions deep-free, are +0.011 nats
(`location_depth`), +0.011 (`location_edge`), +0.024 (`location_side`), +0.085
(`general_location`), and +0.215 (`trajectory`): every dimension clears the
baseline, while the deep-free location fits sit at or just below the
majority-class top-1, which sharpens the disagreement between the two metrics
rather than settling it. The two metrics rank the dimensions differently:
`location_side` is last on top-1 lift and third on log-loss lift.
<!-- src: tables/validation_gates.md --> <!-- src: artifacts/statistical/bayes/geometry_*/*/validation/held_out_metrics.json -->


## What the record cannot tell you

A single-source play-by-play record bounds what coverage modeling can do, and part of a modeling program's job is to say exactly where that bound sits rather than paper over it with a model fit on the wrong estimand. Three model letters in this family — B, I, and K — are withheld, but they are withheld for three different reasons and the difference matters. Only Model B carries an argument about the likelihood: the data are uninformative about its target, so any fit returns the prior — a statement about what this record can teach, defended below, rather than a formal non-identification proof. Model I is withheld because a specific input its correct specification needs does not exist anywhere in the source data — a data-availability limitation, not a proof that the estimand resists identification in principle. Model K is withheld because it has not been built in this pass — unfinished scope, not a claim about the record at all. Presenting the three at the same rigor would overstate the weakest of them, so each subsection below states its claim type before making it.

### Model B — contact-label confusion: the data are uninformative about Ω

This is a claim that the data are uninformative, not a report that confusion happens to be rare. Model B was specified as a scorer confusion model: a latent true contact class `Z_i` generating a recorded label `L_i` through a scorer/decade confusion matrix `Ω`. Identifying `Ω` from data needs one of two things — repeated, independent labels for the same event, or outcome evidence that is itself independent of the recorded label and can disagree with it. Retrosheet-era play-by-play has one scorer per event (scorer is a game-level attribute in `game_scorekeeping`), so the first route is closed: there is no second, independently drawn label to compare against the first.

The obvious candidate for that second label — the "deduced" batted-ball class computed by `calc_batted_ball_type.sql` — is not independent either. When the recorded trajectory is known, the deduction rule sets the deduced broad class equal to the recorded broad class by construction, so comparing recorded against deduced on that majority of events measures the deduction rule, not scorer behavior; the comparison carries zero independent information about `Ω` there. The only place independent evidence exists is where an outcome, not a recode of `L_i`, can adjudicate the class on its own — home runs, outfield-depth putouts, unassisted putouts. Restricted to that outcome-anchored evidence, the recorded broad class and the deduced broad class disagree on 702 of 6.0M recorded-known, non-bunt batted-ball events, 0.0117% <!-- src: memory:model_b_contact_blocked.md -->, all one-directional (GroundBall → AirBall, never the reverse), with no era or scorer structure.

The conclusion follows from the shape of the evidence, not from the size of the disagreement rate. Over the overwhelming majority of the record, the likelihood for `Ω` is flat — unmoved by the data, because the only comparison available there is an identity by construction, not a measurement. Over the 0.0117% outcome-anchored sliver, the 702 disagreements carry some information, but not enough to resolve a matrix indexed by scorer and decade, and they carry no era or scorer structure to resolve it with; a likelihood that is flat almost everywhere and thinly informative on a sliver two orders of magnitude too small to inform a structured matrix returns the prior. The precise statement is that the data are uninformative about `Ω` and any fit returns the prior — not that `Ω` is non-identified in a formal sense, since the anchored sliver does move the posterior, just not by enough to matter — and it is a different statement again from "confusion is rare," which describes `Ω`'s value rather than what the data can say about it. Unblocking Model B needs a second, genuinely independent contact-label source per event — a different feed's trajectory call — which means ingesting a second parser source, out of scope for this repository.

### Model I — fielder responsibility: a data-availability limitation

This is not a claim that responsibility is unidentifiable in principle. It is a claim about what today's source data contains: Model I's correct specification needs an input that does not exist in the record, and if that input existed the identification picture would look nothing like Model B's — there would be no confusion-matrix-style argument to make at all.

Model I's estimand is an analytical opportunity, not a recorded fact: `P(responsible = k | geometry, alignment, …)`, who should have had a play given where the ball went, marginal over who actually got to it — explicitly distinct from the ball handler, the fielder who did get to it <!-- src: notes/data-coverage-implementation/implementation-review.md -->. A first implementation shipped anyway, trained on `ball_handler_position` restricted to range positions, which makes it Model D with a narrower vocabulary rather than a responsibility model at all. The confound is empirical: 28% of the training slice is hits, and on hits the "handler" is just whichever fielder retrieved the ball after it got past the defense — 89% of hit-handlers are outfielders <!-- src: notes/data-coverage-implementation/implementation-review.md -->. A model trained on that label can only relearn who touched the ball, which Model D already publishes; it cannot answer who should have.

The design that would actually estimate responsibility decomposes it into a geometry term the pipeline already has — Model E's location posterior — and a term it does not: `π(k | location, alignment)`, a Dirichlet zone-responsibility kernel giving the positional probability of coverage for a location bin under a given defensive alignment <!-- src: notes/data-coverage-implementation/responsibility-zone-design.md -->. No positioning prior of that shape exists anywhere in the source data; the handler is the only position-valued label a batted ball carries. That is the whole limitation: a positioning-prior source — tracking-era defensive alignment logs, for instance — would resolve `π(k | location, alignment)` directly and turn Model I into a straightforward fit. It is withheld because that source is not in this record, not because the estimand resists identification. The prototype was parked and removed, the `responsibility_artifact_id` column stays as a reserved NULL pointer, and nothing named responsibility ships until the zone kernel exists <!-- src: notes/data-coverage-implementation/responsibility-zone-design.md -->.

### Model K — shift propensity: descoped, not a claim

Model K makes no identification claim and no data-availability claim. It is unfinished scope: designed, not built.

Shift propensity was scoped as its own first-class model — `P(shift | player, batter_hand, defending_team, count, outs, base_state)` — rather than folded into a categorical `alignment_regime` covariate, on the reasoning that defensive shifting is too consequential to bucket into four eras and that post-2015 event data and post-2009 pitch data are rich enough to support a dedicated fit <!-- src: memory:data_coverage_shift_model.md -->. Nothing in the record prevents fitting it. Model K was designed to feed Model I's alignment input, and would plausibly feed geometry and run-value models as well, but the fit was not carried out in this pass. Every model that would consume its posterior — Model I above, and Model E's alignment-regime fixed effect — instead falls back to the era-normal alignment prior it was always meant to use for eras and cells where a shift model has no support; that fallback is the entire operating mode today, not a degraded corner case. K appears in this section for publication-transparency inventory — it records that the shift-propensity posterior does not exist yet — not because it belongs beside a genuine identification or data-availability limit.

### What the three share, and what they don't

These three withheld model letters do not share one failure mode. B's is the narrowest claim: given a single scorer per event and no independent second label, the likelihood barely moves on `Ω` over the whole record, so the data are uninformative and a fit returns the prior. I's is a data-availability limitation: the input a correct model needs is absent from the source, not absent in principle, and a different data source would resolve it outright. K's is neither of those — it is scope not yet completed, and grouping it with B and I asserts nothing about the record at all.

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
Models B, I, and K — discussed in the previous section and mapped in §4's
table — sit in `withheld` today: designed, in two cases prototyped, and not
published, because no amount of additional modeling substitutes for an
identifying source of variation the record does not contain.

The `estimated` tier is the one this paper's results section draws from, and
every row in it carries the same eight-column contract: `artifact_id` (the
fit that produced the row), `model_name`, `model_version`, `source_snapshot_id`,
`method` (`hierarchical_logistic`, `hierarchical_bayes_softmax`, or
`hierarchical_bayes_nb`; the pitch-summary table stamps the softmax method, since
it is a 12-class multinomial), `observed_status` (the constant `estimated`, a
namespace marker), `confidence_status` (see below), and
`weak_identification_flag`. <!-- src: notes/data-coverage-implementation/phase5-conventions.md --> <!-- src: docs/estimated-models.md -->
The flag is derived from group-level diagnostics: the worst r-hat and smallest
bulk ESS among the posterior variables with at most 512 elements — scalars,
pooling hyperparameters such as the negative-binomial dispersion or the
park-persistence $\rho$, and small group-level effects — not the worst element
among thousands of per-cell or per-scorer parameters. A convergent fit is flagged
when that group-level ESS falls under 400, the group-level r-hat exceeds 1.025,
any divergence was recorded, or a diagnostic is non-finite. The sweep described
below recomputes the flag from the stored posterior, so it reflects the current
thresholds rather than the ones in force when the fit ran.
<!-- src: bc/python_models/statistical/validate.py --> <!-- src: docs/estimated-models.md -->
Diagnostic columns such as ESS and r-hat are deliberately absent from the
published rows — they belong in the validation reports, not in the table a
downstream consumer joins against.
<!-- src: notes/data-coverage-implementation/phase5-conventions.md -->

`confidence_status` is copied onto the rows when the table is materialized, from
the `validation_status` the gate sweep stamped on the fit's manifest. Only the
sweep writes that stamp — `just validate-gates --write` re-derives every
artifact's diagnostics from its stored posterior, grades it, and records the
status together with the gate version that graded it — and a manifest that was
never swept, or was swept under an older gate version, materializes as
`exploratory` no matter how its checks went. <!-- src: bc/python_models/statistical/bayes/manifest_ingest.py --> <!-- src: bc/python_models/statistical/validate.py -->
The version stamp is what makes a `passed` row mean something: adding a gate or
raising a severity bumps the version, and every artifact must be re-swept before
it publishes as `passed` again. The publish path refuses to write a pointer for a
smoke-budget fit, so the smoke thresholds the sweep applies to such fits can no
longer reach a published table. <!-- src: bc/python_models/statistical/cli.py -->
The history of the column is part of the record: the tables were first published
as `exploratory`, restated as `passed` on 2026-07-30 after the first sweep,
reverted to `exploratory` when this revision bumped the gate version, and read
`passed` again on every populated table since the version-2 sweep and the
restate of 2026-09-04 <!-- src: tables/table_inventory.md -->. The gate results
in §7 describe what the current fits are graded, not what a row said on any
earlier date.

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
letters out of eleven, each with its blocking condition written down rather
than papered over with a wide interval.


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
treated as unidentified from the observed slice, bounded from below where a
partial-truth slice internal to the record supplies a floor, and otherwise
swept, not solved for.

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
trajectory, reported per class. What the paper adds to the template is a hard
constraint on the sweep — a deduced-trajectory partial-truth slice internal to
the record that places a floor under one class's share on the missing stratum,
and a known-truth subslice on which the missing-at-random fit can be scored —
and a masked backtest that tests the offset's functional form against a
selection process it does not nest. An earlier revision read the same slice as
an estimate of δ_c; §5 explains why it is a floor and not an estimate. The structure of the treatment — a selection-model
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

This section lists what is still open, named against the specific artifact it blocks and the specific condition that would unblock it. None of these are hedges against unknown risk; each is a concrete, already-diagnosed gap. Several items are defects found by the modeling review of 2026-09-03 and fixed in this revision; the refits that carry those fixes into the tables completed on 2026-09-04, and each item states what the restated tables show.

**The MNAR offset `δ_c` is unidentified for every class; the derived slice supplies a floor for one.** §5 withdraws the per-era anchored offset the previous revision published — it was `−ln p_obs` of the observed slice, not a measurement of the unrecorded one — and replaces it with the hard lower bound `P(GroundBall | unrecorded) ≥ n_derived / n_unrecorded` (0.292, 0.339, 0.400 by era) and a known-truth diagnostic of the MAR fit on the derived rows (mean `p(GroundBall)` 0.32, 0.40, 0.38 against a truth of 1). <!-- src: tables/groundball_mnar.md --> The four non-GroundBall trajectory classes have no analogous deduction path and no floor; events whose `observed_status` is `unknown_code` or `missing` sit outside the floor's reach entirely; and the derived slice is used only for that diagnostic — it is not folded into training as a labeled slice, not used as a validation set for the imputation fit beyond §5, and the fielding string that identifies it is not a model covariate, which is why the MAR fit cannot tell derived rows from unknown ones. Unblock condition: an independent partial-truth source for the non-ground classes — a second scorer stream or a tracking-era backfill — and a decision on whether the derived rows should enter the geometry fit as labeled unrecorded events, which would turn the diagnostic into training data and remove it as a check.

**The masked-backtest robustness table is a smoke-budget run.** All four designs in `mnar_backtest_robustness.md` were fit at 50 draws × 50 tune × 2 chains with the convergence gates failing (bulk ESS 20–47), and no run artifacts are checked in; three of the four designs are algebraic identities for the oracle offset, so the table carries one informative number, the covariate-joint design's 0.537. <!-- src: tables/mnar_backtest_robustness.md --> Unblock condition: rerun the four designs at the default sampler budget and check the `metrics.json` / `mask_summary.json` outputs in.

**Three location dimensions were published with a constant per-class shift, and the deep supplement reaches one geometry dimension.** §6 discloses the defect: every production row of `location_side`, `location_depth`, and `location_edge` had no deep prediction, and the centering turned that absence into a shift of `−γ · mean_c` on every imputed logit. The fix zeroes the term on rows without a prediction and forces the deep-free flavor when the production slice has none, and the three dimensions are refit deep-free, and the restated production shares sit on the training shares: `location_edge` `All` 0.010, `location_side` `Default` 0.692, `location_depth` `ExtraDeep` 0.050, against 0.173, 0.24, and 0.22 before <!-- src: tables/geometry_marginals_unobserved.md -->. What remains: the location deep specs score only observed rows, so they cannot feed an imputation model at all. Unblock condition: extend the location specs' row filter to the unrecorded slice the way the trajectory spec already does, refit the proposals, and then refit the location dimensions with the covariate active.

**The deep out-of-fold predictions carry a pretraining leak, and the deep pointers are outside the gate sweep.** Every fold model warm-starts its batter and pitcher embeddings from a pretrain artifact fit on the whole `TRAIN` partition with heads on the geometry labels, so an out-of-fold prediction comes from a model whose embeddings have seen the row's label; 70.1% of the Bayes held-out trajectory rows lie inside that labeled set, and the Bayes held-out metrics inherit the contamination. The four deep pointers are also not validated by `just validate-gates` — the sweep resolves them under a directory layout they do not use and reports them `missing` — and no deep artifact was compared to a baseline before this revision (§6). <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md --> Unblock condition: pretrain on a partition disjoint from every downstream holdout, or re-derive the pretrain per fold; record the pretrain artifact id in each proposal manifest; and teach the sweep the deep artifact layout so the baseline comparison runs on every published proposal.

**`linear_weights_estimated`'s band carries RE-posterior and finite-sample uncertainty, and its floor cells are pooled.** The Dirichlet($n$+0.5) combination-weight propagation described in §7 is the published one — promoted 2026-07-14 <!-- src: bc/python_models/statistical/linear_weights_estimated.py --> — and this revision adds the deterministic sibling's occurrence floor (cells at or below 100 occurrences publish the corpus-pooled value with `is_imputed = True`) and drops transitions whose start state has no posterior cell instead of substituting a run expectancy of zero <!-- src: bc/python_models/statistical/CLAUDE.md -->. The table was re-derived from `re-full-eraregime-v4` on 2026-09-04: on the 2015 NL comparison the deterministic value falls inside the 94% HDI on all 20 play types and the offsets are centered near zero (§7). One gap remains. The 827 floor cells are marked `is_imputed = True`, but a floor cell's interval is the corpus-pooled interval (median width 0.0045 against 0.061 above the floor), which is tight because it pools every season, not because the cell is well measured <!-- src: tables/linear_weights_compare.md -->.

**`run_expectancy`'s held-out posterior-predictive coverage was below the acceptance floor.** On the previously published fit the predictive HDI coverage was 0.8774 against a floor of 0.88, fired at `warn` <!-- src: tables/validation_gates.md -->; the review traced part of the under-coverage to a single global dispersion against empirical variance-to-mean ratios that differ by state, and part to the population defect of §4. The corrected fit carries a per-state dispersion and the corrected population; its predictive coverage is 0.9062, back above the 0.88 floor, so `predictive_coverage_out_of_band` no longer fires <!-- src: tables/validation_gates.md -->. Predictive coverage and held-out calibration error are warn-only in the gate suite, so a fit can pass with either out of band; whether they should block is an open policy question rather than a code gap.

**The posterior-predictive coverage check's parameter layer is a reconstruction, not the stored posterior.** Neither `state_transition` nor `run_expectancy` persists per-draw class probabilities in its published summary export, so the predictive-coverage simulation approximates each cell's parameter posterior as an independent Normal(`prob_mean`, `prob_sd`) truncated to $[0, 1]$ and renormalized across classes, rather than drawing from the true correlated posterior captured at fit time. <!-- src: tables/validation_gates.md --> This is adequate where the finite-sample layer dominates predictive width, which holds for both surfaces on this corpus's dense cells, but it is a stated approximation. Park factors have no coverage hook at all: `park_factor_runs` is graded on convergence and held-out lift only, and the interval-width-versus-sparsity pattern §7 shows for the sparse leagues is a description of the posterior, not a validated coverage claim. Unblock condition: persist a per-draw class-probability export, or a compact sufficient summary of the posterior's correlation structure, for a validation-scoped sample of cells, and add a park-factor coverage hook against held-out team-games.

**`confidence_status` is a version-stamped status, and every populated table reads `passed` only until the next gate bump.** A `passed` stamp publishes as `passed` only when its gate version matches the current one (§9), so this revision's gate bump, which added the smoke refusal, the block on absent held-out evidence, and the group-level weak-identification flag, reverted every table to `exploratory` until the refits completed, the version-2 sweep re-stamped, and the `@model`s re-materialized on 2026-09-04. Every populated table now reads `passed` under gate version 2 <!-- src: tables/table_inventory.md -->; the two placeholders carry NULL. The next gate change reverts the column again, by design.

**Six fits carry `weak_identification_flag = True`, and the flag is per row.** Under the previous flag logic (the minimum ESS over every element of every parameter) the run-expectancy and linear-weights tables were flagged on a single dispersion parameter at ESS 396, the transition table on its densest cells, and the park-factor table was unflagged although its persistence hyperparameters, which control all pooling, mixed at ESS 50 and 17 with r-hat up to 1.17. <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md --> The flag now reads the group-level pair over variables with at most 512 elements and is recomputed by the sweep. On the restated tables it is TRUE on every row of `state_transition_summary` (`alpha_trans[1]` at bulk ESS 227 on `state-transition-v5`, with r-hat 1.026 and no divergences; a longer run does not fit in memory), on the assist rows of `imputed_fielding_credit` but not the putout rows, and on the `ball_handler_position`, `location_depth`, `location_edge`, and `trajectory` rows of `scorer_observation_propensities` but not the `general_location` and `location_side` rows; it is FALSE on every row of the other seven populated tables <!-- src: tables/validation_gates.md -->. The park-factor fit is unflagged on evidence rather than by omission: the non-centered gap-aware refit's `rho_park` and `sigma_park_innov` carry bulk ESS 3,261 and 1,891 at r-hat under 1.001 <!-- src: artifacts/statistical/bayes/park_factor_runs/pf-full-ar1-v4/validation/diagnostics_by_variable.json -->. Because the flag is carried per row, both TRUE and FALSE rows exist inside `imputed_fielding_credit` and `scorer_observation_propensities`, and a consumer filtering on it must do so at row grain.

**Model F's AR(1) chain never resets at park reconfigurations.** The previously published fit treated consecutive fitted seasons of a park-league as adjacent AR steps whatever the calendar gap between them, so one park's 1965 and 1998 seasons were one step apart; the refit (`pf-full-ar1-v4`) is gap-aware, with the innovation over a gap of $d$ seasons following the stationary AR(1) bridge $\rho^d$ and no latent cells for unobserved seasons <!-- src: bc/python_models/statistical/models/park_factor.py -->. What remains is the reset: `park_episode_status` is 100% NULL in both dataset artifacts, so there is no `park_episode_id` to key the chain on; it keys on `(park, league)` alone and runs continuously across a mid-history reconfiguration that should have broken it <!-- src: notes/followups.md --> <!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->. The runs-per-plate-appearance exposure also attenuates the factor relative to a runs-per-inning definition, since plate appearances are themselves inflated by scoring. Unblock condition: the upstream park-history dimension populates a non-null `park_episode_id`, and the chain is keyed on it.

**Model H (advancement) is schema-only.** `model_input_advancement.sql` does not yet emit the dependent variable `advancement_class` or a `time_forward_fold` column, so the advancement DL and Bayes specs no longer register on import <!-- src: notes/followups.md -->. Unblock condition: close the SQL gap upstream (apply the same seven-class advancement derivation the pretrain heads already use for `r1/r2/r3_advancement`), then re-register the specs and restore the target's test suite.

**Model C's error-credit arm is code-complete but data-blocked, and its double-play submodel has no truth column.** The error model (`models/error_credit.py`) fits on the 356K attributed error events but produces an empty production export: every one of the 16.3M error rows in `model_input_fielding_credit` already carries `unknown_credit_need = 0` — the upstream parser emits no unknown-error allocation signal, so there is nothing left to impute <!-- src: notes/followups.md -->. Separately, the double-play submodel has no DP truth to train against: `double_plays` exists upstream in `event_player_fielding_stats` but is not surfaced into the modeling dataset, and there is no `outs_on_play` column to gate "two outs recorded on this play" <!-- src: notes/followups.md -->. Unblock condition for errors: an upstream parser or SQL change that emits an unknown-error allocation need, or a team-game box-residual anchor built from `aggregate_residual_errors`. Unblock condition for double plays: surface `double_plays` and an `outs_on_play` column into `model_input_fielding_credit`.

**Model G's context-neutral linear weights are deferred on an estimand ambiguity, not a modeling gap.** The spec's headline context-neutral `P_LW` integrates the end-state value against the modeled marginal transition `P_LW(end | start)` rather than the realized end state. That estimand is ambiguous at the per-play-type grain, because `P_LW` is keyed on the start state alone and so cannot distinguish play types that share a start state <!-- src: notes/followups.md -->. The Markov submodel that would feed `P_LW` is built; only the estimand decision is open. Unblock condition: pin the context-neutral estimand precisely, or confirm the standard marginal linear-weights surface — built, published, and re-derived above — is the intended deliverable and drop the context-neutral variant from scope.

**Model J's cell-grain summary model has a degenerate source-partial-pooling level.** `model_input_pitch_summary` is single-source — `source_family` and `source_type` each take exactly one distinct value in the modeling dataset — so a partial-pooling level over source is mathematically present in the model but carries no information <!-- src: notes/followups.md -->. Unblock condition: a dataset or SQL change that surfaces more than one source family into the pitch-summary modeling dataset.


## Reproducibility

Most tables in this paper are queries against `bc.db`, a DuckDB database built
entirely through SQLMesh — `MODEL` blocks and Python `@model` decorators, no
ad hoc writes — from 45 source parquet files and the coverage pipeline's
`model_input_*` datasets. <!-- src: CLAUDE.md --> <!-- src: .claude/rules/sqlmesh.md -->
The queries and outputs behind every number are checked in alongside the
prose: `notes/paper/queries/<name>.sql` is the exact query, and
`notes/paper/tables/<name>.md` is its output against the published database,
for each of the SQL-query-backed tables cited in §7. A further set of tables —
the ones behind §5 and §7's Validation subsection — are recipe-driven rather
than single-query outputs, and each carries its own regeneration command inline
in `notes/paper/tables/<name>.md`: `just validate-gates` (read-only; sweeps
every published artifact pointer through `validate_artifact` and reports the
gate-status table and the state-transition and run-expectancy
predictive-coverage numbers; `--write` is the only path that stamps a
manifest's `validation_status`, gate version, and weak-identification flag),
`uv run --group stats python scripts/mnar_anchor.py --run-id <id>` (reads the
frozen geometry dataset and the published trajectory export read-only and writes
the per-era derived-slice bounds and the MAR-on-derived diagnostic), `just
sensitivity-ribbon --bound <anchor-run-dir>` (the per-era ribbon and the offset
at which it reaches the bound), and `just mnar-backtest --model geometry --smoke
--mask-design {w_class_intensity,covariate_joint,scorer_blocked,era_graded}`
(the masked backtest across the four designs — the recorded numbers are
smoke-budget runs, the flag is part of the command that reproduces them, and no
run artifacts are checked in, so a reader must rerun to re-check them). None of
these writes to `bc.db` or mutates any published artifact.

Two game-level partitions are in use and a reader reproducing a held-out number
must pick the right one. Every Bayes fit and every coverage gate holds out fold 0
of `blake2s(game_id) % 10` (`splits.game_hash_fold`); the deep supplements
train, early-stop, and report on the `HASH(game_id) % 100` `TRAIN` / `VALIDATE`
/ `TEST` partition the modeling datasets carry as `primary_fold`, with their
out-of-fold predictions assigned by `blake2s(game_id) % 5` inside `TRAIN`. The
two hashes are unrelated. <!-- src: bc/python_models/statistical/splits.py --> <!-- src: bc/python_models/statistical/deep/training.py -->

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


