---
title: "Estimating the Unrecorded Game"
subtitle: "Coverage, Bayesian Models, and Explicit Imputation for Historical Baseball Play-by-Play"
date: "September 13, 2026"
---

## Abstract

Historical baseball play-by-play records outcomes much more consistently than
pitch sequences, batted-ball geometry, fielding attribution, or game conditions.
We distinguish the process that generates a game from the process that records
it, then combine hierarchical models, deterministic constraints, and explicitly
labelled empirical fallbacks over 18,141,020 events in 205,886 acquired
play-by-play games from 1903 through 2025. The imputation candidate maps 116
target fields to sixteen additive database surfaces, including games before
1989; games known only through box scores or season totals are outside its scope.

The statistical contribution is a treatment of selective observation and its
limits. Deterministically recoverable ground balls constrain the missing
trajectory distribution from below, but do not identify a missing-not-at-random
selection offset. Historical model experiments illustrate this distinction;
their older validation labels do not establish current calibration or historical
transport. Under the current evidence contract, all 24 retained legacy artifact
pointers are explicitly exploratory, with failed overall reports and unsupported
provenance. The full-history candidate passes population, source-preservation,
normalization, and reconciliation checks. These establish implementation
consistency, not reconstruction accuracy. A separate season holdout of 1,000
games demonstrates the roughness of context imputation: attendance error averages
10,227 people and start-time error 234 clock minutes. Recorded values, estimates,
method labels, uncertainty summaries, and unresolved conflicts remain distinct.
The result is a reviewable imputation resource and a framework for stating what
its evidence supports, rather than a claim to recover the unrecorded game as fact.

## 1. Introduction

A historical play-by-play account can identify the batter, the play result, and
the change in base-out state while omitting the pitches, the ball's trajectory,
or the fielder who made an out. These omissions limit comparisons across eras:
a statistic computed only where detail survives describes a selected part of
the archive. An estimate can make the remaining record usable, but its meaning
depends on why the detail is missing and on the assumptions used to fill it.

This paper studies 18,141,020 events in 205,886 acquired play-by-play (PBP) games
from 1903 through 2025. Of these, 119,335 games and 10,308,542 events precede
1989. The earliest decade contributes only 41 games; the calendar span is not a
claim of complete historical acquisition. We target fields on actual PBP rows.
Reconstruction from box scores or season totals is a separate problem because
it requires inventing the event spine as well as its missing details.

Two coupled processes produce the archive. The game generates a trajectory,
location, sequence of pitches, and sequence of fielding actions. Scorers,
sources, translators, and parsers determine which aspects become recorded data.
A source may omit a label precisely when another part of the account makes it
redundant. In the historical geometry dataset, the pre-1950 unrecorded slice
contains 763,993 ground balls deducible from fielding strings, compared with
712,469 explicitly recorded trajectories of all classes. The derived cases
impose a lower bound on the unrecorded ground-ball share; they do not make that
slice a random sample or identify its remaining composition. Section 5 develops
this argument, and Section 7 identifies the dated evidence behind the counts.

The work has two connected parts. The first is a family of hierarchical
coverage models for observation propensity, geometry, fielding credit, handlers,
park factors, run expectancy, transitions, and pitch summaries. These models
make assumptions and uncertainty explicit, but the available experiments do not
establish historical identification or calibrated reconstruction across eras.
The second is an additive imputation layer that covers the acquired PBP schema
using those retained estimates where available and declared rule-based,
empirical, transported, or neutral fallbacks elsewhere. This second part accepts
rough estimates while requiring their estimated status to remain visible.

The contributions are a taxonomy at event-dimension grain; a selection-model
sensitivity formulation that separates lower bounds from point identification;
a full-history PBP field registry and imputation implementation; and an evidence
contract that separates numerical convergence, predictive performance,
calibration, transport, identification, and provenance. The results include
failed and unsupported evidence. Passing a conservation check, or successfully
materializing a database table, does not turn an assumption into an observation.

The manuscript's evidence cutoff is September 13, 2026. Earlier fit summaries
are retained as dated research observations. The new PBP surfaces and migrated
legacy wrappers have been exercised in isolated database copies; production
promotion and public publication of this candidate remain pending. Throughout,
"imputed" describes estimated data, including rough reconstructions, and
"observed" is reserved for recorded source evidence.

## 2. The record and its gaps

The database combines source families recorded at different grains. PBP carries
event and event-player detail; box scores carry official game-player and
game-team totals; gamelogs establish game occurrence and outcomes; season
supplements carry season aggregates. The project builds its model layer through
SQLMesh over source Parquet files. An official total, a deterministic deduction,
an expected credit, and a sampled sequence remain distinct quantities.

The current source snapshot contains 205,886 PBP games and 18,141,020 events
spanning 1903-2025. Before 1910 there are 41 games and 3,262 events. The historical manuscript's 205,845-game count covered
1910 onward while its event count included the early games; this revision uses
the same full PBP population for both counts. Games represented only by box,
gamelog, or season records receive no generated event rows. Available box totals
may constrain attribution within an existing PBP game without bringing box-only
games into the target population.

The field registry classifies 179 columns across ten source relations: 116
imputation targets and 63 bookkeeping or derived duplicates. In the baseline,
28 targets contain null or unspecified values; accounting for whole missing
pitch blocks raises that count to 35. A target can be complete in the source and
still belong in the registry because preserving its observed value is part of
the output contract. Coverage is reconciled by era, league, source type, and
game type, rather than inferred from a single season-level flag.

A missing value does not have one meaning. An absent pitch block differs from an
unknown token within an otherwise recorded sequence. A fielding event may have
known total outs but unknown player attribution. A missing secondary umpire
identity may mean the role did not exist or was not recorded. A contact-strength
code of `Default` is unspecified or neutral; treating it as an unobserved
`Hard` or `Soft` category would impose a false taxonomy. The imputation layer
preserves these distinctions in source values and method or disposition fields.

Geometry requires particular care. Earlier artifacts called six within-zone
angle modifiers `location_side`; they do not encode global field side. The
legacy consumer now exposes that vocabulary as `location_angle`. The full PBP
candidate derives global side, depth, and edge jointly from general location,
with separate treatment of angle modifiers. Counts and performance scores for
the old target cannot be read as evidence for global-side reconstruction.

The older modeling datasets contain two unrelated game partitions. Deep
supplements use DuckDB `HASH(game_id) % 100`: buckets 0-69 for training, 70-84
for validation, and 85-99 for testing. Bayesian fits and coverage gates hold out
fold 0 of a ten-fold BLAKE2s hash, removed before subsampling. The deep pipeline
also uses five-fold BLAKE2s out-of-fold predictions within its training set.
These are different boundaries, not a common outer holdout. Section 6 explains
why cross-fitting within one stage is insufficient to certify the whole
composed predictor as free of leakage.

Population and registry evidence is retained in the September 13 release
manifest, grouped coverage report, and completion registry. The evidence ledger
accompanying this paper binds these reports to their file hashes and separates
them from the older geometry dataset and its descriptive tables.

## 3. A taxonomy of missingness

Missingness assumptions must specify a population and conditioning set.
Assigning a single field-wide label of missing completely at random, missing at
random, or missing not at random is inadequate for this record. A single event can be complete for basic
batting, treated under an MAR working model for trajectory given scorer and result,
exposed to an MNAR risk for hit location, structurally absent for pitch sequence, and
merely aggregate-only for fielding credit, all at the same time. The unit that
carries a mechanism is the event-dimension, not the field and not the row, and
the mechanism is a statement about a process, not a flag.

Two coupled generative processes produce every value. The baseball process
generates the latent state: a ball has a latent contact class and geometry,
fielders handle it, runners advance, runs score, and an official scorer assigns
credit. The observation process generates the record: a source, scorer,
inputter, translator, and parser record or omit parts of that state
.
Written as a schema, the latent baseball state maps to official credit and
outcome, and the latent state together with the scorer-and-source process maps
to the recorded label or its absence
.
Most heuristics blur these two maps; separating them is the point of the
taxonomy.

Under that separation, ten missingness classes recur across the record, each
with its own detector and its own defensible imputation family
.

| Class | What it means |
|---|---|
| Structural absence | The target grain does not exist though a coarser one does - no event rows for a gamelog-only game. |
| Source-family block absence | The grain exists for some games but a whole source family or file block was never acquired. |
| Aggregate-only coverage | An official or parser aggregate exists but event attribution does not. |
| Field-level unknown | The event exists but a parsed field is `Unknown`, `0`, `Default`, or null. |
| Selection-biased detail | Detail is recorded only for non-random subsets, keyed to result salience or scorer habit. |
| Cross-source disagreement | Event-derived and box-derived totals conflict. |
| State reconstruction gap | The event exists but the state needed to interpret it is derived indirectly. |
| Official scoring convention gap | The raw facts are known but official credit follows a scoring convention, not event logic. |
| Taxonomy collapse | Raw codes exist but the wanted category is a coarser, more stable recode. |
| Sparse-context estimation | Detail exists but the conditioning bucket for adjustment is thin. |

These classes call for different responses, and conflating them destroys the
model. Structural absence and aggregate-only coverage are not imputation
targets in the event namespace at all; they stay at aggregate grain with source
flags. Taxonomy collapse is deterministic recode, not inference. Cross-source
disagreement requires an explicit authority rule or a retained conflict, rather
than a fill that silently discards contrary evidence. Field-level unknowns
take deterministic inference first, empirical priors second, and model
predictions last, always preserving the raw and imputed values separately. The
sentinels themselves must stay distinct: null, `Unknown`, `Default`, `0`,
not-applicable, aggregate-only, and known-source-issue are different markers, and
flattening them into one missing indicator discards the missingness model
.

Every imputed value therefore carries companion facts for source, method,
observed status, and confidence or weight; without them a downstream aggregation
cannot tell an official total from a deterministic derivation from a posterior
draw from a block-missing gap
.

The default working assumption for the observation models is missingness at
random conditional on context - source, scorer, inputter, translator, result,
hit-or-out state, leverage, era, and base-out state
.
Two dimensions are flagged as risks to that assumption. Hit location and detailed
contact type are treated as missing-not-at-random risks, because whether the
label was recorded may depend on what the label would have been
.
These are the selection-biased-detail class in its sharpest form, and they are
handled not by an MAR fill but by pattern-mixture sensitivity - letting the
missing values take distributions shifted from the observed ones within bounded
plausibility - the subject of the section on selection that never recorded
itself. A hierarchical model cannot rescue an unidentified estimand: where
scorer, park, team, source, and era are inseparable in a slice, the output is
tagged weakly identified, withheld, or supplied only as an explicit
assumption-based proxy
.

## 4. A family of coverage models

Two modeling layers coexist in the September 13, 2026 repository, and their
evidence must not be combined. The first is the hierarchical Bayesian and deep
learning research developed through September 4. Its retained artifacts preserve
useful specifications and development results, but all 24 isolated legacy pointers
now have gate-version-3 status `failed`; all have unsupported provenance,
identification, and transport evidence. Explicit exploratory pointer authorization
allows those bytes to be materialized for research and compatibility testing. It
does not turn them into scientifically validated estimates.

The second layer is the September 13 full-history PBP imputation candidate. It
completes the acquired play-by-play spine with recorded values, deterministic
derivations, normalized empirical distributions, constrained allocations, and
broad fallbacks. The candidate covers 205,886 acquired PBP games and 18,141,020
events from 1903 through 2025, and its registry maps 116 completion targets into
sixteen additive `pbp_imputed_*` surfaces. It passed artifact-integrity,
population, grouped-coverage, schema, and conservation checks in an isolated
consumer schema. Those checks establish complete and reproducible coverage of the
declared PBP population. They do not establish historical predictive accuracy or
convert an estimated field into historical ground truth.

### The historical model family

The earlier research organized its estimands as a lettered family. The table is
retained as a methods inventory, with current evidence status rather than its
September 4 publication status.

| Letter | Estimand | Legacy consumer or disposition | September 13 status |
|---|---|---|---|
| A | $P(\text{dimension recorded})$ per event | `scorer_observation_propensities` | exploratory wrapper; failed gate v3 |
| B | scorer contact-label confusion $\Omega$ | withheld | data uninformative (§8) |
| C | fielding credit share per position | `imputed_fielding_credit`, `assist_count_distribution` | exploratory wrapper; failed gate v3 |
| D | handling fielder position | `imputed_ball_handler_probabilities` | exploratory wrapper; failed gate v3 |
| E | batted-ball geometry class | `imputed_batted_ball_geometry` | exploratory wrapper; failed gate v3 |
| F | park run effect per season-league | `park_factor_summary` | exploratory wrapper; failed gate v3 |
| G | run expectancy, base-out transition, run values | three legacy consumers | exploratory wrapper; failed gate v3 |
| H | runner advancement class | typed empty legacy surface | no retained fit |
| I | fielder responsibility | withheld | required source input absent (§8) |
| J | final-count coverage and distribution | coverage typed empty; summary wrapped | summary failed gate v3 |
| K | shift propensity | withheld | designed, not fit (§8) |

The 19 Bayesian pointers lack their declared `inference/prior_predictive.nc`
files. Their current reports can preserve old numerical and limited predictive
results, but the missing files and incomplete dependency lineage block provenance.
No retained wrapper passes all six gate-v3 dimensions: numerical, predictive,
calibration, transport, identification, and provenance.

### Shared likelihoods and pooling

The old family used three main likelihood shapes. Recording indicators were
Bernoulli logistic models,

$$R_i \sim \operatorname{Bernoulli}(p_i), \qquad
  \operatorname{logit} p_i = \eta_i.$$

Categorical quantities such as contact geometry, handler position, credited
fielder, and end state used reference-class softmax models,

$$Y_i \sim \operatorname{Multinomial}(n_i, \pi_i), \qquad
  \pi_{i,c} = \operatorname{softmax}_c(\eta_{i,c}).$$

Count and rate quantities used negative-binomial likelihoods,

$$y_i \sim \operatorname{NB}(\lambda_i,\phi), \qquad
  \log \lambda_i = \eta_i.$$

Group effects were expressed through non-centered partial pooling,
$\beta=\sigma z$, $z\sim\mathrm N(0,1)$, and
$\sigma\sim\mathrm{HalfNormal}(s)$. These equations remain the implemented
research specifications. Their presence does not supply the missing observation
model, historical transport evidence, or artifact provenance.

Where a geometry model consumed a deep proposal, the proposal entered the
class-specific logit as

$$\eta_{i,c}=\alpha_c+\sum_j\beta^{(j)}_c[x_i]
  +\gamma\log\widetilde p^{dl}_{i,c}, \qquad
  \gamma\sim\mathrm N(0,0.5).$$

The paired `gamma_dl_zero` and `gamma_dl_shrunk` flavors were intended to measure
whether that learned covariate materially changed the Bayesian surface. The old
ablation outputs needed to reproduce the strongest cell-shift claims are not
retained, so those claims are not evidence in this revision. Section 6 records
what remains verifiable.

### Observation and selection

Model A estimated $P(R_i=1\mid x_i)$ using season, scorer, park, and event context
on the logit scale,

$$\operatorname{logit}p_i=\alpha+
  \beta^{\mathrm{season}}_{t_i}+
  \beta^{\mathrm{scorer}}_{c_i}+
  \beta^{\mathrm{park}}_{k_i}+
  \sum_jX_i^{(j)}\beta^{(j)}.$$

Whole games, rather than individual events, were assigned to the Bayesian
development holdout. Historical reports found high discrimination for whether a
field was recorded, but discrimination of $R_i$ does not identify the missing
class distribution $P(Y_i\mid R_i=0)$. Gate v3 therefore treats the old
observation-propensity numbers as dated development evidence, not proof that
inverse-propensity or missing-at-random imputation is valid. Section 5 states the
selection problem directly.

### Geometry and handler research

Models D and E fit class distributions on observed rows and scored unrecorded
rows. Their common form was

$$\pi_{i,c}=\operatorname{softmax}_c\!\left(
  \alpha_c+\sum_j\beta^{(j)}_c[x_i]+\gamma\log\widetilde p^{dl}_{i,c}
\right).$$

The strongest surviving lesson is semantic. The legacy six-class field called
`location_side` contains within-zone angle modifiers, including `Default`; it is
not a six-class global field-side target. The compatibility wrapper therefore
exposes it as `location_angle`. The September 13 PBP geometry candidate separately
derives global side from general location and preserves angle, depth, edge,
trajectory, handler, and their method fields as distinct quantities.

The PBP geometry completion uses fixed full-history donor populations and
normalized empirical distributions with explicit fallback levels. This supplies
an estimate wherever the declared PBP target is applicable. It is a coverage
policy chosen for the full-history objective, not a new fit of the old Bayesian
geometry model and not evidence that its selected class is historically correct.

Airborne subtype standardization is a separate measurement problem. Recorded
Ground versus Air and bunt status are preserved. Fly, LineDrive, and PopUp are
translated to standardized airborne bands by season, recorded subtype, and result
family. The 2009--2019 translation had useful leave-one-season-out development
evidence; the 2020-onward screen failed because a two-season pipeline could not
identify its concentration under the declared holdout; pre-2009 values depend on
explicit modern-mix and vocabulary-transport assumptions. The accepted estimate
is therefore labelled exploratory, with every pre-2009 row partially and weakly
identified. The later full-history builder extends that nearest translation to
pre-1989 rows as a weakly identified fallback. These outputs extend coverage under explicit assumptions. They do not reopen or reverse the failed
scientific screens.

### Fielding credit and aggregate constraints

The historical Model C combined a supervised event arm with aggregate box
constraints. In its stated form,

$$Y_e\sim\operatorname{Multinomial}(U_e,\pi_e), \qquad
T_m\sim\mathrm N\!\left(\sum_{e\in m}U_e\pi_{e,k},
\sigma_{\mathrm{box}}\right).$$

The supervised arm is necessary because a box total alone cannot determine which
event or eligible fielder receives the credit. The September 13 completion layer
implements the same distinction without claiming a newly validated posterior: it
preserves known official credits, constructs eligible-player candidates, and
accepts an allocation only when official residual capacity and event demand have
an exact compatible integer assignment. Contradictory or incomplete cases retain
an explicit disposition. Conservation certifies the allocation arithmetic, not
the identity of an otherwise unobserved historical fielder.

### Park, run expectancy, transitions, and pitches

The legacy park model used a team-game negative binomial with plate-appearance
exposure, season-league centering, and an AR(1) park history,

$$\theta^{\mathrm{raw}}_{p,t}=\rho^d\theta^{\mathrm{raw}}_{p,t-d}
  +\varepsilon_{p,t},$$

with a gap-aware stationary bridge for an observed gap of $d$ seasons. The chain
still lacks a park-episode reset because the required episode field is absent.
The historical run-expectancy model used a negative binomial over base-out cells;
the transition and pitch-summary models used structurally masked softmaxes so
impossible out decreases and impossible terminal counts carried no free class
parameter. These are useful specification corrections retained from the September
4 research. Their wrapper artifacts now fail gate v3, so old convergence and
held-out values are reported only as dated development findings.

The full-history PBP candidate reuses existing estimates when available and then
backs off to declared season, league, state, park, or corpus donors. Transition
fallbacks transport whole normalized vectors; linear weights may use deterministic
fallbacks. Pitch reconstruction preserves raw sequences and parser statuses,
fills only the declared PBP appearances, and retains source-conflict categories.
These operations were checked for row coverage, normalization, and rollup
conservation. Their historical accuracy remains unconfirmed.

### A scalar random effect under softmax is worth nothing

One algebraic result survives independently of any artifact verdict. A season,
scorer, or park effect added as one scalar to every class logit has no effect:

$$\operatorname{softmax}(x+c\mathbf 1)=\operatorname{softmax}(x).$$

Such a term cannot represent class-specific scorer or era behavior. It only adds
an unidentified posterior direction and potential sampling difficulty. A useful
group effect in a categorical model must vary by class or enter a different part
of the observation process. This correction remains a valid model-design result
even though the refitted legacy artifacts do not satisfy the current publication
evidence contract.

## 5. Selection that never recorded itself

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

### What the derived slice supports

The previous paper revision set
$\delta_{\mathrm{GroundBall}}=\log(p_{\mathrm{derived}}/p_{\mathrm{obs}})$.
Because every row admitted by that derivation was labelled GroundBall,
$p_{\mathrm{derived}}=1$, so the supposed anchor reduced to
$-\log p_{\mathrm{obs}}$. It was a function of the observed slice and supplied no
new measurement of the unknown rows. The offsets and corrected shares derived from
that construction are withdrawn.

If the deduction rule is accepted as correct on its admitted rows, it supplies a
conditional lower bound:

$$P(\mathrm{GroundBall}\mid R_i=0)
\geq\frac{n_{\mathrm{derived\ ground}}}{n_{\mathrm{unrecorded}}}.$$

The September 4 ledger gives floors of 0.292 before 1950, 0.339 in 1950--1987,
and 0.400 from 1988 onward. These are floors conditional on the rule's validity
and the ledger's population definition. They do not prove MNAR, identify a point
share, or validate the derivation on rows without independent labels. No analogous
deduction supplies a floor for fly balls, line drives, pop-ups, or bunts.

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

## 6. Deep-learning supplements

### Intended role

The intended architecture uses a deep classifier as a proposal distribution,
never as recorded truth:

$$\text{deep proposal}\longrightarrow
\text{out-of-fold probabilities}\longrightarrow
\text{Bayesian layer}\longrightarrow
\text{estimated surface}.$$

An argmax class is insufficient because it discards uncertainty. In the legacy
geometry models the proposal entered as
$\gamma\log\widetilde p^{dl}_{i,c}$, with one scalar $\gamma$ shared across
classes and a $\mathrm N(0,0.5)$ prior. Separate `gamma_dl_zero` and
`gamma_dl_shrunk` flavors were meant to show whether the learned proposal added
signal beyond the hierarchical context model.

This remains a design, not current publication evidence. The isolated September
13 migration contains four deep geometry pointers and one event-universe pretrain
pointer. All five have overall status `failed`. Each geometry proposal has
numerical evidence marked passed but predictive and calibration evidence marked
failed; transport, identification, and provenance are unsupported. Every evidence
dimension for the pretrain pointer is unsupported. Explicit exploratory reasons
make the compatibility pointers well formed, but do not validate the models.

### Target semantics and missing proposals

The old target named `location_side` was misdescribed. Its six categories are
within-zone angle modifiers, including `Default`, and are now exposed by the
legacy wrapper as `location_angle`. Development scores for that target cannot be
interpreted as global left/center/right field-side reconstruction. In the new PBP
geometry interface, global side is derived from general location and remains
separate from angle, depth, and edge.

The legacy location proposal specifications also scored observed rows but did not
supply predictions on the unrecorded production rows they were intended to help
impute. Centering a missing zero vector by the training-class mean then introduced
a constant class-specific logit shift. The repair made a missing proposal
contribute zero and selected deep-free flavors where no production proposal
existed. This is a valid implementation finding. It does not make the repaired
legacy artifacts pass the current evidence contract.

### What the retained development reports support

The September 4 review reported modest TEST-partition improvements over
result-family marginals for trajectory, angle, depth, and edge. It also reported
larger held-out gains when the trajectory proposal entered the Bayesian model.
Those figures remain dated development observations. The strongest ablation claims
from the previous draft depended on zero-flavor artifacts and per-cell shift files
that are no longer retained, so their exact log-loss deltas and shares of cells
moving more than 0.25 posterior standard deviations are omitted here.

Gate version 3 now requires aligned model and baseline probabilities on identical
`event_key + partition` rows, declared class truth, improvement in log loss and
Brier score, and classwise calibration evidence. The four migrated deep pointers
fail because the retained artifacts do not provide acceptable baseline and
calibration evidence under that contract. An older numerical pass or a top-1
accuracy improvement cannot substitute for these missing checks.

### Leakage in the legacy stack

Two distinct leakage paths prevent confirmatory interpretation. First, the shared
event-universe pretrain used geometry labels from the whole `TRAIN` partition,
then downstream fold models warm-started embeddings from that fit. A nominally
out-of-fold row could therefore influence its own embedding through pretraining.
The deep partition and Bayesian game-hash holdout were unrelated, so this
contamination also reached part of the Bayesian held-out evaluation.

Second, a trajectory target configured with `fold_count=1` followed a full-fit
fallback while labelling its predictions as out of fold. The later five-fold
repair addressed that direct error, but it did not repair the shared-pretraining
leak or reconstruct complete lineage for the retained artifacts. The dependency
manifest needed to identify exactly which pretrain produced each proposal is also
absent.

The scientific cross-fitting contract is stricter than a prediction-scope label.
Every supervised stage that can encode the target must be fit inside the outer
training games. Predictions must be generated on disjoint games, carry explicit
scope and class labels, and be compared with a baseline on the same rows. Any
calibration stage must also be trained without the evaluation rows. The retained
legacy stack does not demonstrate that complete contract.

### Relation to the September 13 completion layer

The full-history PBP imputation candidate does not require a new deep fit. It uses
recorded values, deterministic derivations, empirical donor distributions,
constrained reconciliation, and explicit broad fallbacks. Legacy probabilities
may be carried as exploratory inputs where configured, but a complete candidate
row does not inherit scientific validation from an old pointer. The builder keeps
the method and artifact identity visible so consumers can distinguish a recorded
fact, a deterministic rule, a transported estimate, and a broad prior.

This separation is deliberate. The coverage objective accepts rough estimates across the acquired historical
PBP population. More deep training would answer a different
question and would require fresh lineage, leakage-free outer folds, retained
baseline predictions, calibration, transport tests, and untouched confirmation
data. None of that work was performed for the September 13 candidate.

## 7. Results and evidence status

The results separate historical model summaries from the September 13 PBP
candidate. The earlier tables describe a frozen research generation; the new
reports measure coverage and implementation consistency. Neither supplies
unobserved historical ground truth. The paper's evidence ledger identifies the
reports and their hashes, and retains the older table snapshots with their dates.

### Full-history PBP coverage

The selected candidate covers 205,886 games and 18,141,020 events from 1903-2025,
including 119,335 games and 10,308,542 events before 1989. Every existing PBP game
and event has an output row in its corresponding combined interface. This is
complete coverage of the acquired PBP population, not of every major-league game
that occurred. The registry maps all 116 targets to their output and evidence
columns. Source preservation, explicit non-applicability, and unresolved
conflicts are valid dispositions; coverage does not mean every field becomes a
known historical value.

Eleven component artifacts feed sixteen database surfaces. Component row counts
have different grains and must not be added as if they were distinct events.

| Component | Rows | Grain |
|---|---:|---|
| Game context | 205,886 | PBP game |
| Pitches | 18,141,020 | Event increments within appearances |
| Event values | 18,141,020 | PBP event |
| Geometry | 12,041,398 | Applicable batted-ball event |
| Runners | 17,411,649 | Runner record |
| Fielding | 15,719,352 | Fielding-play record |
| Officials | 1,441,202 | Game and official role |
| Park factors | 72,930 | Park, league, season, and metric |
| Run expectancy | 8,473 | PBP base-out context |
| State transitions | 211,825 | Start context and end state |
| Linear weights | 7,640 | League, season, and play |

Source: selected component metadata and the release-candidate manifest,
September 13, 2026. The 12,041,398 geometry rows use the full PBP scope and
current eligibility definition; the older geometry fit population below is a
separate dataset generation.

The combined interfaces are `pbp_imputed_games` and `pbp_imputed_events`.
Specialized surfaces expose game context, geometry, pitches, individual pitch
items, pitch totals, runners, fielding plays, fielding totals, officials, event
values, park factors, run expectancy, state transitions, and linear weights.
All use the `main_models.pbp_imputed_*` namespace. They preserve recorded values
and make imputation methods available for downstream filtering or aggregation.

The full validator found no missing or duplicate game/event keys, population
count differences, or non-PBP additions. All sixteen SQLMesh consumer audits passed, as did
materialized artifact-identity checks and the field mapping checks. Coverage by
era, league, source, and game type reconciled with the baseline for every target.
These are structural guarantees: they do not assess whether a missing value has
been recovered accurately.

### Reconstruction constraints and disclosed conflicts

Pitch validation covers 15,822,702 appearances across all 18,141,020 events.
The independent audit found zero counter discrepancies across seventeen checks,
zero unknown tokens, zero unclassified dispositions, and zero unflagged
appearance violations. Separately disclosed source violations include 26,996
ball-boundary cases, 51,245 strike-boundary cases, 816 post-terminal cases,
and 375 terminal-outcome cases. Categories overlap and are not counts of
distinct bad games. A consistent constructed sequence is one possible sequence
under the rules and assumptions, not a recovered historical sequence.

Runner end-state identity resolves 634 of 642 missing non-out destinations;
eight terminal or frame conflicts remain explicit. Fielding allocation uses
known credits and compatible official totals within existing PBP games. An
allocation is called successful only when the integer capacities match and the
allocation delta is zero. Merely having aggregate inputs is insufficient.
Contradictory evidence is retained rather than overwritten to force agreement.
Official identities use contemporaneous candidate support when available;
unresolved slots do not create fictitious people.

Geometry preserves a joint general-location, global-side, depth, and edge
representation. Historical angle modifiers remain separate. Pre-1989 airborne
standardization transports the nearest available 1989 translation backward,
with a weak-identification flag. The selected geometry repair fills 1,123 early
non-bunt standardization gaps; 157,180 bunts remain explicitly outside that
standardization target. These additions extend computational coverage under the
existing airborne research assumptions, without adding confirmation evidence.

Derived run and win changes, park factors, run expectancy, transitions, and
linear weights retain existing nonmissing values or estimates. Their remaining
gaps use declared nearby-season, pooled, deterministic, or neutral fallbacks.
Whole transition vectors are transported together and normalized. Conditional
donor variation and transferred uncertainty summaries do not cover all model,
era-transport, or source-selection uncertainty.

### Recorded-context season holdout

A separate stress test jointly masks ten context fields in a 1,000-game sample
from complete held-out seasons 1903, 1910, 1940, 1960, 1980, 2000, and 2025.
Each target's donor pool excludes those seasons. The denominators below count
only held-out games with a recorded value for that target. The audit detects no
held-out source values reused as donors.

| Numeric target | Recorded test values | Mean absolute error |
|---|---:|---:|
| Temperature | 543 | 7.53 degrees Fahrenheit |
| Attendance | 944 | 10,227 people |
| Wind speed | 403 | 4.32 mph |
| Start time | 578 | 234.34 clock minutes |
| Game duration | 999 | 25.67 minutes |

| Categorical target | Recorded test values | Exact agreement |
|---|---:|---:|
| Time of day | 1,000 | 48.3% |
| Sky | 541 | 48.4% |
| Field condition | 78 | 87.2% |
| Precipitation | 282 | 94.0% |
| Wind direction | 396 | 19.7% |

Source: recorded-context holdout report, September 13, 2026. Agreement is not
adjusted for a majority-class baseline. This intentionally difficult masking
exercise removes context often present when one field is missing. The large
start-time and attendance errors make the practical limitation concrete:
coverage can be useful while individual reconstructions remain very rough.
The experiment does not estimate error on the historically missing stratum,
validate airborne labels, or calibrate total uncertainty.

### Historical trajectory evidence

The earlier geometry dataset separates recorded trajectories from trajectories
deduced from fielding strings and genuinely unknown cases. All derived cases in
this dataset are ground balls. Its retained table reports:

| Era | Unrecorded trajectories | Derived ground balls | Ground-ball floor | MAR probability on derived cases |
|---|---:|---:|---:|---:|
| Before 1950 | 2,615,579 | 763,993 | 0.2921 | 0.3203 |
| 1950-1987 | 3,117,160 | 1,057,776 | 0.3393 | 0.4020 |
| 1988 onward | 134,970 | 53,922 | 0.3995 | 0.3751 |

Source: historical `groundball_mnar` table and frozen geometry dataset used by
the September 4 manuscript. The floor divides derived ground balls by all
unrecorded trajectories. It is conditional on the correctness of the deduction
rules and this target vocabulary. The last column evaluates a missing-at-random
(MAR) fit on known ground balls, whose target indicator is one; it is not the
unknown stratum's true ground-ball share. The selected nature of the derived
slice prevents identifying that share from this comparison alone.

For the pre-1950 slice the old fixed-offset sensitivity table ranges from
0.162 to 0.530 over a plus-or-minus-one-nat grid, with the 0.292 floor inside it.
This is an assumption sweep, not a confidence interval or a data-estimated
selection offset. Historical masked experiments in Section 5 were smoke-budget
diagnostics; their outputs are not a substitute for a retained confirmatory
experiment. The supporting tables remain useful for explaining the derivation,
but do not earn a present-day validation pass.

### Legacy model evidence under the current contract

The retained legacy statistical consumers contain ten populated families and
two typed-empty optional surfaces. For example, geometry contains 253,013,169
class-probability rows, fielding credit 8,335,674 rows, and handler probabilities
11,712,096 rows. These counts reflect class-level expansion and differ from the
new PBP component grains. The legacy six-class angle vocabulary accounts for
41,938,992 geometry rows, exposed as `location_angle`, with no rows mislabelled
as global `location_side` in the isolated migration check.

The current migration creates 24 explicitly exploratory pointers. Every saved
version-3 report has overall status `failed` and provenance `unsupported`;
numerical and some predictive components can pass within that failed overall
assessment. Missing files, incomplete dependencies, unsupported calibration,
and transport or identification gaps are not repaired by wrapping retained
payloads in new manifests. The isolated consumer rehearsal passes all twelve
SQLMesh consumer audits and 56 identity, schema, count, and probability checks. Those
results establish correct consumption of the selected exploratory artifacts,
not statistical acceptance.

The September 4 version-2 table reported 19 passing registered gate targets and
four unresolved deep-pointer targets. It also reported observation-propensity
ECE from 0.0049 to 0.0181, and interval diagnostics of 0.9780 for transitions
and 0.9062 for run expectancy. These are dated diagnostics under the older
contract. In particular, the run-expectancy interval uses held-out sample
variance and a Normal mean approximation; it is now labelled
`studentized_mean`, not a fitted negative-binomial posterior-predictive check.
The transition calculation approximates marginal uncertainty without a full
joint posterior. Neither establishes historical calibration or joint
uncertainty for derived run values.

Later development comparisons reinforce that caution. The shared-split
geometry reference fits passed numerical diagnostics but failed their frozen
predictive criteria; trajectory also regressed on the pre-1988 observed slice.
The old location target was subsequently identified as angle modifiers, making
its scores unsuitable evidence for global-side reconstruction. These findings
support retaining explicit baselines and assumption labels. They do not support
the earlier manuscript's blanket description of the surfaces as validated or
calibrated across baseball history.

## 8. What the record cannot tell you

Complete output coverage and statistical identification are different goals. The
September 13 candidate supplies a reproducible estimate or declared fallback for
applicable targets on the acquired PBP spine. That policy does not make an
unobserved historical fact learnable from a single source. Three legacy model
letters illustrate three different limits.

### Model B - contact-label confusion

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

### Model I - analytical fielder responsibility

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

### Model K - shift propensity

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

## 9. Publication policy

Publication status and evidential support are separate properties. The model
registry distinguishes official values, deterministic derivations, estimates,
synthetic data, and withheld outputs. The current PBP layer uses the explicit
`pbp_imputed_*` names even where a view combines recorded values with estimates.
The source columns remain available, and estimates carry their method,
artifact identity, and applicable uncertainty or conflict fields. These names
make estimated status visible at the point of use.

Legacy probabilistic surfaces carry eight common provenance fields:
`artifact_id`, `model_name`, `model_version`, `source_snapshot_id`, `method`,
`observed_status`, `confidence_status`, and `weak_identification_flag`.
A posterior mean or HDI is conditional on the fitted model. Donor dispersion,
a transported interval, or a neutral fallback in the PBP layer is a different
uncertainty summary and must retain its own method label. The PBP layer is not
uniformly Bayesian and does not promise calibrated intervals for every target.

The September 11 evidence contract, gate version 3, reports six dimensions:
numerical, predictive, calibration, transport, identification, and provenance.
Each dimension can pass, fail, or remain unsupported. Default validated
publication requires current numerical, predictive, calibration, and provenance
passes. Even that status leaves transport and identification as separate
judgments. Finite metrics and positive evaluated sample counts are required;
a missing comparator, missing calibration evidence, or incomplete fitted-stage
lineage cannot be treated as a pass.

An explicit exploratory publication mode permits a research pointer with failed
or unsupported evidence and a recorded reason. It preserves the failed result.
The September 13 migration creates 24 such pointers bound to retained legacy
payloads; all have failed overall reports and unsupported provenance. Nineteen
missing prior-predictive files and other lineage gaps remain disclosed. A strict
pointer check passing means that this exploratory status and its content binding
are internally consistent. It does not mean the model passed validation.

The older paper reported version-2 `passed` labels after the September 4
restatement. Those labels describe a historical materialization under an older
policy. Updating validation code does not revise rows already stored in
production. The isolated candidate's ten populated legacy statistical families
carry failed confidence; two optional statistical surfaces remain typed empty.
This revision therefore reports the policy version, artifact generation, and
consumer environment whenever it reports a verdict.

For the full PBP candidate, publication checks enforce exact population
coverage, field mappings, artifact identities, normalization, and pitch and
fielding reconciliation. All sixteen PBP surfaces passed their structural audits in an isolated
candidate schema. Explicit conflicts remain in the data; checks require their disclosure
rather than erasing them. The publisher repeats the relevant checks against
production consumers before creating the public catalog. No production
promotion or public upload of this candidate had occurred at the paper's cutoff.

Official tables retain their meaning. Probabilistic legacy outputs remain
additive siblings of deterministic tables, and the rough PBP reconstructions are
another explicit interface. Unknown people are represented by candidate support
or unresolved slots, never fabricated person identifiers. Unidentified physical
or scoring quantities may be withheld, while practical proxies can be supplied
under declared assumptions. Neither a selected class nor a narrow conditional
interval is permission to present an unrecorded value as an observed fact.

## 10. Related work

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
formalized sample selection as a two-equation system - an outcome process and a
selection process whose errors are correlated - and showed that selection bias
is a specification error correctable by modeling the selection equation jointly
with the outcome. §5 uses a categorical selection formulation: a
latent class is drawn, then recorded with probability s(c, x), and Bayes' rule
turns the selection process into a per-class reweight of the fitted
distribution - the offset δ_c is the per-class selection log-odds, a
selection-model parameter acting on the recorded class before the softmax
normalizes. The difference from Heckman's program is what we ask of the
parameter. Heckman buys identification with parametric structure - joint
normality of the two equations' errors, or an exclusion restriction that moves
selection without moving the outcome. We have not established a credible exclusion restriction in this study.
Scorer, source, and era covariates can also proxy the game process. We therefore
treat δ_c as unidentified from the recorded labels alone and report sensitivity
to assumed values. A partial-truth slice constrains the missing ground-ball
share from below; it does not independently identify δ_c.

That sweep places the paper in the second tradition: pattern-mixture models and
delta-adjustment sensitivity analysis. Little (1993) specified the distribution
of missing values directly through identifying restrictions rather than a
mechanism, and observed that such models are chronically underidentified -
without additional restrictions the observed data do not identify the
missing stratum's distribution. Inference across alternative restrictions
therefore makes the unresolved assumption visible. Scharfstein, Rotnitzky, and Robins (1999)
made the same move inside the selection formulation: fix a nonidentified
selection-bias parameter at each value in a plausible range, estimate under
each, and report the trajectory - sensitivity analysis in place of an
untestable assumption. That template became standard practice in the
clinical-trials missing-data literature as delta adjustment and tipping-point
analysis, where a fixed offset δ is added to values imputed under a
missing-at-random fit and swept until the conclusion changes; Molenberghs and
Kenward (2007) treat the framework at book length, and Cro, Morris, Kenward,
and Carpenter (2020) give a practical guide to controlled multiple
imputation in this style. The sensitivity analysis of §5 is this device
transplanted: δ_c is a delta adjustment on the class logits, the
missing-at-random fit is the δ = 0 point, and the ribbon is the tipping-point
trajectory, reported per class. What the paper adds to the template is a hard
constraint on the sweep - a deduced-trajectory partial-truth slice internal to
the record that places a floor under one class's share on the missing stratum,
and a known-truth subslice on which the missing-at-random fit can be scored -
and historical smoke-budget masking diagnostics that illustrate the offset's
behavior under a selection process it does not nest. An earlier revision read the same slice as
an estimate of δ_c; §5 explains why it is a floor and not an estimate. The structure of the treatment - a selection-model
parameterization with pattern-mixture-style sensitivity reporting - reflects
the standard observation that the two factorizations describe the same joint
distribution and can be mixed rather than chosen between.

The observation process studied here is informative nonresponse in the survey
statistician's sense: the probability an item is recorded depends on the value
the item would have taken. Rubin (1977) formalized subjective assumptions about
nonrespondents in exactly the spirit adopted here - a parameter the data cannot
estimate, elicited and varied rather than ignored. The survey-methodology
literature collected in Groves, Dillman, Eltinge, and Little (2002) treats
nonresponse as a process with its own covariates and propensities, which is
Model A's stance toward scorers: the observation-propensity surfaces are
response-propensity models with scorers in the role of respondents, and the
event-dimension grain of §3's taxonomy is item nonresponse rather than unit
nonresponse - one event can respond on batting and not on trajectory.

The fitting methodology follows the applied Bayesian workflow. Gelman et al.
(2013) is the reference for the hierarchical models, partial pooling, prior and
posterior predictive checking, and the practice of treating a model as a
hypothesis to be checked against held-out data. Betancourt and Girolami (2013)
document the pathologies that hierarchical models present to Hamiltonian Monte
Carlo and explain why a non-centered parameterization can improve sampling
when data are sparse - the regime of most of our early-era cells - which is why
every model in the family is written non-centered.

In sports analytics, the play-by-play data itself comes from the Retrosheet
archive. The closest methodological predecessor is openWAR (Baumer, Jensen, and
Matthews, 2015), an open, reproducible player-value system that propagates
uncertainty rather than reporting only point values. Marchi and Albert (2014)
is an applied introduction to baseball data analysis. Measurement choices in
the baseball record have also been studied directly: Kalist and Spurr (2006)
find evidence of home-team bias in official scorers' hit-versus-error calls.
That result motivates treating scorer behavior as part of the observation
process, without presuming that a scorer decision can be retrospectively
corrected. Acharya et al. (2008) show that conventional park-factor formulas
can be distorted by unbalanced schedules and inflationary bias, and propose an
ANOVA weighted fixed-effects estimator. It is relevant as an example of a
baseball quantity whose estimate depends on an explicit model for the recording
and game context, rather than as evidence for this paper's hierarchical model.

Our target differs from these studies: we characterize the observation process
for event details and report uncertainty in its fitted coverage surfaces. The
full-history play-by-play output remains an imputed candidate, not a validated
historical reconstruction; the paper does not claim to recover the unrecorded
details as ground truth.

## 11. Limitations and open problems

The principal limitation is evidentiary. The September 13 work completed and
reconciled a full-history PBP candidate, while the retained September 4 model
artifacts fail the current scientific publication contract. These statements can
both be true because coverage validation and model validation answer different
questions.

**The legacy statistical family does not pass gate version 3.** All 24 migrated
pointers have overall status `failed`; provenance, identification, and transport
are unsupported for every pointer. The 19 Bayesian artifacts lack their declared
`prior_predictive.nc` files. The four geometry deep proposals fail current
predictive and calibration evidence, and the event-universe pretrain is unsupported
on every evidence dimension. The strict pointer checker passes the exploratory
wrappers because their identities, reasons, failed verdicts, and retained-byte
bindings are internally consistent. That is an integrity result, not scientific
authorization.

**The full-history candidate is a rough reconstruction of the acquired PBP
population.** It covers 205,886 games and 18,141,020 events from 1903--2025 and
maps all 116 declared completion targets. Its isolated checks establish artifact
identity, source preservation, row coverage, schema compatibility, probability
normalization, and defined conservation rules. They do not show that an
unrecorded temperature, pitch, fielder, trajectory, official, or run value was
historically correct. Games without an acquired PBP event spine, including
aggregate-only games, remain outside the estimand.

**Missingness remains unidentified for the naturally unrecorded class.** A model
fit on recorded labels estimates $P(Y\mid X,R=1)$, not
$P(Y\mid X,R=0)$. Marginal observation propensity does not recover the
class-dependent selection odds. Rule-derived ground-ball rows provide at most a
conditional lower bound and a selected diagnostic subset; they do not prove MNAR
or identify the other trajectory classes. The September 13 candidate uses broad
empirical fallbacks rather than claiming a fitted MNAR correction.

**The location target correction changes the meaning of old results.** The legacy
six-class `location_side` artifact contains within-zone angle modifiers and is
now exposed as `location_angle`. Its historical predictive scores do not validate
global field-side reconstruction. The new PBP interface derives global side from
general location and keeps angle, depth, and edge distinct, but the fallback
accuracy of those historical estimates is not confirmed.

**Airborne standardization is assumption labelled.** The 2009--2019 pipeline has
useful development support. The 2020-onward held-out screen failed under its
two-season design, and pre-2009 translation assumes a modern true-band mix and a
relationship to the 2020-onward vocabulary. Its nominal intervals omit uncertainty
in those assumptions. The PBP candidate carries the nearest 1989 translation
backward for earlier seasons with weak-identification flags. This coverage decision
was explicitly accepted as an estimate; it is not a passed transport result.

**The confirmation reserves remain sealed.** The 121 deferred modern-angle games
and 6,105 historical component-local reserve games were not opened for this paper
revision or the PBP candidate. No new model was fit and no new confirmation data
were acquired. The air-research decision remains closed under its stated
assumptions; the later failed screens and partial-identification findings are
reported without reopening the reserve.

**Legacy deep evidence is incomplete and partly contaminated.** Shared pretraining
could encode a downstream row's geometry label before nominal cross-fitting, and
the Bayesian and deep partitions were not nested. An earlier one-fold trajectory
configuration also labelled full-fit predictions as out of fold. The strongest
ablation files quoted by an older draft are not retained, so their exact effects
cannot be reproduced. A future deep supplement would need target-free outer
folds across every supervised stage, explicit dependency lineage, aligned baseline
probabilities, calibration, and transport tests before it could support a
scientific claim.

**Reported uncertainty is conditional on method and fallback.** Donor dispersion
measures empirical variation within the selected donor pool. It does not include
uncertainty that the fallback hierarchy, historical transport assumption, or source
measurement is wrong. Pre-2009 airborne intervals omit the error in their two main
transport assumptions. Legacy run-expectancy and transition interval diagnostics
use approximations rather than retained joint posterior draws, and park factors
lack a held-out coverage hook. A narrow conditional interval can therefore coexist
with weak historical identification.

**Conservation does not resolve source conflict.** Pitch and fielding reconciliation
ensures that the candidate's defined counters and allocations add up. The builder
retains parser statuses, raw sequences, and explicit conflict dispositions because
many source sequences violate modern or internal boundaries. A reconstructed legal
sequence is one coherent completion of the retained evidence, not proof of the
historical pitch order. Likewise, exact fielding capacity can constrain a credit
allocation without independently identifying the responsible fielder.

**The available context stress test shows material roughness.** In the retained
1,000-game joint holdout, mean absolute error was 7.53 °F for temperature, 10,227
for attendance, 4.32 mph for wind speed, 25.67 minutes for duration, and 234.34
clock minutes for start time; categorical exact agreement ranged from 19.7% for
wind direction to 94.0% for precipitation. This is a test against recorded values
under an artificial joint mask, not a calibrated study of historical missingness,
but it demonstrates why completed context fields should retain their methods and
donor dispersion.

**Several latent baseball quantities remain outside the supported source.** The
record does not supply an independent event-level contact label for a structured
scorer confusion matrix or a positioning/opportunity source for analytical fielder
responsibility. Shift propensity was designed but not fit. The legacy advancement
and pitch-count-coverage consumers remain typed empty because they have no retained
validated pointer. The broader PBP interfaces may still expose rough completed
values where the registry declares a fallback, but that does not resolve these
scientific estimands.

Future scientific promotion would require new artifact IDs, complete retained
lineage and prior-predictive outputs, leakage-free grouped holdouts, calibration,
source-block and era transport tests, and validation against data not used during
development. The current paper instead reports the completed PBP candidate as an
exploratory, assumption-labelled analysis layer and dates older model results to
the evidence that produced them.

## 12. Reproducibility and availability

This manuscript is assembled from the ordered Markdown files in
`notes/paper/sections/`. Its build command is `node notes/paper/build.mjs` from
the repository root. The build uses Pandoc and Typst, removes internal drafting
comments, and writes the assembled Markdown and PDF. The accompanying evidence
ledger records the source generation, report identities, and limitations of the
numbers used in the current results. Historical query files and table snapshots
remain under `notes/paper/queries/` and `notes/paper/tables/`; their dates matter.

The current candidate's source database has SHA-256
`fc94ec74507ceb0e7ab60296006de534253d364b0c9d5b64f352c82749c23f9c`.
The release manifest selects the PBP root and binds its release evidence. The
selected component manifest binds eleven data files, their SQL, frozen
implementation dependencies, source identity, and validation reports. Its geometry
input is `geometry_v2.parquet`; earlier geometry outputs remain diagnostic
artifacts and are not the selected candidate. The namespace revision changed
metadata and output mappings while preserving the eleven component payloads
byte for byte. The release code snapshot is commit `98da62c`.

Candidate paths and the exact report digests are recorded in the paper's
machine-readable evidence snapshot. The source reports are local research
artifacts, not all checked into Git or distributed with the public database.
This repository supplies code and an auditable summary; it does not yet supply
a self-contained public replication archive. In particular, several older
ablation and smoke-backtest run artifacts are not retained, and the legacy
migration cannot recover missing prior-predictive files or missing dependency
lineage. Quoted historical summaries are documentary evidence, not a claim that
every experiment can currently be regenerated from an intact frozen bundle.

The imputation builder reads its source database without modifying it. A small
sample is the default; a full build must explicitly request the full population.
The commands and artifact-selection environment variables are documented in
`docs/pbp-imputation.md`. SQLMesh materialization targets an isolated database
and state copy for candidate testing. Verification checks the actual consumer
views and artifact identities, because changing an artifact-root environment
variable alone need not change a model fingerprint. Neither a successful build
nor this paper's PDF export is a production release.

The recorded-context stress test retains its query, predictions, script, and
report. Entire selected seasons are removed from each field's donor pool before
predicting the 1,000 sampled games. The reported error denominators are the
numbers with recorded truth for each field, not 1,000 for every metric. This
experiment is separate from the older Bayesian and deep partitions described
in Section 2, and from sealed geometry confirmation data. The 121 modern-angle
games and 6,105 historical reserve games were not opened or rescored for the
PBP candidate or this manuscript revision.

The operational database is built through SQLMesh. The current public
distribution pipeline copies approved model and seed tables into a DuckLake
catalog with immutable Parquet data files, semantic views, and metric macros;
the website attaches that catalog read-only. Older standalone database objects
are retained for compatibility and rollback. This distribution mechanism does
not imply that the September 13 candidate has been released or that its local
research artifacts are downloadable from the public catalog.

## References

Acharya, R. A., Ahmed, A. J., D'Amour, A. N., Lu, H., Morris, C. N., Oglevee,
B. D., Peterson, A. W., & Swift, R. N. (2008). [Improving Major League Baseball
park factor estimates](https://doi.org/10.2202/1559-0410.1108). *Journal of
Quantitative Analysis in Sports*, 4(2), Article 4.

Baumer, B. S., Jensen, S. T., & Matthews, G. J. (2015).
[openWAR: An open source system for evaluating overall player performance in
Major League Baseball](https://doi.org/10.1515/jqas-2014-0098). *Journal of
Quantitative Analysis in Sports*, 11(2), 69-84.

Betancourt, M., & Girolami, M. (2013). [Hamiltonian Monte Carlo for hierarchical
models](https://arxiv.org/abs/1312.0906). arXiv:1312.0906.

Cro, S., Morris, T. P., Kenward, M. G., & Carpenter, J. R. (2020).
[Sensitivity analysis for clinical trials with missing continuous outcome data
using controlled multiple imputation: A practical guide](https://doi.org/10.1002/sim.8569).
*Statistics in Medicine*, 39(21), 2815-2842.

Gelman, A., Carlin, J. B., Stern, H. S., Dunson, D. B., Vehtari, A., & Rubin,
D. B. (2013). [*Bayesian Data Analysis* (3rd ed.)](https://www.routledge.com/Bayesian-Data-Analysis/Gelman-Carlin-Stern-Dunson-Vehtari-Rubin/p/book/9781439840955).
Chapman & Hall/CRC.

Groves, R. M., Dillman, D. A., Eltinge, J. L., & Little, R. J. A. (Eds.)
(2002). [*Survey Nonresponse*](https://doi.org/10.1002/0471221273). Wiley.

Heckman, J. J. (1976). [The common structure of statistical models of
truncation, sample selection and limited dependent variables and a simple
estimator for such models](https://www.nber.org/books-and-chapters/annals-economic-and-social-measurement-volume-5-number-4/common-structure-statistical-models-truncation-sample-selection-and-limited-dependent).
*Annals of Economic and Social Measurement*, 5(4), 475-492.

Heckman, J. J. (1979). [Sample selection bias as a specification
error](https://doi.org/10.2307/1912352). *Econometrica*, 47(1), 153-161.

Kalist, D. E., & Spurr, S. J. (2006). [Baseball
errors](https://doi.org/10.2202/1559-0410.1043). *Journal of Quantitative
Analysis in Sports*, 2(4), Article 3.

Little, R. J. A. (1993). [Pattern-mixture models for multivariate incomplete
data](https://doi.org/10.1080/01621459.1993.10476494). *Journal of the American
Statistical Association*, 88(421), 125-134.

Little, R. J. A., & Rubin, D. B. (2019). [*Statistical Analysis with Missing
Data* (3rd ed.)](https://doi.org/10.1002/9781119482260). Wiley.

Marchi, M., & Albert, J. (2014). [*Analyzing Baseball Data with
R*](https://doi.org/10.1201/b17044). Chapman & Hall/CRC.

Molenberghs, G., & Kenward, M. G. (2007). [*Missing Data in Clinical
Studies*](https://doi.org/10.1002/9780470510445). Wiley.

Retrosheet. (n.d.). [Retrosheet data downloads](https://www.retrosheet.org/game.htm).

Rubin, D. B. (1976). [Inference and missing
data](https://doi.org/10.1093/biomet/63.3.581). *Biometrika*, 63(3), 581-592.

Rubin, D. B. (1977). [Formalizing subjective notions about the effect of
nonrespondents in sample surveys](https://doi.org/10.1080/01621459.1977.10480550).
*Journal of the American Statistical Association*, 72(359), 538-543.

Scharfstein, D. O., Rotnitzky, A., & Robins, J. M. (1999). [Adjusting for
nonignorable drop-out using semiparametric nonresponse
models](https://doi.org/10.1080/01621459.1999.10473862). *Journal of the
American Statistical Association*, 94(448), 1096-1120.
