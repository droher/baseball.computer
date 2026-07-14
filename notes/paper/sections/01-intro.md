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
