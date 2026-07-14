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
