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
