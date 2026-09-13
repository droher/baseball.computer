## Results and evidence status

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
