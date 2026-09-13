## A family of coverage models

Two modeling layers coexist in the September 13, 2026 repository, and their
evidence must not be combined. The first is the hierarchical Bayesian and deep
learning research developed through September 4. Its retained artifacts preserve
useful specifications and development results, but all 24 isolated legacy pointers
now have gate-version-3 status `failed`; all have unsupported provenance,
identification, and transport evidence. Explicit exploratory pointer authorization
allows those bytes to be materialized for research and compatibility testing. It
does not turn them into scientifically validated estimates.
<!-- src: docs/modeling-evidence-contract.md -->
<!-- src: artifacts/imputation/legacy-publication-candidate-v1/migration_summary.json -->

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
<!-- src: docs/pbp-imputation.md -->
<!-- src: artifacts/imputation/20260913-pbp-imputed-v1/validation/candidate/candidate_validation.json -->

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
<!-- src: artifacts/imputation/legacy-publication-candidate-v1/README.md -->
<!-- src: artifacts/imputation/legacy-publication-candidate-v1/migration_summary.json -->

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
<!-- src: notes/data-coverage-implementation/03-hierarchical-models.md -->

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
<!-- src: bc/python_models/statistical/splits.py -->
<!-- src: docs/modeling-evidence-contract.md -->

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
<!-- src: docs/modeling-evidence-contract.md -->
<!-- src: artifacts/imputation/legacy-publication-candidate-v1/README.md -->
<!-- src: docs/pbp-imputation.md -->

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
<!-- src: docs/geometry-modeling-handoff.md -->
<!-- src: docs/geometry-air-translation-estimate-2026-09-11.md -->
<!-- src: docs/pbp-imputation.md -->

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
<!-- src: docs/pbp-imputation.md -->

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
<!-- src: bc/python_models/statistical/models/park_factor.py -->
<!-- src: bc/python_models/statistical/models/state_transition.py -->
<!-- src: docs/modeling-evidence-contract.md -->

The full-history PBP candidate reuses existing estimates when available and then
backs off to declared season, league, state, park, or corpus donors. Transition
fallbacks transport whole normalized vectors; linear weights may use deterministic
fallbacks. Pitch reconstruction preserves raw sequences and parser statuses,
fills only the declared PBP appearances, and retains source-conflict categories.
These operations were checked for row coverage, normalization, and rollup
conservation. Their historical accuracy remains unconfirmed.
<!-- src: docs/pbp-imputation.md -->

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
<!-- src: notes/data-coverage-implementation/implementation-review.md -->
