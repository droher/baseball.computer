---
title: Paper plan — Estimating the Unrecorded Game
type: design-doc
status: draft
audience: writer agents, David
last-verified: 2026-07-13
---

# Paper plan

Working title: **"Estimating the Unrecorded Game: Hierarchical Bayesian Coverage
Models for a Century of Baseball Play-by-Play"**. Genre: applied-statistics paper
(JQAS / AOAS register). Audience: sports-analytics researchers and applied
Bayesian statisticians. Total target: 7,000–9,000 words plus tables.

Thesis: the historical play-by-play record is the output of two coupled
processes — the game and its observation. Modeling the observation process
explicitly (who recorded what, when, and why) turns "missing data" from a
cleaning nuisance into an estimand, yields calibrated probabilistic surfaces
where deterministic imputation would fabricate certainty, and — just as
important — tells you which quantities are *unidentifiable* from a
single-source record and must not be published.

## Ground rules for every writer

- Every quantitative claim carries an HTML comment `<!-- src: <repo path or
  tables/<file>> -->`. No number without a source. If you cannot find a source,
  write `<!-- TODO: unverified -->` instead of inventing.
- Prose: declarative, present tense, no hype ("novel", "state-of-the-art"
  banned). Plain words. Reasoning inline, dash-connected. No bullet-broken
  paragraphs; bullets only for true enumerations, tables for comparisons.
- Notation (use exactly this):
  - Event index i; geometry dimension d; class c; K classes.
  - R_i ∈ {0,1} observation indicator; Model A estimates P(R_i=1 | x_i).
  - Softmax logits η_{i,c}; DL proposal term γ_c · log p̃^dl_{i,c}, γ_c ~ N(0, 0.5).
  - MNAR per-class selection offset δ_c: P(c|x,R=0) via softmax(η_c + δ_c).
  - Splits: HASH(game_id) % 100 → TRAIN/VALIDATE/TEST, shared by all models.
- Model letters are canonical: A observation propensity, B contact-label
  confusion (blocked), C fielding credit, D ball handler, E batted-ball
  geometry, F park factors, G run expectancy + state transition, H advancement
  (schema only), I fielder responsibility (blocked), J pitch coverage/summary,
  K shift propensity (blocked). Use them consistently; introduce each with its
  published table name on first mention.
- The corpus: 1910–2025 target span; 205,845 play-by-play games; 18.1M events
  in event_states_full; 1,953 box-only games. Published estimated tables carry
  an eight-column provenance contract (artifact_id, model_name, model_version,
  source_snapshot_id, method, observed_status, confidence_status,
  weak_identification_flag).
- Write ONLY your assigned file under notes/paper/sections/. No git commands of
  any kind. Read any repo file you need.

## Section map

Assembled order (I concatenate; write each as `## <Section title>` top level,
subsections `###`):

| # | File | Section | Writer | Words |
|---|------|---------|--------|-------|
| 1 | 01-intro.md | Introduction | W1 | 1000 |
| 2 | 02-data.md | The record and its gaps | W1 | 900 |
| 3 | 03-taxonomy.md | A taxonomy of missingness | W1 | 700 |
| 4 | 04-models.md | A family of coverage models | W2 | 2200 |
| 5 | 05-mnar.md | Selection that never recorded itself: MNAR | W2 | 1200 |
| 6 | 06-deep.md | Deep-learning supplements | W3 | 1000 |
| 7 | 07-results.md | Published surfaces | W4 | 1400 |
| 8 | 08-unidentifiable.md | What the record cannot tell you | W3 | 800 |
| 9 | 09-publication.md | Publication policy | W4 | 600 |
| 10 | 10-related.md | Related work | W1 | 500 |
| 11 | 11-limitations.md | Limitations and open problems | W3 | 700 |
| 12 | 12-reproducibility.md | Reproducibility | W4 | 300 |

Abstract: written last by me at assembly.

## Per-section briefs

### 01-intro (W1)
The record problem: Retrosheet-era play-by-play is complete in outcomes but
selectively incomplete in detail; detail completeness is a function of scorer
practice, not the game. Hook fact: before 1988, ground balls are recorded at
roughly half their true share because scorers logged trajectory mainly on hits
(observed ground share ~34% vs ~68% deduced pre-1950). Contributions list
(five): (1) missingness taxonomy separating game process from observation
process; (2) family of hierarchical Bayesian models sharing one statistical
contract; (3) MNAR treatment — a refuted learned correction, a validated fixed
per-class offset mechanism, and a published sensitivity ribbon; (4) shared
entity-embedding pretraining feeding shrunk proposals into the Bayes layer with
leakage gates; (5) a publication policy that ships posteriors with provenance
and withholds the unidentifiable. Sources: notes/data-coverage-implementation/
README.md, statistical-modeling-coverage-design.md, implementation-review.md
(T1.1).

### 02-data (W1)
Source families (play-by-play, box, gamelog, season supplements); corpus scale;
gap magnitudes (3,992,018 unknown-trajectory batted balls after deterministic
inference; 6,989,832 unknown recorded location; 463,102 unknown putouts in
calc_fielding_play_agg). Era structure of coverage — use tables/ outputs
(coverage by decade) once W4's query tables exist; if a table is missing, mark
TODO rather than invent. Sources: data-coverage-taxonomy-1910-2025.md,
README.md, tables/.

### 03-taxonomy (W1)
The 10 missingness classes and why one MCAR/MAR/MNAR label per field is the
wrong ontology; two coupled generative processes; default MAR-given-context
assumption and the two flagged MNAR-risk dimensions (trajectory, location).
Source: data-coverage-taxonomy-1910-2025.md,
statistical-modeling-coverage-design.md.

### 04-models (W2) — the statistical core
Open with the shared contract: non-centered partial pooling; NUTS (numpyro /
nutpie); shared hash splits; per-model γ_dl ablation (zero vs shrunk) deciding
the published flavor; acceptance gates pair convergence (rhat/ess/divergences)
with a held-out predictive metric — convergence is necessary, not sufficient.
Then one subsection per shipped model:
- A: event-grain Bernoulli logistic, 6 dimensions; OOS ROC-AUC 0.897–0.973;
  the 10K-row operating point and the sample-size sweep (AUC plateaus at 10K,
  mixing collapses past 500K); saturated-season filter.
- E + D: K-class softmax with per-class fixed effects, DL covariate, handler
  covariate; published as noprop (MAR) flavors — forward-reference §5.
- C: dual-arm credit allocation; putouts shipped; single-assist K=10 softmax
  with NONE sentinel (any-assist PR-AUC 0.516 vs 0.348 base; per-fielder ID ≈
  baseline — report both); assist-count Dirichlet-multinomial (held-out TV −33%).
- F: hierarchical NB with within-season-league sum-to-zero park effects and
  AR(1) persistence (Beta(2,1)).
- G: two submodels — NB run expectancy with multi-hot era regimes (held-out
  RMSE 8% better than state-only), and the Markov base-out transition. Tell the
  structural-zeros story here in full: naive 24×25 softmax gave rhat 4.04 /
  ess≈5; the failure was specification (unreachable end states, unreachable
  global reference, pooling across disjoint supports), not the sampler;
  reachability mask + per-start modal reference + dropped corpus pooling →
  rhat 1.016, 0 divergences, held-out TV 0.070→0.044. This is a paper-worthy
  methodological vignette.
- J: coverage Bernoulli (smoke ROC-AUC 0.995) + cell-grain multinomial pitch
  summary.
- Canceling random effects vignette (short): scalar per-event REs broadcast
  across class logits cancel exactly under softmax — sampled for nothing across
  four models until removed. softmax(x + c·1) = softmax(x).
Sources: 03-hierarchical-models.md, implementation-review.md (T1.2, T2.4),
memory files phase4_obs_propensity_findings, hierarchical_multinomial_
structural_zeros, t2_spec_completions_landed, docs/estimated-models.md.

### 05-mnar (W2)
Derivation: selection on the recorded class ⇒ P(c|x,R=0) ∝ P(c|x,R=1) ·
(1−s(c,x))/s(c,x); class-independent part cancels in softmax; what remains is a
per-class offset δ_c that the observed data cannot identify. The refutation:
learned γ·z_prop on marginal propensity learns the survivor tilt — among
survivors, low P(observed) correlates with *less* focal class, the mirror image
of the masked slice — masked backtest: γ[GroundBall] = −0.083 ± 0.026 (wrong
sign), relative error reduction −0.001 against a ≥0.25 gate, with clean
convergence. Convergence diagnostics cannot detect a wrong estimand. The
validated mechanism: oracle δ_c in the backtest recovers focal share to 0.0001
error (from 0.106). Since δ_c is unidentified, publish MAR (δ=0) plus a
sensitivity ribbon over δ ∈ ±{0.25, 0.5, 1.0} nats; oracle offset (+0.37 for
GroundBall) lands inside the ±1.0 band; the band [0.18, 0.59] brackets the true
0.43 share; a ±2-SD scale was tried and rejected as uninformatively wide.
Sources: mnar-selection-offset-design.md, implementation-review.md T1.1, memory
mnar_learned_gamma_propensity_refuted.md.

### 06-deep (W3)
Role: proposal distributions and entity embeddings, never published facts —
the invariant is deep model → calibration/leakage checks → Bayesian layer →
published surface; argmax-to-fact is banned. Shared pretraining over the full
18.1M-event universe: 13 player-slot inputs through one shared embedding, 13
pretext heads, Kendall-Gal multi-task weighting, two-stage freeze/unfreeze.
Result: batter permutation-importance 8.4× the no-pretrain baseline, pitcher
5.8×. Two negative results, told straight: (1) the frozen-embedding linear
probe predicted a benefit that fine-tuned downstream training did not deliver
(proxy +0.0068 vs real −0.001) — gate on downstream perm-imp against a
disable-pretrain baseline, not probes; (2) a fold_count=1 configuration
produced in-sample predictions labeled OOF, inflating trajectory top-1 by 1.15pp
inside a consumed posterior — caught and refit with 5-fold CV. Cross-fitting
contract; source-probe AUC gates (≥0.75 diagnostic-only, <0.65 publishable).
Sources: 04-deep-learning-supplements.md, memory pretrain_architecture.md,
phase3_pretrain_proxy_misleads_downstream.md, implementation-review.md P3.1.

### 07-results (W4)
The empirical section — built from tables/ (query outputs). Twelve published
main_models tables (docs/estimated-models.md is the definitive list; two are
schema-only placeholders — say so). Present: coverage-by-era table (Model A
p_observed by decade × dimension); geometry marginals on the unobserved slice
by era; the 2015 NL run-expectancy matrix with HDIs; state-transition example
row; park-factor extremes (Coors 1996 NL 1.392) and interval width vs data
density (sparse Negro-league cells get wide HDIs — uncertainty honesty is the
point, not a defect); linear_weights_estimated vs the deterministic siblings;
assist-count and pitch-summary examples. Every table cites its
tables/<name>.md source; include the SQL file name (queries/<name>.sql) so
results regenerate.

### 08-unidentifiable (W3)
The negative-results section — frame as a contribution: single-source records
bound what coverage modeling can do. B: one scorer per event, the "deduced"
label is a deterministic recode of the same scorer's fielding string —
outcome anchors disagree on 702 of 6.0M events (0.0117%) — Ω is prior-driven
identity; unblock requires an independent second label source. I: the shipped
prototype was Model D with a narrower vocabulary (28% of the training slice is
hits where the "handler" is the retriever); the zone-responsibility kernel
π(k | location, alignment) doesn't exist in the record; parked, name reserved.
K: shift propensity designed but skipped — era-normal alignment prior stands in.
Close with the principle: publish the propensity to observe, the posterior over
what was observed, and a sensitivity band over what wasn't — nothing else.
Sources: memory model_b_contact_blocked.md, implementation-review.md T1.3,
responsibility-zone-design.md, data_coverage_shift_model.md.

### 09-publication (W4)
Five tiers (official, deterministic, estimated, synthetic, withheld);
eight-column provenance contract; posteriors published as distributions —
argmax columns exist for convenience but are never canonical; estimated
surfaces are siblings (_estimated suffix), never overwrite deterministic
legacy surfaces; official credit always published with a confidence column
rather than withheld. Sources: phase5-conventions.md, docs/estimated-models.md,
06-rollout-and-validation.md.

### 10-related (W1)
Short. Missing data: Rubin (1976), Little & Rubin, MNAR sensitivity analysis
tradition (pattern-mixture / selection models — tie δ_c to the selection-model
lineage). Bayesian workflow: Gelman et al., Betancourt non-centering. Sports:
Retrosheet as data source, openWAR (Baumer, Jensen, Matthews 2015) for
uncertainty in sports value estimates, Marchi & Albert. VERIFY every citation
with WebSearch before including — a wrong citation is a Block finding. If
unverifiable, cut it.

### 11-limitations (W3)
From notes/followups.md and memory: δ_c point estimate anchored on
partial-truth sources still open (ribbon published, MAR default); H schema-only
(advancement_class not yet emitted upstream); C error-credit code-complete but
data-blocked (no unknown-error signal — all 16.3M error rows already
attributed) and DP submodel lacks a truth column; G context-neutral P_LW linear
weights deferred on an estimand ambiguity (P_LW keyed on start state alone
cannot distinguish play types sharing a start state); linear_weights_estimated
band is RE-posterior-only — finite-sample uncertainty absent, sparse cells look
too tight; F park_episode_id NULL upstream so AR(1) chains never reset at
reconfigurations; J single-source degeneracy; Model A's published path gained
its holdout split only in fix wave 1. Honest, specific, no padding.

### 12-reproducibility (W4)
SQLMesh-built DuckDB database; artifact IDs on every published row resolve to
MLflow runs; queries/ + tables/ in this paper regenerate every number; fits run
on fixed seeds with published sampler settings. Point to bc.db distribution
and the repo. Keep to ~300 words.

## DAG

```
recon (done) ──────────────┐
Q (query agent, bc.db RO) ─┼─→ W4 (results, publication, repro)
                           ├─→ W1 (intro, data, taxonomy, related)  [needs only recon docs]
                           ├─→ W2 (models, mnar)
                           └─→ W3 (deep, unidentifiable, limitations)
W1..W4 ─→ assembly (me) ─→ R1 (Opus review vs repo + rubric) ─→ fixes ─→ commit
```

W1–W3 run parallel with Q. W4 launches after Q lands. No writer touches git.
