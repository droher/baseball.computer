<!-- Shape: reference guide, not arc42. Per-model sections (estimand / table / formula / example query) replace building-blocks + runtime-view because the "system" is a set of independent published tables, not a request-flow architecture. Deviation is deliberate. -->
---
title: Estimated Models — Reference
type: architecture
status: active
audience: humans-and-agents
last-verified: 2026-07-13
---

# Estimated Models — Reference

## TL;DR

The estimated tier is twelve `main_models.*` tables that fill documented gaps in the play-by-play record with hierarchical-Bayes posteriors: observation propensities, fielding credit, ball-handler, batted-ball geometry, park factors, run expectancy, base-out transitions, assist counts, and final-count distributions. Every value is a posterior estimate, never an official fact. The tables carry an eight-column provenance contract so a consumer can always tell an estimate from a recorded value, and the two are kept in separate namespaces.

**Invariant:** a posterior expected counter, an official source value, and a deterministic derivation are three different quantities. Never add an `expected_share` into an official counter.

## Purpose & scope

- **Solves:** Bayesian imputation for events whose fielders, batted-ball type, location, runner advancement, or final count were never recorded, plus park and run-value surfaces that carry uncertainty instead of a point fallback.
- **Does not solve:** it does not overwrite recorded facts, does not MNAR-correct the imputation shares by default (see [MNAR caveat](#mnar-the-shares-are-mar-not-mnar-corrected)), and does not yet cover runner advancement (the table ships empty: see [deferred](#imputed_advancement_probabilities-model-h-deferred)).

## How to read these tables

Two shapes:

| Shape | Grain | Payload | Tables |
| --- | --- | --- | --- |
| **Event-level imputation** | `event_key` + a sub-key | a posterior-mean probability or share (`*_share` / `p_observed_mean`) | observation propensities, pitch-count coverage, fielding credit, ball handler, geometry, advancement |
| **Aggregate summary** | a `(season, league, …)` cell | a posterior mean + sd + 94% HDI | run expectancy, state transition, park factor, pitch summary, assist count, linear weights |

Event-level tables ship the posterior **mean only**. The per-event posterior tensor is too large to persist (geometry alone is 253M rows). Aggregate tables ship mean, sd, and HDI bounds, because uncertainty is the point of a season-league cell.

Every table carries the same eight provenance columns:

| Column | Meaning |
| --- | --- |
| `artifact_id` | the fit that produced the row, e.g. `state-transition-v4` |
| `model_name` | the registered Bayes target |
| `model_version` | model code version at fit time |
| `source_snapshot_id` | source-data snapshot the fit read |
| `method` | `hierarchical_logistic` (Bernoulli), `hierarchical_bayes_softmax` (multinomial shares), or `hierarchical_bayes_nb` (count/distribution summaries) |
| `observed_status` | constant `estimated`, the namespace marker |
| `confidence_status` | the fit's validation status |
| `weak_identification_flag` | `True` when a convergent fit is nonetheless weakly identified (low group-level ESS, high r-hat, any divergences, or non-finite diagnostics); see [weak identification](#weak-identification). Populated from each fit's own diagnostics; several current tables carry `True`, the rest `False`. |

`event_key` joins to `main_models.event_states_full` for season, league, game, batter, and base-out context.

## Shared model structure

All twelve fits are hierarchical Bayesian, sampled with NUTS (nutpie). Three likelihood families cover them:

| Family | Likelihood | Used by | Published as |
| --- | --- | --- | --- |
| Bernoulli logistic | `R ~ Bernoulli(p)`, `logit p = η` | observation propensities | `p_observed_mean` |
| Reference-class softmax | `Y ~ Multinomial(n, softmax(η))` | credit, ball handler, geometry, transition, pitch, assist count | `expected_share` / `prob_mean` |
| Negative binomial | `y ~ NB(λ, φ)`, `log λ = η` | park factors, run expectancy | `re_value_mean` / `park_factor_mean` |

One identification rule runs through the event-level softmax builders: scalar-per-event terms (season, scorer, park random effects) are dropped from the per-event softmax. They enter every class logit equally and cancel exactly inside the softmax, so they carry no information and only slow mixing. Cell-grain summary models keep the hierarchy because the cell, not the event, is the unit.

---

## Event-level imputation surfaces

### `scorer_observation_propensities` (Model A)

**Estimand.** P(a batted-ball dimension was actually observed and recorded) for each event, per dimension. This is the inverse-probability weight a downstream model needs to de-bias complete-case analysis.

**Table.** Grain `(event_key, dimension)`. `dimension ∈ {trajectory, location_side, location_depth, location_edge, general_location, ball_handler_position}`. Payload `p_observed_mean ∈ (0,1)`.

**Formula.** Event-grain Bernoulli with partial pooling on scorer, park, and season, fixed effects on game/play context:

```
R_i ~ Bernoulli(p_i)
logit p_i = α + b_season[s_i] + b_scorer[r_i] + b_park[k_i] + Σ_j δ_j[c_ji] + Σ_m γ_m x_mi
p_observed_mean = E[ sigmoid(η_i) | data ]
```

The model graph never materializes a per-event `p` tensor. At 12M events × 4000 draws that would be ~380 GB; the likelihood uses `logit_p=η` and the export reconstructs `sigmoid(η_i)` analytically.

**Example: average observed-ness by dimension, 2015.**

```sql
SELECT p.dimension, ROUND(AVG(p.p_observed_mean), 3) AS avg_p
FROM main_models.scorer_observation_propensities p
JOIN main_models.event_states_full e USING (event_key)
WHERE e.season = 2015
GROUP BY 1 ORDER BY 1;
```

| dimension | avg_p |
| --- | --- |
| trajectory | 0.993 |
| location_depth | 0.943 |
| location_edge | 0.987 |
| general_location | 0.994 |
| location_side | 0.993 |
| ball_handler_position | 0.949 |

Modern scoring records nearly everything. The propensity weight matters most in the sparse early era, where it drops well below these values.

### `imputed_batted_ball_geometry` (Model E)

**Estimand.** P(batted-ball class) for each event whose geometry was not recorded, per geometry dimension: trajectory (Fly/LineDrive/GroundBall/PopUp), location side/depth/edge, and general location (the fielding region).

**Table.** Grain `(event_key, geometry_dimension, class_index)`. `class_label` is the human-readable class. Per-event shares over a dimension's classes sum to 1.

**Formula.** Per-event K-class reference-class softmax. Per-class intercept plus a fixed-effect interaction per context column:

```
G_i ~ Categorical(π_i)
π_{i,k} = softmax_k( α_k + Σ_fe δ_fe[c_fi, k] )      fe ∈ {result_family, base_state_start, outs_start, alignment_regime, batter_hand}
expected_share = E[ π_{i,k} | data ]
```

The four DL-backed dimensions optionally carry a centered per-class deep-learning logit (`gamma_dl`) as a regularized covariate. The published operating points use the shrunk DL flavor on those four and a DL-free fit on `general_location`.

**Example: most-likely trajectory per event (argmax), 5 events.**

```sql
SELECT event_key, class_label, ROUND(expected_share, 3) AS p
FROM main_models.imputed_batted_ball_geometry
WHERE geometry_dimension = 'trajectory'
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY expected_share DESC) = 1
LIMIT 5;
```

| event_key | class_label | p |
| --- | --- | --- |
| 282541797 | LineDrive | 0.478 |
| 282541801 | Fly | 0.377 |
| 282541803 | GroundBall | 0.484 |
| 282541804 | PopUp | 0.316 |
| 282541805 | LineDrive | 0.354 |

Take the full distribution, not the argmax, when you need calibrated probabilities. The top class often sits below 0.5.

### `imputed_ball_handler_probabilities` (Model D)

**Estimand.** P(fielder position handled the batted ball) for each event whose handler is unobserved: the fielder who first fielded it, positions 1–9.

**Table.** Grain `(event_key, player_id, fielding_position)`. `player_id` is attached by joining `personnel_fielding_states` so the probability lands on the actual fielder in the lineup. Shares over the 9 positions sum to 1.

**Formula.** Single-arm K=9 categorical softmax, a trimmed credit model (supervised arm only, no aggregate-box arm, no park/source random effects):

```
H_i ~ Categorical(π_i)
π_{i,k} = softmax_k( α_pos[k] + Σ_fe δ_fe[c_fi, k] )      fe ∈ {result_family, base_state_start, outs_start, alignment_regime}
expected_share = E[ π_{i,k} | data ]
```

**Example: handler distribution for one event.**

```sql
SELECT fielding_position, ROUND(expected_share, 3) AS p
FROM main_models.imputed_ball_handler_probabilities
WHERE event_key = 1615170787
ORDER BY expected_share DESC;
```

| fielding_position | p |
| --- | --- |
| 6 (SS) | 0.191 |
| 4 (2B) | 0.167 |
| 5 (3B) | 0.132 |
| 8 (CF) | 0.123 |
| … | … |

Held-out top-1 accuracy is 0.220 against a 0.171 position-prior baseline. The handler is hard to pin from pre-event state alone, which is why the full distribution ships rather than a label.

### `imputed_fielding_credit` (Model C)

**Estimand.** P(fielder position earned the credit) for events where the official record left a putout or assist unattributed, per credit type.

**Table.** Grain `(event_key, player_id, fielding_position, credit_type)`. `credit_type ∈ {putout, assist}`. `none_share` rides along on assist rows (the K=10 NONE-sentinel mass) so a consumer computes `P(any assist) = 1 − none_share` without a re-join; it is NULL for putouts.

**Formula.** Per-event softmax over the personnel-eligible position set, fit with two likelihoods sharing the same `π`:

```
supervised:  Y_e ~ Multinomial(U_e, π_e)                       on well-attributed events
aggregate:   T_m ~ Normal( Σ_e U_e · π_{e,k}, σ_box )          on the masked subset, at box-cell grain
π_{e,k} = softmax_k( α_pos[k] + Σ_fe δ_fe[c_fe, k] )
expected_share = E[ π_{e,k} | data ]
```

The supervised arm carries the per-event signal; the aggregate arm anchors the masked subset to box-score totals. The assist export is scored over the production unknown slice and marginalizes the unknown putout position over the published putout posterior.

**Example: assist credit shares for one event, with any-assist probability.**

```sql
SELECT fielding_position, ROUND(expected_share, 3) AS p, ROUND(1 - none_share, 3) AS p_any_assist
FROM main_models.imputed_fielding_credit
WHERE credit_type = 'assist'
  AND event_key = (SELECT event_key FROM main_models.imputed_fielding_credit WHERE credit_type='assist' LIMIT 1)
ORDER BY expected_share DESC;
```

### `imputed_advancement_probabilities` (Model H): deferred

**Estimand.** P(advancement class) per baserunner per event: `Stayed`, `Advanced1/2`, `Scored`, `OutAdvancing`, `OutCaughtStealing`, `OutPickoff`.

**Status.** The `@model` and grain `(event_key, baserunner, advancement_class)` exist, but **the table is empty**. No advancement Bayes pointer is published, so it materializes a typed zero-row frame. The upstream `model_input_advancement` dataset and the multinomial builder are wired; the fit is the remaining step.

### `pitch_count_coverage` (Model J, coverage arm): deferred

**Estimand.** P(a plate appearance's final ball-strike count was actually observed and recorded). The same missingness-propensity question `scorer_observation_propensities` answers for batted-ball dimensions, asked instead of the population `pitch_summary_distribution` draws its final-count classes from. A single dimension, `dimension = 'has_count'`.

**Table.** Grain `(event_key, dimension)`, the same shape as `scorer_observation_propensities`. Payload `p_observed_mean ∈ (0,1)`.

**Status.** The `pitch_count_observedness` Bayes target is registered end to end — event-grain Bernoulli builder (`alpha` + a `season|league` cell + a `scorer` random effect + sum-to-zero context fixed effects, structurally the observation-propensity arm reused against the pitch-count population), an `event_propensity.parquet` export, a dataset-scoped propensity aggregator, and this `@model` — and it clears its smoke gate (held-out ROC-AUC 0.995). **No full-scale fit has published.** `aggregate_pitch_coverage_frames` finds no published artifact pointer for `pitch_count_observedness`, so the table materializes its typed zero-row frame — the same deferred-publication mechanism `imputed_advancement_probabilities` uses above. This is a bug-free empty table, not a data-blocked one: the remaining step is running and publishing the full-scale fit.

---

## Aggregate posterior summaries

State grain uses the string `{outs}_{base_state}`. `0_0` is 0 outs / bases empty, `0_7` is 0 outs / bases loaded. `base_state` is a 3-bit code: 1=1B, 2=2B, 4=3B, so 3=1B+2B and 7=loaded.

### `run_expectancy_summary` (Model G)

**Estimand.** Expected runs scored from a base-out state to the end of the half-inning, per season and league.

**Table.** Grain `(state, season, league, outcome)`, with `base_state` and `outs` split out. Payload `re_value_mean` + sd + HDI.

**Formula.** Cell-grain negative binomial. The sum of n i.i.d. `NB(λ, φ)` sharing a cell mean is `NB(nλ, nφ)`, so ~10M events collapse to a few thousand cells exactly:

```
runs_cell ~ NB( n_cell · λ_cell, n_cell · φ )
log λ:  global_mu → mu_state[24 base-out states] → theta_cell        (centered hierarchy)
re_value_mean = E[ exp(theta_cell) | data ]
```

**Example: top run-expectancy states, 2015 NL.**

```sql
SELECT state, base_state, outs, ROUND(re_value_mean, 3) AS re
FROM main_models.run_expectancy_summary
WHERE season = 2015 AND league = 'NL'
ORDER BY re_value_mean DESC LIMIT 4;
```

| state | base_state | outs | re |
| --- | --- | --- | --- |
| 0_7 | 7 (loaded) | 0 | 2.202 |
| 0_6 | 6 (2B+3B) | 0 | 1.980 |
| 0_5 | 5 (1B+3B) | 0 | 1.672 |
| 1_7 | 7 (loaded) | 1 | 1.554 |

Bases loaded, nobody out: 2.20 expected runs, matching standard run-expectancy tables.

### `state_transition_summary` (Model G, transition arm)

**Estimand.** The Markov transition matrix: P(end base-out state | start base-out state) per season and league, over the 24 base-out states plus an inning-end sentinel.

**Table.** Grain `(start_state, season, league, end_class)`. Payload `prob_mean` + sd + HDI. Rows per `start_state` sum to 1.

**Formula.** Reachability-masked reference-class softmax. Outs never decrease within an event, so reachable end classes for a start state are the base states at out counts `≥ start_outs` plus inning-end; unreachable cells are pinned to a large negative logit and carry no free parameter, and each start state pins its own modal reachable class as the reference:

```
counts_cell ~ Multinomial( N_cell, p_cell )
p_cell = softmax over reachable end classes (unreachable → logit −30; per-start reference → 0)
reachable(start, end)  ⇔  end_outs ≥ start_outs
prob_mean = E[ p_cell | data ]
```

This masking is what made the fit converge. Treating all 24×25 pairs as reachable left >50% structural zeros and an unreachable global reference, which walled the sampler out at rhat 4. The published `state-transition-v4` clears the strict gate (rhat 1.048, ess 185, 0 divergences).

**Example: transitions from bases-empty / 0 outs, 2015 NL.**

```sql
SELECT end_class, ROUND(prob_mean, 3) AS p
FROM main_models.state_transition_summary
WHERE start_state = '0_0' AND season = 2015 AND league = 'NL' AND prob_mean > 0.001
ORDER BY prob_mean DESC;
```

| end_class | p | reading |
| --- | --- | --- |
| 1_0 | 0.486 | batter out, bases stay empty |
| 0_0 | 0.309 | bases empty, still 0 out |
| 0_1 | 0.165 | batter reaches first |
| 0_2 | 0.035 | batter reaches second |
| 0_4 | 0.004 | batter reaches third |

Every reachable end has outs ∈ {0, 1}: no 2-out end appears from a 0-out start, the reachability mask in effect. The distribution sums to exactly 1.0.

### `park_factor_summary` (Model F)

**Estimand.** The multiplicative run effect of a park in a given season-league, net of the teams that played there and home-field advantage.

**Table.** Grain `(park_id, season, league, outcome)`. Payload `theta_mean` (log scale) + sd + HDI, plus `park_factor_mean = exp(theta_mean)` (1.0 = neutral).

**Formula.** Team-game negative binomial with a sum-to-zero park effect centered within each season-league:

```
team_runs_g ~ NB( λ_g, φ )
log λ_g = log(PA_g) + α_{season,league} + offense[team,season] + pitching[opp,season] + θ_park[park,season,league] + h·is_home
θ_raw[park,league]:  AR(1) over the season steps of that park-league chain
    θ_raw_1 ~ Normal(0, σ_init)
    θ_raw_t = ρ · θ_raw_{t-1} + ε_t,   ε_t ~ Normal(0, σ_innov),   ρ ~ Beta(2,1)
θ_park = center-within-group( θ_raw, group = (season, league) )
park_factor_mean = exp(θ_park)
```

The raw per-cell effect follows an AR(1) persistence prior across consecutive seasons within a `(park, league)` chain before centering, so a park's factor is pulled toward its own recent history rather than fit independently cell by cell; `BC_PARK_FACTOR_DISABLE_AR1` swaps it for an independent `z · σ_park` raw effect when needed. Centering θ within the season-league group rather than globally stops era-level scoring from leaking into the park effect.

**Example: most hitter-friendly park-seasons.**

```sql
SELECT park_id, season, league, ROUND(park_factor_mean, 3) AS pf
FROM main_models.park_factor_summary
ORDER BY park_factor_mean DESC LIMIT 3;
```

| park_id | season | league | pf |
| --- | --- | --- | --- |
| DEN02 | 1996 | NL | 1.392 |
| DEN02 | 1995 | NL | 1.391 |
| DEN02 | 1997 | NL | 1.383 |

`DEN02` is Coors Field: a ~39% run boost in the mid-90s, from Denver's altitude.

### `pitch_summary_distribution` (Model J)

**Estimand.** P(plate appearance ended on a given ball-strike count) per result family, season, and league, for the slice where the final count was recorded.

**Table.** Grain `(result_family, season, league, final_count_class)`, with `balls` and `strikes` split out. 12 classes (`balls 0–3 × strikes 0–2`). Rows per cell sum to 1.

**Formula.** Cell-grain multinomial with a centered reference-class softmax. Class `b0_s0` is the pinned reference; a three-level hierarchy carries the rest:

```
counts_cell ~ Multinomial( N_cell, p_cell )
p_cell = softmax( [0, cell_logodds] )
cell_logodds:  beta0 → result_family_logodds → cell_logodds         (centered)
prob_mean = E[ p_cell | data ]
```

**Example: final-count distribution for strikeouts, 2015 NL.**

```sql
SELECT final_count_class, ROUND(prob_mean, 3) AS p
FROM main_models.pitch_summary_distribution
WHERE result_family = 'strikeout' AND season = 2015 AND league = 'NL'
ORDER BY prob_mean DESC LIMIT 4;
```

| final_count_class | p |
| --- | --- |
| b1_s2 | 0.341 |
| b2_s2 | 0.283 |
| b0_s2 | 0.226 |
| b3_s2 | 0.150 |

Every strikeout ends on two strikes: the model recovers the structural constraint without it being hard-coded.

### `assist_count_distribution` (Model C, v3.1)

**Estimand.** P(an assist play produced M assists), M ∈ {1,2,3,4}, per result family and start base-out state. This is the count head that places the multi-assist mass before the credit-allocation softmax distributes it.

**Table.** Grain `(result_family, base_state_start, outs_start, assist_count_class)`. Conditions on at least one assist (M=0 is the NONE class the credit model handles). Rows per cell sum to 1.

**Formula.** Cell-grain multinomial, centered reference-class softmax (M=1 is the reference):

```
counts_cell ~ Multinomial( N_cell, p_cell )
p_cell = softmax over {1,2,3,4}
logits:  beta0 → event_class_logodds → cell_logodds                 (centered)
prob_mean = E[ p_cell | data ]
```

**Example: assist-count distribution for a routine out, bases empty, 0 out.**

```sql
SELECT assist_count_class, ROUND(prob_mean, 3) AS p
FROM main_models.assist_count_distribution
WHERE result_family = 'out_in_play' AND base_state_start = 0 AND outs_start = 0
ORDER BY assist_count_class;
```

| assist_count_class | p |
| --- | --- |
| 1 | 0.992 |
| 2 | 0.008 |
| 3 | 0.000 |
| 4 | 0.000 |

A bases-empty groundout is one assist (6-3) 99% of the time. Multi-assist mass concentrates in double-play states.

---

## Derived surface

### `linear_weights_estimated`

**Estimand.** The run value of each play type per season-league, carrying the run-expectancy posterior's uncertainty. This is the estimated sibling of the deterministic `linear_weights` point surface, which is left untouched.

**Table.** Grain `(season, league, play)`. `play_category ∈ {BATTING, BASERUNNING}`. Payload `run_value_mean` + sd + HDI, plus `n_events`.

**Formula.** Posterior propagation, not a new fit. Each run-expectancy draw flows through the deterministic linear-weights formula:

```
for each RE posterior draw d:
    rv_play[d] = Σ_transitions  count_weighted ΔRE  (using draw d's run-expectancy values)
    center rv_play[d] against the per-(season, league) all-play weighted mean
run_value_{mean, sd, hdi} = collapse over draws d
```

**Example: batting and baserunning run values, 2015 NL.**

```sql
SELECT play, play_category, ROUND(run_value_mean, 3) AS rv
FROM main_models.linear_weights_estimated
WHERE season = 2015 AND league = 'NL'
ORDER BY run_value_mean DESC LIMIT 7;
```

| play | play_category | rv |
| --- | --- | --- |
| HomeRun | BATTING | 1.387 |
| Triple | BATTING | 1.065 |
| Double | BATTING | 0.742 |
| ReachedOnError | BATTING | 0.475 |
| Single | BATTING | 0.436 |
| HitByPitch | BATTING | 0.316 |
| Walk | BATTING | 0.298 |

Standard linear weights (home run ~1.4 runs, walk ~0.3), now with an HDI per value instead of a single number.

---

## Risks & known issues

| Risk | Impact | Status |
| --- | --- | --- |
| MNAR: shares assume missing-at-random | med | Imputation shares ship under the MAR (`gamma_propensity_zero`) flavor; the per-class selection-offset mechanism and sensitivity ribbon quantify the MNAR band but are not baked into the published shares. |
| Weak identification in sparse slices | med | `weak_identification_flag` is set from fit diagnostics at publish time (see below); the retrained fits now carry a real per-fit mix of `True`/`False`. |
| Advancement empty | low | `imputed_advancement_probabilities` ships zero rows until Model H fits. |
| Pitch-count coverage empty | low | `pitch_count_coverage` ships zero rows until the smoke-verified `pitch_count_observedness` target gets a full-scale fit published. |
| Full rebuild OOM | low | A from-scratch `rebuild-prod` runs out of memory on `model_input_fielding_credit`'s audit at 14 threads. See `notes/followups.md`. |

### MNAR: the shares are MAR, not MNAR-corrected

The event-level imputation shares are fit on observed-only data under a missing-at-random assumption. The masked backtest showed the naive learned-propensity correction learns the survivor tilt and extrapolates it wrong-signed, so it is not published. The correct correction is a fixed per-class selection-offset applied at scoring time; `python_models/statistical/sensitivity.py` produces a per-class sensitivity ribbon that bounds how far a class share could move under plausible MNAR. Treat the published share as the MAR point and the ribbon as the uncertainty around the missingness mechanism.

### Weak identification

`weak_identification_flag` is populated at fit time from the run's own convergence diagnostics, via `weak_identification_thresholds()` / `diagnostics_indicate_weak_identification()` in `python_models/statistical/validate.py`. A fit that clears the convergence gate is nonetheless marked weakly identified (`True`) when any of:

- the minimum group-level bulk ESS across the fit's random effects falls below 4× the convergence gate's ESS floor (400 for a default fit, 12 for a smoke fit)
- r-hat exceeds a comfort band set at half the convergence gate's margin above 1.0 (1.025 for a default fit, 1.25 for a smoke fit)
- the fit recorded any divergences
- any diagnostic is non-finite (fail-safe: treat "can't tell" as weak)

The retrained league-dependent fits populate this flag from their own diagnostics, so it is now a live signal on the published rows. Several tables carry `True` — the run-expectancy and state-transition summaries, the pitch-summary distribution, the estimated linear weights (which inherit run-expectancy's flag), and part of the observation-propensity rows — reflecting group-level ESS below the 4× floor or r-hat above the comfort band in those cell-grain fits. The geometry, ball-handler, fielding-credit, park-factor, and assist-count tables carry `False`. Read the flag per row: `True` means the fit converged but is weakly identified in that slice, not that the value is unusable.

## Glossary

| Term | Definition |
| --- | --- |
| estimated tier | the twelve `main_models.*` coverage tables holding posterior estimates, `observed_status = 'estimated'` |
| `event_key` | event-grain primary key; joins to `event_states_full` for context |
| state `{outs}_{base_state}` | base-out state string; `base_state` is a 3-bit code (1=1B, 2=2B, 4=3B) |
| `expected_share` / `prob_mean` | posterior-mean probability; per-grain shares sum to 1 |
| `p_observed_mean` | posterior-mean probability an event's dimension was recorded |
| reachability mask | the structural constraint that base-out transitions cannot decrease outs, enforced in `state_transition` |
| estimated contract | the eight provenance columns stamped on every estimated row |
