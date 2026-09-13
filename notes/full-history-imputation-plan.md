# Full-history imputation completion plan

Status: active scope and implementation plan. Updated September 13, 2026.

## User objective

The user clarified: "full imputation across mlb history for all of the fields" and "fine with estimates being as rough as they need to be."

The completion target is full historical coverage of applicable baseball fields with the best available estimate. Closing only the currently populated estimated tables, stopping at 1910 or 1989, or indefinitely withholding estimates because historical prediction is weak does not meet this objective. This supersedes the narrower completion proposal and the original 1910–2025 scope. Existing experiment results remain evidence with their original limitations.

An estimate can be a broad historical prior, an expected count, a probability distribution, or a reproducibly sampled reconstruction. Poor historical accuracy must be disclosed and measured where possible; it is not by itself a reason to leave an applicable target without an estimate. Roughness does not permit contradictory counts, impossible sequences, invented source provenance, or estimates presented as recorded facts.

## Historical population

Start from the union of available game, schedule, roster, box-score, and season sources, reconciled by identity. Cover the earliest supported season through the latest ingested season; the current game catalog spans 1871–2025. Remove the implicit 1910 lower bound from the completion contract, while retaining appropriate bounds on each training population. Do not substitute an invented game list for absent historical evidence. Reconcile the catalog against schedule and season sources and report remaining population discrepancies.

Include the historical leagues represented by the project's major-league taxonomy, including Negro Leagues, and account explicitly for each game type. Training on regular-season games does not establish equivalent evidence for postseason or other game types. Keep those applications in the output with a declared basis rather than silently dropping them. Historical rule regimes govern applicability and legal transitions; modern ball/strike and game rules cannot be imposed across the whole history.

Read-only baseline from `bc.db`, September 13, 2026, over all catalogued game types:

| Period | Play-by-play games | Box-score games | Gamelog games | Existing event rows |
| --- | ---: | ---: | ---: | ---: |
| Before 1910 | 41 | 13,719 | 17,880 | 3,262 |
| 1910–1988 | 119,294 | 1,953 | 4 | 10,305,280 |
| 1989–2025 | 86,551 | 0 | 0 | 7,832,478 |

These are source-catalog counts, not proof that every historically played game has been acquired. The source breakdown comes from `main_models.game_start_info` grouped by period and `source_type`; event counts come from `main_models.event_states_full` grouped by period. Existing statistical defaults are 1910–2025 in `bc/config.py` and `bc/python_models/statistical/config.py`.

## Field contract and consumer outputs

Build an exhaustive, versioned field registry from the catalog and source schemas. Map repeated columns to the same baseball quantity, while recording every consumer column. Every field needs a grain, type, applicable population/rule regime, missing-value semantics, observed source, authority order, deterministic derivation, estimate method, fallback, constraints, uncertainty representation, and final output location. Count missing rows as well as null/sentinel fields inside existing rows.

The registry must distinguish baseball payload from source bookkeeping. Unknown scorers, umpires, and players need candidate identities or explicitly unresolved participant slots with uncertainty; a generated identifier is not a discovered person. Source filenames, recorded comments, acquisition timestamps, and model hashes cannot be invented as evidence. Fields that genuinely do not apply retain a specific not-applicable status, not a fabricated fill. Every such classification must be visible in the registry rather than an undocumented exclusion.

Publish a complete estimated analysis layer alongside the official layer. A consumer should not need to assemble fourteen unrelated posterior tables to obtain a completed dataset. Preserve recorded values and source statuses; expose the selected value or expectation, its source/method, fallback level, uncertainty, and artifact identity. Probability tables and coherent sampled histories complement the convenient completed values. Recompute derived counters and rates from the completed inputs rather than estimating each redundant metric independently.

Fallback order, adapted to the quantity and available constraints:

1. Authoritative recorded value or compatible coarser aggregate constraint.
2. Deterministic reconstruction or derivation from known facts.
3. Existing model prediction with supported inputs.
4. Partially pooled comparable-era, league, role, result, park, and context estimates.
5. Broader historical prior with explicit assumption and uncertainty status.

No tier may require a covariate that is itself absent without defining its fallback or integrating over its uncertainty. Missing-context patterns must be represented jointly; independently filling related categories is not enough to create a valid baseball play. A prior-only estimate is a valid completion outcome; an arbitrary constant disguised as a fitted probability is not.

## Field-family worklist

| Family | Existing foundation | Completion work |
| --- | --- | --- |
| Game conditions and exposure | Game/source/context/exposure ledgers; game and line-score models | Estimate applicable missing time, attendance, weather, park/context and exposure fields; cover aggregate-only games and historical rules. |
| People, participation, and lineups | Rosters, biographies, personnel states, appearances, synthetic lineup optimizer | Complete missing eligible participant assignments and lineup/substitution states; propagate uncertain identities instead of treating them as hard eligibility facts. |
| Game/player/season counters | Event, box-score, gamelog, and season-source rollups | Estimate missing batting, pitching, and fielding totals; reconcile known totals across grains and explicitly disposition conflicts. |
| Event outcomes and state | Event spine and state reconstruction | Complete gaps within existing plays and reconstruct legal state transitions without changing recorded outcomes. Whole absent event streams depend on the decision below. |
| Fielding credit and run attribution | Putout/assist allocation, handler probabilities, official credit authority, run assignment | Finish errors, multi-fielder chains, earned-run and pitcher responsibility, and no-box fallbacks; retain the distinction between official credit and analytical responsibility. |
| Contact geometry | Existing geometry posteriors, corrected target ledger, airborne translation | Correct the old location-side artifact semantics; cover depth, side, angle, edge, strength, general location, trajectory, and normalized categories throughout history, including missing recorded trajectory. |
| Pitches and counts | Parsed sequence/status/issues, pitch summaries, smoke-tested count observedness | Complete per-event counts, pitch results, strike types, sequence lengths, and other applicable pitch fields; preserve known subsequences and mark reconstructed portions. Whole-sequence generation depends on the decision below. |
| Runner advancement and defensive responsibility | Advancement builder, observed baserunner states, geometry/handler models | Publish advancement estimates, complete missing runner outcomes and responsibility distributions, and carry geometry/personnel uncertainty forward. |
| Park/run values and downstream metrics | Park factors, run expectancy, transitions, estimated linear weights | Extend fallback coverage to every applicable historical slice and recompute complete analysis metrics with denominators and uncertainty. |

This family list guides implementation but is not the exhaustive field registry. No field family is deferred merely because its estimate will be crude.

## Decision awaiting the user

For games with only box scores or final scores and no play-by-play, should completion include synthetic plays and pitch sequences, or complete only game/player totals while leaving the unrecorded sequence absent? The question was submitted on September 13, 2026. There is no default authorization from elapsed time; event-stream generation for aggregate-only games waits for the answer.

Both choices require full aggregate coverage and the same per-field accounting. If synthetic histories are included, they need a separate generated-event identity, reproducible random seed/draw identity, historical rule state, and reconciliation to known game, inning, player, and season constraints. If sequence generation is excluded, the finished contract must explicitly report that aggregate-only games have completed totals but no generated event history.

## Delivery order

1. **Coverage registry and baseline.** Inventory all applicable fields and absent row populations over the full historical universe. Deliver machine-readable counts by field, era, league, source, and game type, with an implemented fallback or tracked implementation item for every gap. The broad family inventory above and game counts are complete; the exhaustive registry is not.
2. **Complete-value interface and integrity repair.** Define the additive completed outputs and per-value provenance. Repair the published location-side target mismatch and migrate legacy artifact publication metadata. Restore needed retained evidence or version replacements; do not relabel historical `passed` stamps as current validation.
3. **Historical fallback coverage.** Complete earlier-period geometry, contact normalization for both recorded and missing trajectory, missing personnel/context, and aggregate counters. Use simple coherent priors first wherever no richer model is ready; improve estimates without changing consumer contracts.
4. **Remaining event and pitch families.** Fill fielding/run responsibility, advancement, count and sequence gaps. Integrate the aggregate-only history decision and enforce cross-field baseball constraints.
5. **Whole-history reconciliation and release.** Check complete outputs across grains, report measured accuracy and uncertainty, retain reproducible artifacts/rollback inputs, and prepare the production release. Production data changes and publication follow the existing explicit approval boundary.

Use small real-data smoke slices spanning early aggregate-only history, early play-by-play, pre-1989 sparse geometry, and modern records before full runs. Cache intermediate frames, log progress, and checkpoint long work. Select model complexity from demonstrated value; full-scale Bayesian or deep refitting is not a prerequisite for supplying a defensible broad estimate.

## Acceptance criteria

- Every applicable field and historical population in the registry has an observed value, deterministic derivation, probability/expectation, or clearly marked generated value. No applicable gap silently disappears through a date filter, missing join, sentinel, empty artifact, or absent row.
- Genuine non-applicability, unresolved source identity, and contradictory evidence are reported explicitly with their chosen treatment. They do not become unexamined ways to exclude difficult baseball quantities.
- Recorded facts and authoritative constraints are preserved. Conflicting sources receive a documented authority disposition rather than forcing mutually inconsistent totals to match.
- Estimates satisfy their joint constraints: nonnegative counts, valid probability distributions, eligible participants or explicit unresolved slots, historical rule legality, outs/base/score continuity, and known aggregate totals.
- Grouped holdouts and historical sensitivity checks report how inaccurate estimates may be. Failed predictive or transport screens select a disclosed rougher fallback or status; they do not automatically block all completion. Invalid arithmetic, target semantics, provenance, or irreproducible artifacts remain release blockers.
- Uncertainty distinguishes fitted variation from assumed historical transport. Wide intervals or prior-only distributions are acceptable; narrow intervals conditional on strong assumptions must not be sold as total uncertainty.
- Complete analysis views expose source and estimate contributions and propagate them into derived metrics. Official-only views remain available.
- Completion is verified against the exact released artifacts and materialized outputs, not merely the existence of model code or an old checklist.

## Evidence retained from the current inventory

The local production database has 14 estimated output tables, of which 12 are populated; advancement and pitch-count coverage are empty. Sampled rows from each populated table carry `confidence_status='exploratory'`. The published location-side artifact remains `e-v12-noprop-location_side-zero` with angle-modifier categories. The current pointer checker stops at `published/assist_count.json` because its publication evidence policy is absent; legacy manifests also reference removed `prior_predictive.nc` files. These are implementation/integrity gaps, separate from the user's acceptance of rough historical estimates.

The airborne-only research thread remains closed under its accepted assumptions. Extending coverage is authorized by this broader objective; reopening its failed validation experiments or consuming sealed confirmation data is not necessary to begin. Frozen protocols, datasets, and historical reports retain their original meaning and are not rewritten to claim a pass.

See the [estimated output reference](../docs/estimated-models.md), [evidence contract](../docs/modeling-evidence-contract.md), [geometry handoff](../docs/geometry-modeling-handoff.md), and [original coverage design](data-coverage-implementation/README.md) for reusable implementation and historical evidence.
