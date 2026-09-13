# Full-history PBP imputation completion plan

Status: implementation complete; the full PBP development candidate passes reconciliation. Production release requires separate approval. Updated September 13, 2026.

The [PBP completion interface](../docs/pbp-imputation.md) documents sixteen additive outputs and the reproducible read-only builder. The registry classifies 179 columns over ten source relations, including 116 completion targets. The full audit identifies null/sentinel gaps in 28 target fields and 35 affected targets when absent pitch blocks are included. Source parser statuses and optional absent actions remain distinct from missing baseball values.

All eleven component families are built over 205,886 PBP games and 18,141,020 events from 1903–2025, including early derived-metric fallbacks. The independent full pitch audit passes all seventeen counters with no unflagged appearance violations. Grouped coverage reconciles every target by era, league, game type, and source type. The isolated SQLMesh candidate passes all sixteen consumer audits and the full publication validator, including exact component row counts, artifact identities, composite keys, and counter/appearance conservation. Evidence is retained under `artifacts/imputation/20260913-pbp-full-v1/validation`.

All 24 legacy pointers have an isolated, explicitly exploratory migration with current failed/unsupported evidence preserved; the strict pointer checker passes and the original 48 manifest/pointer hashes match. The legacy geometry consumer maps the six angle-modifier labels to `location_angle`. Production data and canonical published pointers remain unchanged. The release bundle retains database, SQLMesh state, DuckLake catalog, and schema-packet rollback copies under `artifacts/imputation/release-candidate-v1/rollback`.

The isolated production-promotion rehearsal passes all twelve legacy model audits and 56 consumer checks against the clone's `main_models` schema. The full PBP publication validator passes again after this rehearsal; 176 targeted tests and strict checks pass. The original production database SHA-256 remains unchanged. The candidate is ready for the reserved production-release approval.

## User objective

The user clarified: "full imputation across mlb history for all of the fields" and "fine with estimates being as rough as they need to be."

The completion target is full historical coverage of applicable baseball fields for games with a play-by-play event spine, using the best available estimate. Closing only the currently populated estimated tables, stopping at 1910 or 1989, or indefinitely withholding estimates because historical prediction is weak does not meet this objective. This supersedes the narrower completion proposal and the original 1910–2025 scope for PBP games. Existing experiment results remain evidence with their original limitations. Box-score and season-based imputation is a separate project.

An estimate can be a broad historical prior, an expected count, a probability distribution, or a reproducibly sampled reconstruction. Poor historical accuracy must be disclosed and measured where possible; it is not by itself a reason to leave an applicable target without an estimate. Roughness does not permit contradictory counts, impossible sequences, invented source provenance, or estimates presented as recorded facts.

## Historical population

Start from games whose canonical source is `source_type = 'PlayByPlay'`, using schedule, roster, box-score, and season sources only as evidence or constraints for those games. Cover the earliest available PBP season through the latest ingested season; the current PBP population is 1903–2025, 205,886 games and 18,141,020 event rows. Aggregate-only games are explicitly outside this project. Do not substitute an invented game list or synthetic event history for absent PBP. Reconcile the PBP catalog against schedule and season sources and report remaining population discrepancies.

Include the historical leagues represented by the project's major-league taxonomy, including Negro Leagues, and account explicitly for each PBP game type. Training on regular-season games does not establish equivalent evidence for postseason or other game types. Keep those applications in the output with a declared basis rather than silently dropping them. Historical rule regimes govern applicability and legal transitions; modern ball/strike and game rules cannot be imposed across the whole history.

Read-only baseline from `bc.db`, September 13, 2026, restricted to PBP games:

| Period | Play-by-play games | Existing event rows |
| --- | ---: | ---: |
| Before 1910 | 41 | 3,262 |
| 1910–1988 | 119,294 | 10,305,280 |
| 1989–2025 | 86,551 | 7,832,478 |
| **Total** | **205,886** | **18,141,020** |

These are source-catalog counts, not proof that every historically played game has been acquired. The counts come from `main_models.game_start_info` and `main_models.event_states_full`, filtered to `source_type = 'PlayByPlay'` and grouped by period. Existing statistical defaults remain 1910–2025 in `bc/config.py` and `bc/python_models/statistical/config.py`; the completion population for this plan is broader because it follows the available PBP spine.

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
| Game conditions and exposure | Game/source/context/exposure ledgers; game and line-score models | Estimate applicable missing time, attendance, weather, park/context and exposure fields for PBP games; preserve historical rules. |
| People, participation, and lineups | Rosters, biographies, personnel states, and appearances; the synthetic lineup optimizer is retained as historical evidence only | Complete missing eligible participant assignments and lineup/substitution states within PBP games; propagate uncertain identities instead of treating them as hard eligibility facts. |
| Game/player/season counters | Event rollups, constrained by box-score and season sources where available | Estimate missing batting, pitching, and fielding totals for PBP games; reconcile known totals across grains and explicitly disposition conflicts. Filling absent games from aggregate sources is separate. |
| Event outcomes and state | Event spine and state reconstruction | Complete gaps within existing PBP plays and reconstruct legal state transitions without changing recorded outcomes. Absent event streams are out of scope. |
| Fielding credit and run attribution | Putout/assist allocation, handler probabilities, official credit authority, run assignment | Finish errors, multi-fielder chains, earned-run and pitcher responsibility, and no-box fallbacks; retain the distinction between official credit and analytical responsibility. |
| Contact geometry | Existing geometry posteriors, corrected target ledger, airborne translation | Correct the old location-side artifact semantics; cover depth, side, angle, edge, strength, general location, trajectory, and normalized categories throughout history, including missing recorded trajectory. |
| Pitches and counts | Parsed sequence/status/issues, pitch summaries, smoke-tested count observedness | Complete per-event counts, pitch results, strike types, sequence lengths, and other applicable pitch fields within actual PBP games; preserve known subsequences and mark reconstructed portions. |
| Runner advancement and defensive responsibility | Advancement builder, observed baserunner states, geometry/handler models | Publish advancement estimates, complete missing runner outcomes and responsibility distributions, and carry geometry/personnel uncertainty forward. |
| Park/run values and downstream metrics | Park factors, run expectancy, transitions, estimated linear weights | Extend fallback coverage to every applicable PBP slice and recompute complete analysis metrics with denominators and uncertainty. |

This family list guides implementation but is not the exhaustive field registry. No field family is deferred merely because its estimate will be crude.

## Resolved scope decision

On September 13, 2026, the user resolved that this project covers PBP games only. Do not generate synthetic plays or pitch sequences for games lacking PBP, and do not fill those games from box-score or season aggregates. Box totals, rosters, and season context may constrain estimates for games that do have PBP. Box-score and season-based imputation, including aggregate-only games, is a separate project.

## Delivery order

1. **Coverage registry and baseline — complete.** All 116 targets have verified output/evidence mappings. The full baseline and era, league, source, and game-type breakdowns reconcile without mismatches.
2. **Complete-value interface and integrity repair — implemented.** Sixteen completed surfaces preserve raw evidence, methods, and artifact identities. The geometry label repair and isolated 24-pointer migration preserve old evidence gaps rather than restamping old passes.
3. **Historical fallback coverage — complete.** Full component builds cover the earliest available PBP games, earlier-period geometry standardization, missing context, and park/run/win-value fallbacks. Refining historical predictive accuracy remains separate.
4. **Event and pitch families — complete.** Full fielding, runner, pitch, count, and rollup builds pass their integrity checks. Source contradictions retain explicit dispositions; completed and interrupted appearance counts reconcile.
5. **Whole-PBP reconciliation and release — candidate verified, production approval pending.** Full-population candidate checks and grouped coverage pass. The recorded-context stress test reports how rough estimates can be. Reproducible artifacts and rollback inputs are retained. Production data changes and publication follow the existing explicit approval boundary.

Use small real-data smoke slices spanning early PBP, pre-1989 sparse geometry, and modern records before full runs. Cache intermediate frames, log progress, and checkpoint long work. Select model complexity from demonstrated value; full-scale Bayesian or deep refitting is not a prerequisite for supplying a defensible broad estimate.

## Acceptance criteria

- Every applicable field and PBP population in the registry has an observed value, deterministic derivation, probability/expectation, or clearly marked generated value. No applicable gap silently disappears through a date filter, missing join, sentinel, empty artifact, or absent row.
- Genuine non-applicability, unresolved source identity, and contradictory evidence are reported explicitly with their chosen treatment. They do not become unexamined ways to exclude difficult baseball quantities.
- Recorded facts and authoritative constraints are preserved. Conflicting sources receive a documented authority disposition rather than forcing mutually inconsistent totals to match.
- Estimates satisfy their joint constraints: nonnegative counts, valid probability distributions, eligible participants or explicit unresolved slots, historical rule legality, outs/base/score continuity, and known aggregate totals.
- Grouped holdouts and historical sensitivity checks report how inaccurate estimates may be. Failed predictive or transport screens select a disclosed rougher fallback or status; they do not automatically block all completion. Invalid arithmetic, target semantics, provenance, or irreproducible artifacts remain release blockers.
- Uncertainty distinguishes fitted variation from assumed historical transport. Wide intervals or prior-only distributions are acceptable; narrow intervals conditional on strong assumptions must not be sold as total uncertainty.
- Complete analysis views expose source and estimate contributions and propagate them into derived metrics. Official-only views remain available.
- Completion is verified against the exact released artifacts and materialized outputs, not merely the existence of model code or an old checklist.
- The registry and completed outputs cover only the PBP population in this plan. Downstream rollups computed from completed PBP are in scope; filling absent games from box-score or season aggregates is outside this plan.

## Evidence retained from the current inventory

The local production database has 14 estimated output tables, of which 12 are populated; advancement and pitch-count coverage are empty. Sampled rows from each populated table carry `confidence_status='exploratory'`. The published location-side artifact remains `e-v12-noprop-location_side-zero` with angle-modifier categories. The current pointer checker stops at `published/assist_count.json` because its publication evidence policy is absent; legacy manifests also reference removed `prior_predictive.nc` files. These are implementation/integrity gaps, separate from the user's acceptance of rough historical estimates.

The airborne-only research thread remains closed under its accepted assumptions. Extending coverage is authorized by this broader objective; reopening its failed validation experiments or consuming sealed confirmation data is not necessary to begin. Frozen protocols, datasets, and historical reports retain their original meaning and are not rewritten to claim a pass. Aggregate-only games and box/season-only imputation remain outside this plan.

See the [estimated output reference](../docs/estimated-models.md), [evidence contract](../docs/modeling-evidence-contract.md), [geometry handoff](../docs/geometry-modeling-handoff.md), and [original coverage design](data-coverage-implementation/README.md) for reusable implementation and historical evidence.
