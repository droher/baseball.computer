# Modern geometry observation identifiability

This audit uses only the sealed 373-game PRIMARY TRAIN fitting collection. It does not read the 121 deferred modern-angle evaluation games, the confirmation reserve, or any TEST or VALIDATE labels.

## Decision

The collection does not identify separate global scorer, park, and source effects. It can support a pooled or strongly partially pooled modern translation from a recorded airborne subtype to `trajectory-air-standard-v1`, with scorer effects treated as game-header proxies and park/scorer contrasts limited to their connected components. It cannot support unconstrained person-specific confusion matrices or causal scorer-versus-park attribution.

## Scorer provenance

All 373 games have a non-null `scorer` value, with 119 distinct values. Every value is eight characters; 118 match the common alphanumeric `...701` pattern and one is `lee-j701`. That format does not establish a verified person identity or label authorship.

The [Retrosheet event-file documentation](https://www.retrosheet.org/eventfile.htm) distinguishes `info,oscorer`, the official scorer, from the separate administrative `info,scorer` field. The sibling Rust parser does not preserve that distinction: [`info.rs`](../../baseball.computer.rs/src/event_file/info.rs) maps both keys to `InfoRecord::Scorer`, [`game_state.rs`](../../baseball.computer.rs/src/event_file/game_state.rs) assigns sequential occurrences to the same `GameMetadata.scorer` field, and [`schemas.rs`](../../baseball.computer.rs/src/event_file/schemas.rs) exports only that one value. If both records occur, the later record overwrites the earlier one.

The exported field is documented in [`event_observation_context.sql`](../bc/models/intermediate/coverage/event_observation_context.sql) as raw, pre-cleaning `stg_games.scorer`. [`stg_games.sql`](../bc/models/staging/game/stg_games.sql) passes it through without connecting it to event-level trajectory coding. [`entity_link_reliability.sql`](../bc/models/intermediate/coverage/entity_link_reliability.sql) classifies these scorer source IDs as unresolved, low-confidence identities with `no_master_record`. No inspected model or metadata field says the surviving game-header value came from `oscorer` rather than administrative `scorer`, or that either value authored `recorded_class` for each event.

Accordingly, downstream names and reports should call this a **game-header scorer proxy**. They should not describe it as verified person-level label authorship.

## Connectivity

The 119 scorer values and 40 parks form 156 observed scorer-park cells. Only 34 scorer values appear at more than one park; 85 occur at one park, 28 occur in one game, and 57 scorer-park cells contain one game. Median support is three games, 144 batted-ball events, and 75 resolved airborne targets per scorer value.

The scorer-park bipartite graph has 17 disconnected components. The largest contains 19 scorer values, six parks, 60 games, and 3,107 events, only 15.99% of the sample. Eight components contain one park. When separated by season, the graph fragments into 25 components in 2015, 25 in 2019, 24 in 2023, and 26 in 2025. Effects in different components have no data bridge; a hierarchical prior can regularize them but cannot create an identifying comparison.

There is also no source contrast: all 19,436 events have `source_family='play_by_play'` and `source_acquisition_status='observed'`. A source effect is therefore unestimable from this collection.

## Observation-process support

Recorded trajectory is observed for 19,308 of 19,436 events (99.34%). The remaining 128 are 23 events in 2015, 104 in 2019, and one in 2025; 2023 has none. Sixty-four scorer values have all events observed and 55 have both states, but the small, season-concentrated negative class cannot support a rich scorer-by-park observation model. It is adequate for denominator accounting and descriptive checks, not stable separate random effects.

## Airborne translation support

The canonical contract resolves 9,918 airborne targets: 4,200 LineDrive, 4,069 Fly, and 1,649 PopUp. Of these, 9,869 also have a recorded airborne subtype. All three target classes occur for 118 of 119 scorer values, but only 68 scorer values have at least ten examples of each target class. One hundred scorer values contain all three recorded airborne subtypes, while only 31 have at least ten of each.

This supports estimating a shared translation and, if regularized heavily, deviations for well-supported scorer proxies or connected components. It does not support a free three-by-three confusion matrix for every scorer. The archived `target_strata.parquet` preserves canonical resolution and unresolved statuses by season, result family, recorded subtype, fielder group, and fielder-chain availability so modeling can check whether translation support changes with available clues.

Result and fielding clues are associated with both the recorded label and target construction and must remain predictors or stratification variables rather than be interpreted as independent labels. The earlier source audit also shows local and Statcast broad categories can disagree; those 81 broad-source conflicts remain unresolved and must not be forced into the translation fit.

## Recommended boundary

Use a pooled airborne translation as the base specification. If scorer-proxy deviations are included, use strong partial pooling, expose component IDs, and report component-relative estimates. Do not include a separately interpreted source effect from this sample. Treat park effects as physical/context terms only when another design supplies cross-component connections or constraints; this fitting collection alone cannot separate them globally from scorer proxies.

The bound artifact is `artifacts/statistical/backtests/geometry_reliability/20260911-modern-observation-identifiability-v1`, with manifest SHA-256 `220164b8b4212f9a1b8746d668406d436a178f74034b4d7f8565fa2ff64c07ca`. It contains scorer and scorer-park support, graph components in `report.json`, target-resolution strata, airborne translation support, archived target code, and executable analysis. All source, graph, and partition conservation invariants passed; Ruff passed.
