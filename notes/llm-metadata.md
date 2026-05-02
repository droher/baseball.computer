Design Doc: Metadata Bridge for Natural-Language Querying of bc_remote

  Author: site-repo (baseball.computer.site) → data-repo (bc_remote / SQLMesh / metrics player)
  Status: Proposal
  Last updated: 2026-05-02

  1. Context

  We're building a natural-language → SQL feature on baseball.computer.site. The pipeline runs entirely in the user's browser against the existing remote DuckDB database (bc_remote.db on Cloudflare R2), with pluggable LLM backends (WebLLM
  in-browser, local OpenAI-compatible servers like LM Studio / Ollama, OpenRouter via OAuth, and BYOK for hosted providers).

  The architecture follows the consensus shape from current research (CHESS / CHASE-SQL / Arctic-Text2SQL / dbt MetricFlow benchmarks): retrieval-augmented prompting + constrained decoding + execution-grounded retry, with a closed-vocabulary
  semantic layer to eliminate hallucinated metric definitions. dbt's 2026 benchmarks show this approach moves accuracy from ~40% (raw text-to-SQL) to ~83% (semantic-layer-bounded), with frontier models hitting 100% on in-scope queries. The single
  biggest accuracy lever is the metadata, not the model.

  The data repo is the canonical source for that metadata. This doc specifies what the data repo needs to publish, in what format, and how it integrates with SQLMesh + the custom metrics player.

  2. Architectural split

  ┌───────────────────────────────────────────────────────────────────┬───────────┬───────────┐
  │                              Concern                              │ Data repo │ Site repo │
  ├───────────────────────────────────────────────────────────────────┼───────────┼───────────┤
  │ Schema / FK graph / metric definitions / domain rules             │ ✅ owns   │ consumes  │
  ├───────────────────────────────────────────────────────────────────┼───────────┼───────────┤
  │ Compile pipeline (YAML → JSON / SQL artifacts)                    │ ✅ owns   │ —         │
  ├───────────────────────────────────────────────────────────────────┼───────────┼───────────┤
  │ bc_remote.db artifact + uploads to R2                             │ ✅ owns   │ —         │
  ├───────────────────────────────────────────────────────────────────┼───────────┼───────────┤
  │ Retrieval index, grammar generation, validators, prompt rendering │ —         │ ✅ owns   │
  ├───────────────────────────────────────────────────────────────────┼───────────┼───────────┤
  │ LLM backend abstraction, UI, caching                              │ —         │ ✅ owns   │
  └───────────────────────────────────────────────────────────────────┴───────────┴───────────┘

  The contract between them is a small set of static artifacts published alongside bc_remote.db. No runtime API.

  3. Pipeline shape (for context)

  NL question
    ↓
  [entity extraction + value grounding]   ← uses compiled metadata
    ↓
  [retrieval: top-K tables, metrics, glossary, examples, anti-patterns]
    ↓
  [prompt assembly: DDL/M-Schema + metric cards + rules + few-shots]
    ↓
  [constrained generation: xgrammar GBNF built from active scope]   ← grammar derived from metadata
    ↓ (multi-candidate when backend supports)
  [execution + result-aware validators]   ← FK/aggregation/era/reconstruction checks use metadata
    ↓ retry on failure
  [selector + render]

  Every box marked "uses compiled metadata" is a place where the data repo's outputs are load-bearing.

  4. Source of truth: metadata/ directory

  Authoring format is YAML (humans edit it, comments are critical). Validated against JSON Schemas. Compiled into multiple machine-readable forms (§7).

  metadata/
    tables/<schema>.<table>.yaml          # one per user-facing table
    metrics/<metric_id>.yaml              # one per named metric
    rules/<rule_id>.yaml                  # resolvers, era rules, splits, qualifiers, date parsers
    glossary.yaml                         # domain terms + aliases + canonical refs
    examples/<example_id>.yaml            # NL ↔ SQL pairs
    antipatterns/<id>.yaml                # common wrong patterns + why
    relationships.yaml                    # FK graph (when not declarable in SQLMesh)
    schema/*.json                         # JSON Schemas for each card kind
    README.md                             # authoring workflow

  Scope: restrict to main_models.* (user-facing). Internal staging models stay undocumented.

  4.1 Table card

  id: bc_remote.main_models.batting_events
  grain: one row per plate appearance
  era: { start: 1871, full_pbp: 1951, statcast: 2015 }
  description: |
    Atomic record of every plate appearance...
  columns:
    - name: game_id
      type: VARCHAR
      pk: true
      fk: { table: games, column: game_id }
      example: TBA202504150
    - name: batter_id
      type: VARCHAR
      fk: { table: people, column: player_id, role: batter }
    - name: events
      type: VARCHAR
      enum: [SINGLE, DOUBLE, TRIPLE, HOMERUN, WALK, STRIKEOUT, HBP, ...]
      aggregations: { valid: [count, count_distinct], invalid: [sum, avg] }
    - name: launch_speed_mph
      type: DOUBLE
      nullable: true
      null_reason: pre-Statcast (<2015) or non-batted-ball
      aggregations: { valid: [avg, max, min, percentile], note: filter NULLs }
  relationships:
    - { name: batter, to: people, via: [batter_id, player_id], cardinality: N:1 }
    - { name: pitcher, to: people, via: [pitcher_id, player_id], cardinality: N:1 }
  common_joins:
    - desc: PA with batter name
      sql: "FROM batting_events b JOIN people p ON b.batter_id = p.player_id"
  gotchas:
    - events column uses Retrosheet codes — see events_lookup view for labels
    - pre-1951 lacks pitch-level detail; filter games.detail_level
  sample_values:
    events: { distinct: [...], top_20_by_freq: [...] }    # populated by compile pipeline from DB

  4.2 Metric card

  id: woba
  display_name: Weighted On-Base Average
  aliases: [wOBA, "weighted on base average"]
  macro: bc_remote.metrics.woba
  formula_natural: weighted average of offensive events by run value
  formula_sql: |
    (0.69*uBB + 0.72*HBP + 0.89*1B + 1.27*2B + 1.62*3B + 2.10*HR)
    / (AB + BB - IBB + SF + HBP)
  weights_source: FanGraphs 2024 fixed weights
  required_grain: counts aggregated from plate_appearance events
  valid_groupings: [player, team, league, season, month, game]
  related: [obp, slg, ops, wrc_plus]
  typical_range: [0.250, 0.450]   # for sanity-check validator
  notes: |
    For season-weighted dynamic weights use metrics.woba_dynamic(...).
    Don't reconstruct inline — use the macro.
  tests:
    - description: matches FanGraphs leaderboard 2024 within 1%
      sql: |
        SELECT player_id, woba FROM ... WHERE year = 2024 AND pa >= 502
      reference: { source: fangraphs, url: ..., tolerance: 0.01 }

  4.3 Rules registry — five subkinds

  Unified shape, one file per rule. Each compiles into appropriate runtime form.

  Entity resolver (rules/player_resolver.yaml):
  kind: resolver
  id: player
  target: { table: people, column: player_id }
  strategies:
    - { type: exact, column: full_name }
    - { type: exact, column: last_name, only_if: unique }
    - { type: trigram, column: full_name, threshold: 0.7 }
  disambiguation:
    by: [debut_year, primary_position]
  duckdb_macro: resolve_player(name VARCHAR) -> VARCHAR
  notes: bc_remote uses Bbref IDs (ohtas01), not MLBAM (660271)

  Era-aware identity (rules/cleveland_team_history.yaml):
  kind: era_alias
  id: cleveland_baseball
  canonical_id: CLE
  aliases:
    - { name: "Cleveland Indians", years: [1915, 2021] }
    - { name: "Cleveland Guardians", years: [2022, null] }

  Domain shortcut (rules/risp.yaml):
  kind: domain_shortcut
  id: risp
  matches: [RISP, "runners in scoring position"]
  applies_to: [batting_events, pitching_events]
  expands_to: "(runner_on_2b OR runner_on_3b)"
  duckdb_macro: is_risp(...) -> BOOLEAN

  Implicit qualifier (rules/qualified_batter.yaml):
  kind: qualifier
  id: qualified_batter
  matches: [qualified, "qualified hitter"]
  context: season-level batting
  expansion_sql: "pa >= 3.1 * team.team_games"
  note: 3.1 PA per team game; canonical MLB qualifier

  Date / season parser (rules/season_phrase.yaml):
  kind: phrase_pattern
  id: season_phrase
  patterns:
    - { regex: "(\\d{4}) season", expansion: "year = {1} AND game_type = 'R'" }
    - { regex: "(\\d{4}) postseason", expansion: "year = {1} AND game_type IN ('WC','DS','LCS','WS')" }

  4.4 Glossary

  terms:
    - term: WAR
      aliases: [war, wins above replacement]
      refers_to: { metric_id: war_position }
      disambiguation: position-player WAR ≠ pitcher WAR
    - term: Ohtani
      aliases: [shohei ohtani, sho-time]
      refers_to: { table: people, where: "player_id = 'ohtas01'" }
    - term: split
      aliases: [splits]
      context: usually L/R-handedness or home/away
      see: rules.platoon

  4.5 Few-shot examples

  id: ohtani_2024_woba_monthly
  question: Ohtani's wOBA by month in 2024
  sql: |
    SELECT month, metrics.woba(SUM(ubb), SUM(hbp), SUM(single), ...) AS woba
    FROM batting_events
    WHERE batter_id = 'ohtas01' AND year = 2024
    GROUP BY month
    ORDER BY month;
  tags: [metric_call, monthly_split, single_player]
  notes: demonstrates metric macro call + entity resolution + temporal grouping

  Aim for 30+ examples covering aggregation grain switches, era filters, splits, multi-FK joins, metric calls, common gotchas. Highest-leverage authoring task after metric cards.

  4.6 Anti-patterns

  id: sum_era
  matches_pattern: "SUM(era) | AVG(era) over multiple pitchers"
  why_wrong: ERA is a rate; recompute from earned_runs + innings_pitched
  correct_pattern: "9.0 * SUM(earned_runs) / SUM(innings_pitched)"
  applies_to: [pitching_events, pitcher_seasons]

  Retrieved as cards alongside relevant tables. Validators also use these as detection patterns.

  4.7 Relationships

  Single canonical FK graph, even where SQLMesh / DuckDB don't enforce them at engine level:

  foreign_keys:
    - { from: batting_events.batter_id, to: people.player_id, cardinality: N:1, name: batter }
    - { from: batting_events.pitcher_id, to: people.player_id, cardinality: N:1, name: pitcher }
    - { from: batting_events.game_id, to: games.game_id, cardinality: N:1 }
    ...

  Bootstrap from SQLMesh column-level lineage / audits where possible; hand-author the rest. This is what the FK validator walks.

  5. SQLMesh integration

  5.1 Where the metric registry lives

  Open question — needs your call. Three plausible homes:

  1. Inside the SQLMesh project, alongside model defs. Pros: lineage, single repo, audits possible. Cons: SQLMesh doesn't natively model "metric" as a first-class concept yet (last we checked); you'd be inventing the convention.
  2. Alongside SQLMesh in the data repo, in a metrics/ directory the metrics player owns. Pros: clean separation, metrics player has full freedom. Cons: requires the player to have a stable spec.
  3. Standalone repo, consumed by both SQLMesh project and metadata compile. Pros: reusable across projects. Cons: probably overkill for one project.

  We have a mild preference for #2. Whatever you pick, the metadata bridge needs a single canonical reference to a metric registry — a single file or module exposing {id, macro_name, signature, formula_sql, required_grain, valid_groupings, ...} for
  every metric.

  5.2 Bootstrap from SQLMesh manifest

  A generator script extracts as much of the YAML as possible automatically:

  - Model name, columns, types → table card skeleton
  - Model description → description field
  - Audits with not_null / unique → PK candidates
  - Audits with relationships (or your equivalent) → FK entries in relationships.yaml
  - Column-level lineage → FK candidates where audits don't declare them
  - Metric registry entries → metric card skeletons + macro names

  Hand-edit only what the manifest can't carry: gotchas, common_joins, era coverage, glossary, examples, anti-patterns, rules, sample_values (those come from data sampling).

  The generator must preserve hand-written sections on regeneration — round-tripping is critical, not optional. Pattern: hand-written sections live in # === HAND === blocks the generator never touches, or a sidecar <table>.hand.yaml merged at
  compile time. Either works; pick whichever fits SQLMesh's regeneration cadence.

  5.3 Macro emission — load-bearing assumption

  For the closed-vocabulary semantic layer to work, every named metric must be callable in bc_remote.db by name. The LLM emits metrics.woba(...), never 0.69*ubb + ....

  Concretely the metrics player needs to either:

  - Emit CREATE MACRO bc_remote.metrics.<name>(...) statements baked into bc_remote.db at build time. DuckDB MACROs are read-only when the DB is attached READ_ONLY, so the LLM cannot redefine them inline. This is the cleanest path.
  - Or pre-materialize metric outputs as views (CREATE VIEW metrics.woba_player_season AS ...) — works but less flexible for ad-hoc grouping.

  If neither is achievable today, that's the single most important gap to close before this whole approach pays off. Worth a pre-sync conversation if there are blockers.

  5.4 Sample values

  Compile pipeline runs sampling queries against bc_remote.db for each user-facing column:

  - Low-cardinality (COUNT(DISTINCT) < 100): full enum
  - Medium-cardinality: top-100 by frequency
  - High-cardinality (player names, etc.): top-1000 by appearances, with the entity ID alongside

  Output goes into both the table card YAML (for docs) and the runtime value index (for entity grounding).

  6. Compiled outputs (what gets uploaded)

  All published to the same R2 bucket as bc_remote.db. Versioned together via schema_version + data_version stamps.

  ┌────────────────────────┬────────┬────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │        Artifact        │ Format │                                              Purpose                                               │
  ├────────────────────────┼────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ metadata.compiled.json │ JSON   │ normalized canonical blob; consumed by client validators, sidebar, autocomplete, grammar generator │
  ├────────────────────────┼────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ metadata.search.json   │ JSON   │ pre-built BM25 index over cards (or per-card weighted text for client-side indexing)               │
  ├────────────────────────┼────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ metadata.values.json   │ JSON   │ sampled distinct values per column for entity grounding                                            │
  ├────────────────────────┼────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ metadata.macros.sql    │ SQL    │ CREATE MACRO statements applied to bc_remote.db at build time                                      │
  ├────────────────────────┼────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ metadata.comments.sql  │ SQL    │ COMMENT ON ... statements applied to bc_remote.db                                                  │
  ├────────────────────────┼────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ metadata.version.json  │ JSON   │ { schema_version, data_version, generated_at } for cache invalidation                              │
  └────────────────────────┴────────┴────────────────────────────────────────────────────────────────────────────────────────────────────┘

  Site renders prompt-time formats (DDL+comments, M-Schema, compact metric markdown) from metadata.compiled.json at runtime — not your problem.

  7. Format choices, briefly

  - Authoring: YAML. Human-friendly.
  - Canonical / machine: JSON. Validated against JSON Schemas in metadata/schema/.
  - In bc_remote.db: SQL DDL (COMMENT ON, CREATE MACRO).
  - Prompt-time (rendered by site): annotated DDL or M-Schema for tables, compact markdown for metric cards, terse term: meaning lines for glossary, raw Q: ... \n SQL: ... for few-shots.

  You don't need to optimize YAML for LLM consumption — site re-renders. Optimize YAML for your authors' editor ergonomics.

  8. Compile pipeline

  metadata/*.yaml
    ↓ JSON Schema validation (fail fast)
    ↓ merge with auto-extracted SQLMesh manifest
    ↓ run sample-value queries against staging bc_remote.db
    ↓ render canonical JSON
    ↓ render SQL artifacts
    ↓ apply SQL artifacts to bc_remote.db
    ↓ upload all artifacts to R2 (atomic swap)

  Suggested commands:
  - pnpm metadata:check (or python equivalent) — validate YAMLs
  - pnpm metadata:bootstrap — regenerate auto-extracted skeletons from SQLMesh
  - pnpm metadata:compile — emit all artifacts
  - CI step: validate + compile + upload + invalidate

  9. Versioning + refresh

  - Every artifact carries schema_version (bumps on metadata structural change) and data_version (bumps on data refresh).
  - Client caches by version; refetches on version mismatch.
  - Backwards compatibility: site repo pins a min schema_version; data repo can add fields freely, must coordinate before changing/removing fields.

  10. Phasing

  Ordered by leverage / blocking dependency:

  1. JSON Schemas for all card kinds. Authoring guardrails before authors write much.
  2. Macro emission contract confirmed in metrics player. The whole thing depends on this.
  3. Bootstrap script from SQLMesh manifest. Gets first-pass YAMLs for all user-facing tables.
  4. Hand-author core: top 10 tables, top 5 metrics (wOBA, FIP, OPS+, ERA+, WAR), 30 few-shot examples, glossary v1.
  5. Compile pipeline + metadata.compiled.json + metadata.macros.sql + metadata.comments.sql. Site can start consuming.
  6. Rules registry: resolvers (player, team, park), era aliases (Cleveland, Tampa Bay, etc.), domain shortcuts (RISP, vs LHP, qualified, splits), date parsers.
  7. Sample-value sampling step + metadata.values.json.
  8. Anti-patterns + metric tests.
  9. Schema versioning + cache invalidation hooks.
  10. Round-trip generator: re-running bootstrap preserves hand-written sections.

  Phases 1–5 unblock site-side work. 6–8 are accuracy levers stackable later. 9–10 are operational hygiene.

  11. What I don't need from you

  - Documentation for internal staging models. Stay scoped to main_models.*.
  - A query-execution API. Site is fully client-side.
  - Embeddings — BM25 is fine at our scale. If retrieval accuracy gaps appear later, we'll add transformers.js in the browser.
  - Anything that requires a server.

  12. Open questions back to you

  1. Where does the metric registry live, and what's its current shape (Python? YAML? something else)? Whatever it is, we should consume it directly rather than duplicate.
  2. Can the metrics player emit DuckDB-callable MACROs (or pre-materialized views) baked into bc_remote.db? If not today, what would block it? This is the single load-bearing assumption for the whole closed-vocabulary approach.
  3. Where does FK information live today, if anywhere? SQLMesh audits, hand-authored, derivable from column-lineage, or unspecified?
  4. Grain metadata — is "one row per X" recorded in SQLMesh model descriptions today? If not, willing to adopt a grain: convention?
  5. Era / temporal coverage — where's the canonical statement of which tables cover which seasons?
  6. Where should the glossary live — data repo (next to metrics, where domain knowledge already is) or site repo? We have a mild preference for the data repo.
  7. Refresh cadence — how often does bc_remote.db rebuild? Daily? Weekly? Affects how aggressive cache invalidation needs to be.
  8. Round-tripping — what's your preference for preserving hand-written YAML through bootstrap regeneration? Inline marker blocks, sidecar files, or something else?

  13. Deliverable

  A PR (or series) that adds:

  1. metadata/ directory with JSON Schemas + first batch of authored cards (10 tables, 5 metrics, 30 examples, glossary v1, core rules).
  2. Bootstrap script from SQLMesh manifest (whatever shape works for your pipeline).
  3. Compile pipeline emitting the six artifacts in §6.
  4. CI step: validate → compile → apply to bc_remote.db → upload.
  5. metadata/README.md explaining authoring workflow, hand-vs-generated split, regeneration command.

  Once §6 artifacts ship, site repo can wire up retrieval + validators + grammar generation in parallel. End-to-end NL→SQL pipeline blocks on the metrics-player MACRO emission (open question §12.2).

  ---
  Bottom line: the metadata bridge is the durable competitive advantage. Models will keep getting better and we'll route to the best one available. But the metric closed vocabulary, FK graph, glossary, rules, examples, and anti-patterns are
  project-specific knowledge that no model brings on its own — and they're what makes the difference between a system that sounds smart and one that's actually correct on baseball questions.