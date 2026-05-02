# Migration Evaluation: dbt+Jinja → modern transformation + expression + semantic stack

**Status:** Evaluation doc with chosen direction. Stack 3 (SQLMesh + Ibis + BSL/MetricFlow + Hamilton + DuckLake) selected; phased migration plan included near end. All other stacks retained as alternatives discussion.

**Chosen direction (decided after evaluation):**
- **Transformation framework**: SQLMesh, adopted via `sqlmesh init -t dbt` and incremental cutover.
- **Expression layer**: Ibis as primary; sqlglot as fallback for AST-level metric codegen.
- **Semantic layer**: choice between BSL and MetricFlow deferred to a 1-day verification spike on the OPS-derived-tree pattern.
- **ML pipeline**: Hamilton (Apache project) for the Python feature-engineering DAG; consumes Ibis-produced tables. ML re-enablement is part of the stack, not bolted on later.
- **Publish layer**: DuckLake on R2.
- **Schema shape**: long-format base + on-demand wide reshape via semantic layer; cached wide views for hot queries. Open to consumer migration.
- **Event-locality (axis D)**: Polars `forward_fill().over()` and Numba `@njit` in SQLMesh Python models for FSMs; iterate-Python-graduate-Rust pattern; `boxball-rs` upstream pushdown decisions deferred.

## Context

`baseball.computer` builds a DuckDB analytics database from Retrosheet event data via dbt + Jinja, then publishes parquet to Cloudflare R2 plus a stateless DuckDB pointing at HTTPS URLs. 108 SQL models, all `+materialized: table`, no incremental, no snapshots, no Python models. Stack: dbt-core 1.10, dbt-duckdb 1.10.1, DuckDB 1.5.2, Python 3.12, Polars already present (no Ibis yet).

The pain isn't dbt as such — it's the **in-house Jinja DSL** that's grown to express metrics:
- `bc/macros/metric_calcs.sql` defines metric formulas as dicts of SQL strings (`"batting_average": "SUM(hits) / SUM(at_bats)"`).
- `bc/macros/metrics_table_generator.sql` is a multi-CTE generator parametrized by `(kind ∈ {offense, pitching, fielding}, grouping_keys, agg_type ∈ {player, team, league})` that emits 9 pre-aggregated metric tables (`metrics_player_season_league_offense`, etc.). Each table can be ~645 columns.
- `bc/macros/stat_lists.sql` enumerates ~80 base stat names, looped for column generation.
- `bc/macros/park_factors.sql` does Bayesian shrinkage with rolling windows, parametrized by `(rate_stats, denominator_stat, prior_sample_size, prev_years, batter_hand_split, use_odds)`.
- `bc/macros/init_db.sql` walks `graph.sources` to register parquet/CSV sources via `read_parquet()`/`read_csv()`, then issues procedural `CREATE TYPE ... AS ENUM` for dimension columns.

Symptoms of the pain: adding a metric edits a Jinja dict and forces all 9 tables to rebuild; adding a grain means a new model + macro invocation; metric formulas live as opaque strings (no validation, no introspection); `docs/semantic_manifest.json` is empty (MetricFlow not adopted); `bc/tests/` is just `.gitkeep`.

This doc surveys what to migrate to across four axes:
- **Transformation framework** (dbt vs alternatives)
- **Expression layer** (Jinja+SQL strings vs Python expression libraries)
- **Semantic layer** (Jinja codegen vs declarative metric definition)
- **Vectorized event-sequence transformations** — for state machines disguised as SQL, bitfield decoding, and merge-joins-as-hash-joins. Event data has a natural traversal order (`game_id, event_id`); the relational model throws this away. SQL is fine for declarative aggregation; imperative/vectorized passes are better for stateful per-event-stream logic.

…plus cross-cutting evaluations of **DuckLake** (publish-layer replacement) and **`boxball-rs` upstream pushdown** (settled rulebook logic moves into the parser, eliminating downstream models).

## Decisions captured from interview

These shape what's in scope:

1. **Output**: evaluation doc only. No code yet.
2. **No self-hosted services.** Library / CLI only. Excludes Cube, Lightdash, Rill (server), GoodData, Looker, Hex.
3. **R2 publish + remote views functionality** must be replicable. How (via DuckLake, parquet, or other) is open.
4. **DuckLake in scope** as publish-layer replacement candidate.
5. **Materialization should be invisible to consumers, configurable, "cache-like"** — config-driven (not auto-managed by query patterns, not server-routed).
6. **Cutting-edge OK; moderate bus-factor risk acceptable.** Ibis post-Voltron, BSL, lea, prototype features all in play.
7. **Migration cadence: incremental, slice by slice.** Tools that coexist with current dbt strongly preferred over greenfield rewrites.
8. **Authoring DSL: Malloy on the table** alongside Python/YAML; user will judge from concrete syntax in this doc.
9. **Event-locality matters.** Event data is naturally sorted; SQL self-joins on `event_id` and stacked windowed CTEs throw away that locality. Imperative/vectorized passes are in scope where they fit.
10. **`boxball-rs` is in scope for migration.** Settled rulebook logic (save eligibility, base-state derivation) can move into the parser; analytical iteration stays in the analytical layer.
11. **Iterate-Python-graduate-Rust pattern accepted.** Hot paths start in Python (Polars/Numba); graduate to Rust DuckDB extensions (via `quack-rs` + DuckDB's new UDWFs) once stable.
12. **All four offender types are typical, not isolated.** `event_pitching_flags` (state machine), `event_baserunning_stats` (bitfield), `calc_park_factors_advanced` (statistical model), and `metrics_table_generator` (codegen) are representative; recommendations should generalize.

---

## Axis A — Transformation framework (dbt and alternatives)

### A1. Stay on dbt-core + Jinja (status quo)

What you have. Mature, works, the pain is the Jinja DSL not dbt itself. Worth listing because the cleanest migration may be "keep dbt for orchestration, replace just the metric DSL."

- **Strengths**: zero migration cost; existing macros, tests scaffolding, docs work. Adapter ecosystem is the broadest. dbt-duckdb 1.10.1 actively maintained.
- **Weaknesses**: Jinja is text-level, no SQL-aware validation. `adapter.get_columns_in_relation()` is the only schema introspection and it runs at compile time only. No column-level lineage. No virtual envs. License direction post-Fusion is splitting.
- **Fit for your pain**: low — doesn't address metric DSL ergonomics by itself.

### A2. dbt Fusion engine

dbt Labs' Rust rewrite of the dbt engine, born from the SDF Labs acquisition (Jan 2025). Public beta May 2025, "preview" Aug 2025, GA target unknown. Keeps the dbt project structure (your 108 models stay where they are), but adds:
- Real SQL parsing (Rust port of sqlglot + Rust Jinja).
- Column-level lineage.
- Type-aware semantic checks at compile time (catch column rename breakage before run).
- ~30× faster parsing.

- **License**: mixed. Core engine is **ELv2** (source-available, not OSI open source); ADBC adapters and grammars Apache-2.0. ELv2 means you can use freely but can't offer it as a hosted service. Fine for solo / library use.
- **DuckDB story**: adapter exists and is actively developed, but warehouse adapters (Snowflake/BigQuery) get attention first.
- **Fit for your pain**: medium. Doesn't change the metric-DSL pattern but makes Jinja codegen safer. Pairs naturally with sqlglot-based custom macros.
- **Maturity**: dbt Labs commercial backing, but new code; expect rough edges through 2026.

### A3. SQLMesh (Tobiko Data, acquired by Fivetran 2025)

Apache-2.0 Python framework. ~3.1k stars, v0.234.1 (April 2026), very active. Closest functional replacement for dbt with explicit improvements over dbt's pain points. License is Apache-2.0 — Fivetran acquisition didn't change OSS terms.

Three features that map directly to your codebase:
1. **Blueprints**: a single SQL or Python model template expanded over a Python list of variants. Your `metric_table_generator(kind, grouping_keys, agg_type)` matrix becomes one blueprint with a list of `[{kind: "offense", agg_type: "player", grouping_keys: [...]}, ...]`. The 9 metric models become 1 blueprint definition.
2. **SQLMesh macros** (`@DEF`, `@EACH`, `@IF`) compile via SQLGlot — emitted SQL is type-checked at compile time, unlike Jinja text expansion.
3. **Python models** (`is_sql=True` returns SQLGlot expression or SQL string; `is_sql=False` returns DataFrame) execute *locally* in your Python process, unlike dbt's Python models which require warehouse-side Python. Pairs cleanly with Ibis (see B2).

Other relevant features:
- Virtual environments via view/table swap — zero-copy dev branches in DuckDB.
- First-class audits (data quality checks as `.sql` files referencing models).
- `dbt-import` command: one-shot conversion of a dbt project to SQLMesh.
- Coexists with dbt projects via `sqlmesh init -t dbt` (runs alongside, not full migration).
- DuckLake support shipped alongside DuckLake v1.0.

- **DuckDB story**: first-class. Quickstart uses DuckDB; engine integration is mature.
- **Fit for your pain**: high. Blueprints + Python+SQLGlot macros directly replace Jinja codegen patterns. Incremental adoption via dbt-import or coexistence.
- **Weaknesses**: half of SQLMesh's value is incremental-loading + virtual-env dev that you don't currently use (all `+materialized: table`, single-machine builds). Macro mental model shift from Jinja text to SQLGlot AST. Tobiko-now-Fivetran ownership is a soft watch.

### A4. Bruin (bruin-data, Go CLI)

Apache-2.0 Go binary, ~1.6k stars, v0.11.557 (April 2026). Asset-per-file with YAML frontmatter, Jinja2 supported but mostly for parameter substitution.

- **Fit for your pain**: misfit. Bruin's value is end-to-end (ingestion + transformation + checks + scheduling); you don't have ingestion or scheduling pain. Asset-with-frontmatter format is a regression from macro-based code-gen for your use case. Marketing now leans into "AI data team" / managed cloud direction.
- **DuckDB story**: first-class.
- **Worth knowing**: viable if you grow into ingestion needs later, but doesn't address the metric DSL.

### A5. lea (Carbonfact / Max Halford)

Apache-2.0 Python, ~321 stars, no formal releases (head-of-main development), production-used at Carbonfact. "Minimalist SQL orchestrator." DuckDB-native, Jinja-driven, "Quack mode" runs BigQuery scripts on DuckDB via transpilation. Recent DuckLake support.

- **Strengths**: small, file-based, philosophically aligned with your "no service" constraint. Existing macros port mechanically.
- **Weaknesses**: bus factor — single org, no tagged releases, smaller feature set than SQLMesh (no virtual envs, no SQLGlot-aware macros, no blueprints). Halford himself says "SQLMesh might be a better choice for starting fresh."
- **Fit for your pain**: medium. Minimal dbt replacement but doesn't level up the DSL.

### A6. Dagster + dbt-core

Keep dbt-core for transformation; replace dbt's CLI/scheduler with Dagster's asset graph.

- **Fit for your pain**: zero. You don't have orchestration pain. Skip unless you grow into multi-asset / multi-source orchestration.

### A7. yato (single-author, MIT)

The "smallest DuckDB orchestrator on Earth." Uses SQLGlot to infer the DAG from a folder of `.sql` files. ~200 stars, single maintainer.

- **Worth knowing**: useful as the *floor* of complexity. If your code-gen needs are met by Python files that emit SQL strings into a folder before yato runs, you don't need a framework at all. Not a serious candidate for 108 models with rich macros, but a sanity-check baseline.

### A8. Boring Data CLI

Generates Terraform/code templates to glue existing tools. **Not a transformation framework** — it scaffolds dbt rather than replacing it. Mention only because the same org publishes Boring Semantic Layer.

### Transformation-framework summary

| Tool | License | Server-free | DuckDB-first | Code-gen story | Coexists w/ dbt | Incremental adopt? |
|---|---|---|---|---|---|---|
| dbt-core (status quo) | Apache-2.0 | Yes | Yes (1.10) | Jinja text | N/A | N/A |
| dbt Fusion | ELv2 + Apache | Yes (CLI) | Adapter, lags | Jinja + parser | Drop-in | Yes (engine swap) |
| **SQLMesh** | **Apache-2.0** | **Yes** | **First-class** | **Blueprints + SQLGlot macros + Python** | **dbt-import / coexist** | **Yes** |
| Bruin | Apache-2.0 | Yes (Go) | First-class | YAML+Jinja | No (asset format diff) | Forklift |
| lea | Apache-2.0 | Yes | Native | Jinja only | No | Forklift |
| Dagster + dbt | Apache-2.0 | Library mode possible | Via dbt | Unchanged | Wraps dbt | Yes |
| yato | MIT | Yes | DuckDB only | Folder of SQL | No | Forklift |

---

## Axis B — Expression layer (replacing Jinja+SQL-strings with composable code)

### B1. Stay on Jinja + macros

What you have. Text templating with no AST awareness. Extensible only by writing more macros. The metric formula dict pattern is at the limit of what Jinja can express comfortably.

### B2. Ibis (Python, deferred-execution DataFrame API → compiles to SQL)

v12.0.0 (Feb 2026), Apache-2.0, ~6.5k stars, ~monthly releases. Compiles to ~20 backends; DuckDB is the default and best-supported. Expressions are first-class Python values — a metric is an object you can name, store in a dict, parametrize, combine.

**Voltron Data wrinkle (verified)**: VoDa laid off ~50% of staff late 2024, substantially de-staffed through 2025; Wes McKinney transitioned out of full-time CTO role; QuantStack stepped in to backfill Apache Arrow maintenance. Ibis itself is governance-independent (since 2022) and continued shipping (v12 in Feb 2026), but core-maintainer headcount is almost certainly lower than the 2023 peak. Apache-2.0 license + active community PRs mean a fork is always possible. Treat this as moderately elevated bus-factor risk, not existential.

How your patterns translate (concrete):

```python
# Today: Jinja dict of SQL strings, looped to emit columns
# {% set metrics = {"batting_average": "SUM(hits) / SUM(at_bats)", ...} %}
# {% for name, formula in metrics.items() %}{{ formula }} AS {{ name }},{% endfor %}

# Ibis: dict of Python expressions
metrics = {
    "batting_average": lambda t: t.hits.sum() / t.at_bats.sum(),
    "on_base_percentage": lambda t: t.on_base_successes.sum() / t.on_base_opportunities.sum(),
    # ... derived metrics reference others
    "ops": lambda t, m: m["on_base_percentage"](t) + m["slugging_percentage"](t),
}
agg = t.group_by(["player_id", "season"]).aggregate(
    **{name: f(t).name(name) for name, f in metrics.items()}
)
print(agg.compile())  # inspect generated DuckDB SQL
```

Window functions are first-class (`.over(ibis.window(group_by=..., order_by=..., preceding=N))`), so LAG/LEAD and rolling-window park-factor logic translates cleanly. Bitwise on `base_state` works (`t.base_state.bit_and(7)`). Scalar Python UDFs supported on DuckDB backend.

Integration paths with dbt (incremental):
- **dbt-ibis** (community package): models written as Ibis expressions, compiled to SQL, fed to dbt.
- **Codegen pattern**: write a Python script that emits SQL strings via Ibis `.compile()` and writes them into `bc/models/` for dbt to consume.
- **Inside SQLMesh**: Python models with Ibis-expressed SQL.

- **Strengths**: best fit for your codegen-shaped problem. DuckDB-native execution (compiles to SQL, doesn't exfiltrate). Window functions and CTEs map naturally. Composable — metrics are values.
- **Weaknesses**: bus factor (above). Generated SQL sometimes verbose vs hand-tuned (DuckDB optimizer usually doesn't care, but harder to audit). Migration of 108 models is substantial unless adopted incrementally.

### B3. Polars

In your deps already (`polars>=0.20.17,<2`). Eager + lazy DataFrame, Rust-native, MIT.

**Honest issue**: Polars wants data in Polars memory. Your park-factor pipeline is a multi-CTE join graph DuckDB's optimizer handles in-engine. Doing the same in Polars materializes intermediates in process memory. Codecentric's 2026 benchmarks show Polars hitting ~17 GB RSS on workloads where DuckDB stays at ~1.3 GB. Zero-copy via Arrow is real (`duckdb.sql("...").pl()`) but doesn't change the fact that you're moving compute *out* of DuckDB.

- **Verdict**: keep for one-off post-processing or where Arrow-frames are already in flight. Wrong primary tool for replacing the SQL transformation layer in this codebase.

### B4. PRQL

Pipelined relational query language. v0.13.12 (April 2026), 10.8k stars, Apache-2.0. DuckDB community extension lets you run PRQL in DuckDB. Maintainers acknowledge "development has slowed" while resolver redesign happens; sub-1.0 after 4+ years is a yellow flag.

- **Verdict**: solves "SQL is ugly to write," not "I want to compose metrics in code." You'd still need a host language to generate PRQL strings. Skip for your goal.

### B5. Malloy

A semantic-modeling-first language by Lloyd Tabb (Looker founder). MIT, ~2.5k stars. Compiles to SQL for ten+ engines including DuckDB. Killer feature: aggregate-locality semantics that prevent fan-out / chasm traps automatically — exactly your cross-grain problem.

```malloy
source: events is duckdb.table('event_offense_stats') extend {
  measure: total_h is h.sum()
  measure: total_ab is ab.sum()
  measure: batting_avg is total_h / total_ab
  measure: obp is (h.sum() + bb.sum() + hbp.sum()) / (ab.sum() + bb.sum() + hbp.sum() + sf.sum())
  measure: slg is tb.sum() / ab.sum()
  measure: ops is obp + slg
}
```

`malloy-py` (33 stars, smaller priority than TS core) provides a Python runtime that compiles `.malloy` files and either executes against DuckDB or returns SQL via `.get_sql()`. VS Code extension for authoring.

- **Strengths**: deepest semantic model — symmetric aggregates, `all()`/`exclude()` for cross-grain, nested queries as first-class output schemas. Designed by the LookML author.
- **Weaknesses**: new language to learn. `malloy-py` is auto-generated and lower priority than TS. No native YAML; you write `.malloy` files. Smaller community. Can express your metric registry well, but **not** programmatic loops over Python dicts — you'd be migrating from Jinja to Malloy syntax, not from string-generation to first-class composition.
- **Crossover**: Malloy is *both* an expression layer and a semantic layer. It appears again in axis C.

### B6. sqlglot directly (build-it-yourself)

Pure-Python SQL parser/transpiler/optimizer. ~9.2k stars, MIT, multiple releases per week. Substrate under SQLMesh and used by dbt Fusion.

```python
import sqlglot.expressions as exp
metrics = {
    "batting_average": exp.Div(
        this=exp.func("SUM", exp.column("hits")),
        expression=exp.func("SUM", exp.column("at_bats"))
    ),
}
select = exp.select(
    *[m.as_(name) for name, m in metrics.items()]
).from_("event_offense_stats")
print(select.sql(dialect="duckdb"))
```

You get an AST, not strings. Optimizer passes (column resolution, CTE inlining, dialect translation) come free. Netflix built their internal metrics platform on similar primitives.

- **Build calculus**: ~300–600 lines of Python wrapping sqlglot to model your `Metric(name, numerator, denominator, valid_grains)` registry and CTE-pipeline. Cost is ongoing maintenance of the resolver (cross-grain correctness, time-grain handling). Saving: total control, baseball-specific extensions (handedness splits, league-year park factors) attach directly to metric definitions.
- **When to pick**: if MetricFlow's YAML constraints feel limiting and you want full control. Pairs naturally with stay-on-dbt-core (replace `bc/macros/` with a Python module that writes SQL via sqlglot, dbt models become thin wrappers).

### B7. dbt Fusion (engine, not language)

Already covered in A2. As an *expression layer*, Fusion's contribution is column-level lineage and SQL-aware compilation of existing Jinja — which makes the same Jinja DSL safer but doesn't change its expressiveness.

### B8. DataFusion

Apache Arrow's Rust query engine, embeddable library. Replacing DuckDB itself is much bigger than replacing Jinja. **Out of scope.** Worth knowing only as "engine portability target if you adopt Ibis."

### B9. Substrait

Cross-language IR, not user-facing alone. Skip as candidate; mention as plumbing.

### B10. Hamilton (Apache, ex-DAGWorks)

Apache Software Foundation project as of 2024. ~2k stars, active. "dbt for Python functions" — DAG of decorated Python functions, where each function's parameters declare its dependencies. Materializers handle output (parquet, DuckDB, in-memory). Plays well with Ibis (functions can return Ibis expressions), Polars, pandas, scikit-learn, XGBoost.

**Crucial role here**: Hamilton is the natural complement to Ibis for the ML side of the project. The split:
- SQLMesh + Ibis: DAG of SQL/Ibis transformations producing analytical tables.
- Hamilton: DAG of Python functions producing ML features and trained models.

Both consume the same DuckDB; both run as libraries (no server). The seam is clean: Hamilton's leaves are usually feature tables or model artifacts; SQLMesh's leaves are aggregated tables. Either can call into the other (a Hamilton function can run an Ibis query; a SQLMesh Python model can call a Hamilton subgraph).

```python
# Hamilton example: feature engineering on Ibis tables
import ibis
from hamilton.function_modifiers import config

def player_season_features(events: ibis.Table, player_id: str) -> ibis.Table:
    return events.filter(events.player_id == player_id).group_by('season').aggregate(
        total_pa=events.pa.sum(),
        rolling_avg_ba=events.hits.sum() / events.at_bats.sum(),
    )

def projection_model(player_season_features: pd.DataFrame) -> XGBRegressor:
    model = XGBRegressor()
    model.fit(player_season_features.drop(columns=['target']), player_season_features['target'])
    return model
```

- **License**: Apache-2.0.
- **Bus factor**: Apache project — strong governance. DAGWorks (the company) is the primary maintainer but it's no longer single-vendor.
- **Strengths**: explicit DAG (testable, debuggable), no boilerplate, plays well with anything Pythonic. Excellent for the kinds of feature engineering and model-training pipelines that `bc/models/intermediate/machine_learning/` will house.
- **Weaknesses**: not a SQL replacement; pairs with one. Function-as-DAG style takes a beat to internalize but pays off fast.

### B11. Other (skipped)

- **Daft** — distributed Polars-alike with multimodal/AI focus. Same exfiltration concern as Polars. Skip.
- **Modin** — pandas API on Ray/Dask. Wrong shape. Skip.
- **Snowpark Python** — Snowflake-only. Skip.
- **Featuretools** — automated feature engineering. Library mode exists, but the abstraction adds more than it gains here; Hamilton + Ibis is more direct.
- **Feast** — feature store. Server required for full functionality. Skip.
- **MLflow** — model tracking. Has a library-only mode (file-based tracking URI). **Worth knowing about** for tracking ML experiments under Stack 3, but not core to the stack.

### Expression-layer summary

| Tool | License | Composable values | DuckDB native | Bus factor | Coexists w/ dbt |
|---|---|---|---|---|---|
| Jinja+SQL strings (status quo) | Apache | No (text) | Yes | None (dbt) | N/A |
| **Ibis** | **Apache-2.0** | **Yes (Python objects)** | **Yes (compiles to DuckDB SQL)** | **Elevated** | **Yes (dbt-ibis or codegen)** |
| Polars | MIT | Yes (Python) | No (exfiltrates) | Low | Yes (post-processing) |
| PRQL | Apache-2.0 | No (text) | Via extension | Yellow flag | Awkward |
| **Malloy** | **MIT** | **Yes (DSL)** | **Yes** | **Low (Google alumni)** | **Alongside, not in** |
| **sqlglot direct** | **MIT** | **Yes (AST)** | **Yes** | **Very low (load-bearing infra)** | **Yes (drops into macros)** |
| dbt Fusion | ELv2+Apache | No (text) | Adapter | Low (dbt Labs) | Engine replacement |

---

## Axis C — Semantic layer (replacing Jinja codegen of metric tables)

The hard constraint: no self-hosted server. This excludes Cube Core (Node API gateway + Rust CubeStore required), Lightdash, Looker, GoodData, Hex, Synmetrix, Drizzle Cube, and Rill (BI tool, `rill start` runs a server). All listed for completeness, not as candidates.

### C1. MetricFlow (dbt Labs, standalone)

Apache-2.0. Already in your dbt project structure; `docs/semantic_manifest.json` exists but is empty (semantic models / metrics / measures arrays are `[]`). Lives inside dbt-core; `pip install dbt-metricflow`; CLI `mf query --metrics ops --group-by season --explain` emits SQL. Does **not** require dbt Cloud.

Native types map cleanly to baseball:

```yaml
semantic_models:
  - name: events
    model: ref('event_offense_stats')
    entities:
      - name: player
        type: primary
        expr: player_id
      - name: season
        type: dimension
        expr: season
    measures:
      - {name: total_h, agg: sum, expr: hits}
      - {name: total_ab, agg: sum, expr: at_bats}
      - {name: total_bb, agg: sum, expr: bb}
      - {name: total_hbp, agg: sum, expr: hbp}
      - {name: total_sf, agg: sum, expr: sf}
      - {name: total_tb, agg: sum, expr: total_bases}
      - {name: obp_num, agg: sum, expr: 'h + bb + hbp'}
      - {name: obp_den, agg: sum, expr: 'ab + bb + hbp + sf'}

metrics:
  - name: batting_avg
    type: ratio
    type_params: {numerator: total_h, denominator: total_ab}
  - name: obp
    type: ratio
    type_params: {numerator: obp_num, denominator: obp_den}
  - name: slg
    type: ratio
    type_params: {numerator: total_tb, denominator: total_ab}
  - name: ops
    type: derived
    type_params:
      expr: 'obp + slg'
      metrics: [{name: obp}, {name: slg}]
```

This is the cleanest expression of OPS = OBP + SLG anywhere in this survey. Grain handled via entities — measure declared on `events` is queryable at any reachable grain via entity joins; if a grain isn't reachable, compile error.

- **Strengths**: native ratio + derived types. First-class grain via entities. Lives inside dbt — zero migration cost for orchestration. Can run against dbt's existing models (your aggregated tables become `semantic_models`). Battle-tested at scale.
- **Weaknesses**: YAML-only; no Python escape hatch for programmatic metric definition (you'd still maintain the metric registry as YAML files, possibly generated). Cross-grain joins through entities have known quirks. Time-grain handling assumes specific column conventions.
- **Materialization fit**: pairs with your dbt models naturally — define semantic models over existing aggregated tables (your `metrics_player_season_league_offense` becomes a `semantic_model`); MetricFlow compiles queries against them. Config-driven cache: dbt models are the cache; MetricFlow generates the queries. **Best match for the "config-driven cache, invisible materialization" pattern you described.**
- **MCP / agent story**: dbt Cloud has a hosted MCP, but the underlying compiler runs locally. You can wire local mf-compiled SQL to your own tools.

### C2. Malloy

Already covered as B5. As a semantic layer, Malloy's strengths over MetricFlow:
- Aggregate locality avoids fan-out/chasm traps automatically (no need to design entity graph carefully).
- Nested queries as output schemas — you can return hierarchical results (e.g., a player's season + nested rolling 10-game windows).
- `extend` for incremental measure additions to existing sources.

Weaknesses vs MetricFlow:
- New language. Single solo developer is bounded but real.
- No first-class derived metric *type* — composition is via measure references inside other measures, which works but the IR isn't as introspectable.
- Authoring/build pipeline integration weaker than MetricFlow-in-dbt.

### C3. Boring Semantic Layer (BSL)

`pip install boring-semantic-layer`. v0.3.12 (April 2026), MIT, ~436 stars. Backed by a small consultancy (Boring Data) + xorq-labs. Built on Ibis.

```python
from boring_semantic_layer import semantic_table

events = semantic_table(
    name='events',
    table=duckdb_conn.table('event_offense_stats'),
    primary_key='event_id',
).with_dimensions(
    player_id=lambda t: t.player_id,
    season=lambda t: t.season,
).with_measures(
    total_h=lambda t: t.hits.sum(),
    total_ab=lambda t: t.at_bats.sum(),
    batting_avg=lambda t: t.hits.sum() / t.at_bats.sum(),
)
result = events.query(
    dimensions=['player_id', 'season'],
    measures=['batting_avg']
).execute()
```

- **Strengths**: genuinely embeddable, pure Python. Same definition runs on event-grain DuckDB and on materialized aggregate parquets on R2 (Ibis swaps the backing table). MCP server option for AI consumers.
- **Weaknesses**: young, small (~436 stars). No first-class derived-metric type the way MetricFlow has — `ops = obp + slg` works as Ibis expression composition but isn't introspectable as "a derived metric." YAML mode explicitly doesn't support self-joins / `.all()`. Documentation thin past the basic count example. **OPS derived-tree spike strongly recommended before committing.**
- **Materialization fit**: Ibis-native — you'd configure which Ibis tables point at materialized aggregates vs event-grain. Config-driven materialization is build-it-yourself but tractable.

### C4. SQLMesh metrics

SQLMesh has a `METRIC(name ..., expression ...)` syntax with column-lineage-driven grain awareness. Apache-2.0, no server.

**Status: explicitly prototype.** Docs say "currently in a prototype phase and not meant for production use."

- **Worth tracking**, not worth building on yet. If SQLMesh metrics matures, a SQLMesh-everywhere stack becomes very attractive (axis A + B + C in one tool).

### C5. Custom-built (sqlglot + Python)

Already covered as B6. As a semantic layer specifically, this looks like:
- Python dataclasses: `Metric(name, type, numerator, denominator, valid_grains, agg_compatible)`.
- Registry module.
- `compile_metric(metric, grain) -> str` using sqlglot AST.
- Validation: `grain in metric.valid_grains`, fan-out detection via column-lineage.

Build calculus: 300–600 lines for MVP handling count + ratio + derived + grain validation. With Claude as copilot for sqlglot AST manipulation, more tractable than two years ago.

When to pick: if MetricFlow YAML feels limiting and you want richer baseball-specific metadata (handedness splits, league-year park factors, era adjustments) attached to metric definitions.

### C6. CUE for typed metric configs

Not a semantic layer — a typed configuration *language*. Unifies + validates configs. Could author metric definitions in CUE, get schema validation (e.g., `#Metric` requires numerator+denominator if `type: ratio`), then `cue export` to YAML/JSON for MetricFlow / BSL / custom layer.

Niche, but baseball metrics have lots of structural rules (rate stat denominator can't be zero, grain compatibility, etc.) that CUE could encode. Mention only as a complementary tool.

### C7. Excluded (server-required)

- **Cube Core**: Node API gateway + Rust CubeStore. No documented compile-only mode. Reference deployment is Docker. Excluded.
- **Drizzle Cube**: Cube-compatible TS-native. HTTP service. Excluded.
- **Lightdash, Looker, GoodData, Synmetrix**: hosted or self-hosted services. Excluded.
- **Rill**: BI tool. `rill start` launches server. Excluded for build pipeline; revisit as local dashboard tool layered on top.

### C8. Dark horses (verify before considering)

- **Semantica** (Hawksight-AI) — early-stage, "explainable semantic layers with provenance tracking." Probably not production-ready, but provenance graphs are interesting for baseball ("OPS+ → OPS → OBP → on-base events → game source").
- **Airbnb Minerva-style Python implementations** — multiple bloggers have built minimal Minerva-style semantic layers as reference designs.

### Semantic-layer summary

| Tool | Server-free | Native ratio | Native derived | Grain awareness | OPS expression | dbt coexist | Maturity |
|---|---|---|---|---|---|---|---|
| **MetricFlow** | **Yes** | **First-class** | **First-class** | **Via entities** | **Cleanest YAML** | **Lives inside dbt** | **High (dbt Labs)** |
| **Malloy** | **Yes (malloy-py)** | **Composed** | **By reference** | **Aggregate locality (auto)** | **Clean DSL** | **Alongside, not in** | **Medium-high** |
| **BSL** | **Yes** | **By composition** | **Ibis expression composition** | **By table structure** | **Needs spike** | **Yes (Ibis bridges)** | **Medium (small project)** |
| SQLMesh metrics | Yes | Yes | Yes | Column lineage | Should be clean | Within SQLMesh | Low (prototype) |
| Custom sqlglot | Yes | DIY | DIY | DIY | DIY | Yes | High but costly |
| Cube/Rill/etc | No | n/a | n/a | n/a | n/a | n/a | Excluded |

---

## Axis D — Vectorized event-sequence transformations

The user's structural insight: event data has a natural traversal order (`game_id, event_id`). Three flavors of pain in the current codebase exploit none of it:

1. **State machines disguised as windowed SQL.** `event_pitching_flags` does save/hold/blown-save propagation via three CTE-stages of `LAG/LEAD ... OVER (PARTITION BY game_id, batting_side ORDER BY event_id)` with `IGNORE NULLS` to carry flags forward. This is a literal state machine — would be ~30 lines of imperative Python over a sorted iterator.

2. **Bitfield interpretation that should be a UDF.** `event_baserunning_stats` decodes the 3-bit `base_state` integer through scattered `CASE WHEN base_state >> 2 & 1 = 0` patterns + multi-table joins. The actual logic is "given a base_state and a runner role, return derived flags."

3. **Sort-shared joins as hash joins.** `event_states_full` joins 4 tables on `event_key`/`game_id` — but all those tables share the natural `(game_id, event_id)` sort order. These are merge joins masquerading as hash joins.

### D1. Stay in SQL but harvest 2025–2026 ergonomic primitives

Free wins available without leaving dbt:

- **`QUALIFY`** — filter on a window function without wrapping in a CTE. Would shorten `event_pitching_flags.sql` measurably (the conditional-flag CTEs collapse).
- **Recursive CTEs with `USING KEY`** (DuckDB May 2025) — the recursive CTE becomes a keyed dictionary that updates payloads instead of unioning rows. Big win for graph-shape problems (runner advancement traversal). Less helpful for ordered-stream FSMs.
- **`list_reduce` over `array_agg(... ORDER BY ...)`** — fold over a sorted array of structs inside SQL. Works for simple FSMs, awkward when state has lookahead (and your save logic does — `pitcher_finish_flag` looks ahead via `LEAD`).
- **DuckDB ASOF JOIN** — for nearest-time-match joins between sorted tables. Doesn't replace `LAG IGNORE NULLS` carry-forward but cleans up adjacent patterns.
- **Stream windowing functions** (DuckDB blog, May 2025) — `tumble`, `hop`, `slide` for time-binned aggregations. Not state machines but signal that DuckDB is investing in this area.

**Verdict**: low-cost, low-ceiling. Use these regardless of which axis-D primary path you pick.

### D2. DuckDB Python UDFs — scalar, vectorized, table-returning

DuckDB 1.5 has `con.create_function(..., type='arrow')` for scalar UDFs (zero-copy PyArrow batch in/out, ~2k-row chunks) and table-returning UDFs via experimental Python API. **Critical limitation: scalar UDFs are stateless across chunks** — you cannot carry `save_situation_start_flag` forward across a chunk boundary in a scalar UDF. Two workarounds: (a) pre-shape with `array_agg` and pass the whole partition as a list to a UDF that runs the FSM, then `unnest`; (b) use a Python table function over an already-grouped Arrow batch.

**Status (April 2026)**: scalar/vectorized UDFs stable. Table-returning UDFs experimental. **Python UDAFs (aggregate UDFs) still don't ship from Python in 2026** — issue #5116 open. UDAFs are written in C++ today; quack-rs (see D5) just made Rust UDAFs ergonomic. **DuckDB user-defined window functions landed on main in April 2026 with blocking and streaming APIs — but only via the C/Rust extension surface, no Python yet.**

**Fit for FSM-class logic**: workable but awkward. Pre-aggregation forces materialization per partition. **Verdict**: viable for irregular logic where Polars expressions don't fit and you don't want to write Rust yet.

### D3. Polars expressions (`forward_fill().over(...)` and friends)

Polars 1.31+ has the new streaming engine; your pin (`polars>=0.20.17,<2`) is behind. Bumping to 1.31+ gets you native parallel forward-fill within partitions. The save-flag carry-forward becomes:

```python
import polars as pl
df.lazy().sort('event_id').with_columns(
    pl.col('save_eligible_start_flag')
      .forward_fill()
      .over(['game_id', 'batting_side', 'pitcher_id'])
      .alias('save_carry')
).with_columns(
    save_flag=pl.col('save_carry') & pl.col('conditional_save_flag')
).collect(streaming=True)
```

**Performance**: native Rust, no GIL on the expression path, parallelizes across cores. Streaming engine spills to disk. Known issue: streaming `group_by` memory blowup (Polars #25607 still open early 2026); `forward_fill().over()` is fine.

**Integration**: Polars and DuckDB share Arrow buffers zero-copy. Run as a dbt-duckdb `python` model returning a Polars DataFrame; DuckDB consumes it natively. SQLMesh has Python models that take DataFrames natively.

**Fit**: cleanest expression-level path for stateful columnar ops that decompose to `forward_fill`, `cum_*`, `rolling_*`, etc. The save-flag logic factors that way. **Verdict**: best Python-side primary tool for state-machine class logic.

### D4. Numba `@jit` over numpy from PyArrow

For irregular FSMs that don't decompose to columnar ops — or that have multi-flag interdependence the way save vs blown-save vs hold do. Pattern: extract numpy arrays from a sorted Arrow batch via `pa.Array.to_numpy(zero_copy_only=True)`, run `@njit` function with raw imperative loop, return numpy → Arrow → DuckDB.

```python
from numba import njit
import numpy as np

@njit(cache=True)
def save_fsm(game_id, pitcher_id, eligible_start, cond_save):
    n = len(game_id)
    out = np.zeros(n, dtype=np.bool_)
    carry = False
    last_g, last_p = -1, -1
    for i in range(n):
        if game_id[i] != last_g or pitcher_id[i] != last_p:
            carry = bool(eligible_start[i])
            last_g, last_p = game_id[i], pitcher_id[i]
        elif eligible_start[i] != -1:  # NULL sentinel
            carry = bool(eligible_start[i])
        out[i] = carry and bool(cond_save[i])
    return out
```

**Performance**: native LLVM-compiled code, no GIL inside, 50–200× faster than pure Python loops. **License**: BSD-2. Bus factor moderate (Anaconda-funded, small team) but API has been stable for years.

**Fit**: the imperative-hot-path option. Wrap as a DuckDB Arrow UDF (or call from a `python` dbt/SQLMesh model). **Weakness**: nullable handling is annoying (use sentinels or extract validity buffer separately); compile overhead first call (mitigated by `cache=True`).

**Verdict**: paired with Polars (D3), this covers nearly all your stateful-event-stream needs without leaving Python.

### D5. Rust DuckDB extensions via `quack-rs`

`quack-rs` 0.4.0 (March 2026) is a production-grade Rust SDK for DuckDB extensions. Type-safe builders for `ScalarFunctionBuilder`, `AggregateFunctionBuilder`, `TableFunctionBuilder`, plus `cargo`-based scaffold. Tested against DuckDB 1.4.4 and 1.5.0. Combined with DuckDB's new user-defined window functions (April 2026), `save_fsm()` becomes a SQL-callable function with the same performance and parallelism as DuckDB's built-in windows.

**Distribution**: `.duckdb_extension` binary per platform, signed. The DuckDB community-extensions registry handles distribution (`INSTALL save_fsm FROM community`). Extra friction vs Python but durable.

**Fit**: the long-term home for *settled* hot-path logic. Same SQL surface as today's window functions; same speed; reusable across any DuckDB binding (CLI, Python, JS, R, future tools).

**Verdict per user's stated preference**: iterate in Python (D3+D4), graduate to Rust extension (D5) once a state machine is stable. Don't start in Rust.

### D6. Skip: in-process stream processors

Surveyed and rejected:
- **bytewax**: last OSS release v0.21.1 in November 2024; key tooling archived. Effectively dormant.
- **arroyo**: acquired by Cloudflare 2025; project focus moved to Workers integration.
- **pathway**: embeddable but real-time-AI focused; overkill for batch parquet.
- **Feldera**: actively developed, VLDB'23 paper. Genuinely interesting but the runtime model is "push deltas in, push deltas out" — not "transform a parquet file." Worth filing for a future *incremental rebuild* story (e.g., "maintain dbt outputs as new games arrive") but not for current batch shape.

The "Flink-as-library" category mostly doesn't fit batch event processing. Right primitive isn't a stream processor; it's a state-machine UDF or a smarter parser.

### D7. DataFusion / Acero streaming exec from Python

DataFusion exposes streaming exec from Python (`datafusion-python`); core streaming for windowed aggregates exists. Heavier lift than DuckDB UDFs, no obvious win except cross-compatibility with Arrow Rust ecosystem. **Verdict**: not primary; mention as "if I ever leave DuckDB" hedge.

### Axis-D summary

| Tool | License | In-process | DuckDB integration | Statefulness | Performance | Bus factor | Effort |
|---|---|---|---|---|---|---|---|
| **SQL ergonomic primitives (QUALIFY, USING KEY, list_reduce)** | MIT | Yes | Native | Limited (no carry-forward across chunks except via list_reduce) | DuckDB engine speed | Excellent | Free |
| DuckDB Python scalar UDFs | MIT | Yes (GIL on call) | Zero-copy Arrow | Stateless | Fast (native compute inside UDF) | Excellent | Low |
| DuckDB Python table UDFs | MIT | Yes (GIL) | Zero-copy Arrow | Per-group materialized | Slower (per-group) | Excellent | Low-medium |
| **Polars expressions (forward_fill / over / cum_*)** | **MIT** | **Yes (no GIL on expr path)** | **Zero-copy Arrow** | **Yes (columnar)** | **Native Rust, parallel** | **Excellent** | **Low** |
| **Numba @njit over numpy** | **BSD-2** | **Yes (no GIL inside jit)** | **Via Arrow.to_numpy zero-copy** | **Yes (imperative)** | **LLVM-compiled native** | **Moderate (Anaconda)** | **Medium** |
| **Rust DuckDB ext via quack-rs** | **MIT/Apache** | **Yes (in DuckDB worker threads)** | **First-class** | **Yes (full control)** | **Same as DuckDB built-ins** | **Small (single-author crate)** | **High** |
| Stream processors (bytewax/arroyo/pathway) | Mixed | Various | Weak | Yes | Various | Mostly bad | Skip |
| DataFusion streaming | Apache-2 | Yes | Via Arrow | Yes | Native | Apache project | High, no clear win |

---

## Cross-cutting: Pushing logic upstream into `boxball-rs`

`boxball-rs` already parses Retrosheet event files in event-sorted order. That parser sees events in their natural traversal *before* they ever land in parquet. Anything that's a function of the per-game stream — counts, base-state, pitcher transitions, save eligibility — can be computed once, at parse time, and emitted as parquet columns. The dbt models then become projections instead of computations.

This eliminates the problem class for a subset of models rather than optimizing them.

**What fits upstream (push to `boxball-rs`)**:
- Definitionally rule-bound logic: save-eligibility windows, base-state encoding, count tracking, frame/inning state, base-out state transitions. Any logic dictated by the rulebook that won't change.
- Logic that depends only on the per-game event stream (no joins, no analytical comparison across games).

**What stays downstream (in dbt / SQLMesh / Python)**:
- Analytical flags whose definition might evolve.
- Park factors and other cross-game statistical models.
- Metric definitions and aggregations.
- Anything joining across games / seasons / leagues.

**Trade-offs**:
- *Iteration speed*: Python-side flag tweaks become Rust-side parser changes + parquet rebuild + R2 republish. Slower to iterate.
- *Versioning*: parquet on R2 must be re-uploaded when the FSM changes. Cheap if `boxball-rs` is fast.
- *Testability*: same FSM in two test surfaces (Rust unit tests + dbt SQL tests). Pick one.
- *Boundary discipline*: the contract with the parquet schema becomes more load-bearing.

**Migration shape**: incremental — push one settled FSM upstream at a time. Validate by diffing parquet output before/after, then drop the now-redundant dbt model.

**Cross with axis D**: this is *complementary* to axis D, not a replacement. Settled rulebook logic graduates from "Python FSM in dbt-duckdb model" → "Rust FSM in `boxball-rs` parser." The intermediate stop is "Python FSM that you've ironed out and validated"; once it's stable, it can either go to a Rust DuckDB extension (D5, stays in the analytical layer) or to `boxball-rs` (this section, leaves the analytical layer entirely). The choice depends on whether the logic is a baseball-rule fact (→ `boxball-rs`) or an analytical artifact (→ DuckDB extension).

---

## Cross-cutting: DuckLake as publish-layer replacement

DuckLake is a lakehouse format from DuckDB Labs: metadata in a SQL catalog (SQLite, Postgres, or DuckDB), data files as Parquet in object storage. v1.0 released April 13, 2026 with backward-compatibility guarantees. Top-10 DuckDB extension by usage. DuckDB Labs benchmarks show 926× faster queries vs Iceberg (their own benchmarks, take with salt — but the format is genuinely simpler than Iceberg).

**Public read-only DuckLake on R2 is officially documented** (e.g., tobilg's `cloudflare-ducklake` reference). Pattern: build the DuckLake locally, detach the catalog, upload SQLite catalog file + Parquet data to R2 with appropriate caching headers. Consumers attach via `ATTACH 'ducklake:https://data.baseball.computer/baseball.ducklake' AS bc;` and get full schema, snapshots, time-travel — without running any service.

For your specific publish layer (`scripts/create_web_db.py` exports parquet to R2 + recreates `bc_remote.db` with views pointing at HTTPS URLs):

| Capability | Today (parquet + DuckDB views) | DuckLake |
|---|---|---|
| Stateless consumer | Yes | Yes (single attach) |
| Schema in artifact | Yes (DuckDB views encode it) | Yes (catalog) |
| Snapshots / time-travel | No | Yes |
| Cache-bust on republish | Querystring trick | Catalog snapshots make this clean |
| ENUM types preserved | Yes (in DuckDB views) | Yes (DuckLake supports DuckDB types) |
| Discoverability | Manual docs | Catalog itself |

**Both SQLMesh and lea support DuckLake natively** as a backend. dbt-duckdb can write DuckLake via the extension but doesn't have first-class adapter integration yet (verify current state before committing).

This is largely orthogonal to which transformation framework you choose, but the migration story differs:
- **DuckLake from SQLMesh**: cleanest — SQLMesh has a tutorial.
- **DuckLake from dbt**: works via the DuckDB extension, but not a first-class adapter feature. Likely some custom hooks.
- **DuckLake from lea**: Halford has blogged about it; works.

**Recommendation for this axis** (the one place this doc takes a position): DuckLake is a strict upgrade for your publish layer. The migration is bounded (it's just where you write the output), can run alongside the current parquet output during transition, and the consumer story is meaningfully better. **Worth a separate spike independent of transformation/semantic-layer choices.**

---

## Cross-cutting: Init layer — sources, ENUMs, R2 cache-bust

`bc/macros/init_db.sql` is the highest-friction migration boundary. It iterates `graph.sources` (dbt-specific) to register parquet/CSV sources, and procedurally creates DuckDB ENUMs from data values via separate `CREATE TYPE` statements. Three migration patterns:

1. **Stay on dbt**: keep the macro as-is. Easiest.
2. **Move to SQLMesh**: convert to a Python pre-build hook or a SQLMesh "before_all" macro. SQLMesh's macro language can iterate over a config dict (replacing `graph.sources`); ENUM creation becomes either pre-build initialization or a SQLMesh model materialized as a `MATERIALIZED VIEW` with type cast (less elegant). Estimate: 1–2 days.
3. **Move to DuckLake**: ENUMs become catalog metadata; cache-bust querystring goes away (catalog snapshots replace it). Source registration still needed but simpler (DuckLake handles the parquet-on-R2 indirection).

ENUM stability: DuckDB 1.5.2 ENUMs are stable. Your `notes/upgrade-spike.md` flagged this as "highest concern" but the bump succeeded. Not a blocker for any path.

---

## Cross-cutting: Bus-factor / maturity assessment

You said cutting-edge OK, moderate risk acceptable. Honest ranking:

**Low risk** (well-funded, large community, stable):
- dbt-core, dbt Fusion (dbt Labs commercial backing)
- SQLMesh (Tobiko/Fivetran, Apache-2.0)
- MetricFlow (dbt Labs, Apache-2.0)
- DuckDB + DuckLake (DuckDB Labs, MotherDuck commercial)
- sqlglot (load-bearing infra for SQLMesh + dbt Fusion + Superset + Dagster)
- Malloy (Google alumni team, MIT, modest community but engaged)

**Moderate risk** (smaller team or recent disruption):
- **Ibis** — independent governance but Voltron Data wound down ~50% in 2024–2025. v12 shipped Feb 2026; still active. Forkable. Not existential but not what it was in 2023.
- BSL — small consultancy + xorq-labs. ~436 stars. Active but tiny.

**Higher risk** (small/solo or pre-1.0):
- lea — single org (Carbonfact), no formal releases.
- PRQL — sub-1.0 after 4+ years; "development slowed" per maintainers.
- SQLMesh metrics — explicit "prototype" status.
- yato — single maintainer, ~200 stars.
- Semantica — early-stage research project.

---

## Cross-cutting: Incremental migration paths

You said "incremental, slice by slice." Concretely, the lowest-risk slices ranked by ease + payoff:

1. **MetricFlow on top of existing dbt models** (1–2 days for a slice). Define semantic models for one domain (offense), define metrics, run `mf query --explain` against existing aggregated tables. No model changes. Validates the metric DSL replacement before any deeper migration.

2. **Replace one Jinja macro with sqlglot Python** (1 day). Pick `metric_calcs.sql`. Write a Python module that exposes the same dict but as sqlglot expressions. Have a thin Jinja macro call out to it via `{{ run_query() }}` or compiled-output codegen. Removes string-formula opacity without touching model graph.

3. **DuckLake publish layer** (3–5 days). Add a parallel publish target that writes DuckLake to R2 alongside the current parquet pipeline. Lets you compare consumer experience without breaking anything.

4. **Migrate one slice (offense metrics) to SQLMesh** (1 week). `sqlmesh init -t dbt`, port the offense-metric blueprint, run alongside dbt for the rest. Test virtual-env dev workflow.

5. **Ibis pilot for park factors** (1 week). The single most macro-heavy model (`calc_park_factors_advanced.sql`, Bayesian shrinkage with rolling windows, parametrized by 6 inputs) is the best stress test for Ibis expressivity. Port it to Python+Ibis, compile to DuckDB SQL, diff outputs against current dbt model. If this works, the rest of the codebase will too.

6. **Polars `forward_fill().over()` reimplementation of `event_pitching_flags`** (2–3 days). Port the save/hold/blown-save state machine to a dbt-duckdb `python` model returning a Polars DataFrame. Diff outputs against the current SQL model. Validates axis-D feasibility on the canonical FSM target. Requires Polars version bump (current pin `>=0.20.17,<2`; need `>=1.31` for streaming engine).

7. **Numba @njit + Arrow for one irregular FSM** (2–3 days). Pick a logic that doesn't decompose to columnar ops cleanly and write the imperative version. Wrap as a DuckDB Arrow UDF. Validates the imperative-hot-path path.

8. **`boxball-rs` upstream pushdown of one settled FSM** (1–2 weeks, includes Rust changes). Pick base-state encoding or count tracking. Add columns to the parquet output. Strip the corresponding dbt model. Validates the architectural pushdown story.

Greenfield rewrite **not recommended** given (a) 108 models with subtle baseball-domain logic, (b) no test suite to catch regressions, (c) you said incremental.

---

## Recommended stack combinations

Five complete stacks, ordered from least to most ambitious. Each is internally coherent. Each now also specifies an axis-D event-locality strategy. None is "the" recommendation — picking among them depends on weights you haven't expressed yet.

### Stack 1 — Minimum viable upgrade ("kill the metric DSL pain only")

- **Transformation**: dbt-core 1.10 (status quo). Optionally upgrade to dbt Fusion engine when its DuckDB story matures.
- **Expression**: Jinja for orchestration; **sqlglot in Python** replaces `metric_calcs.sql` and `stat_lists.sql`. Macros become thin wrappers calling `bc/python/metrics.py`.
- **Semantic**: **MetricFlow** on top of existing aggregated tables. Define semantic models pointing at `metrics_player_season_league_offense` etc. Metrics defined once in YAML; consumed via `mf query`.
- **Event-locality (axis D)**: **SQL ergonomic primitives only** (`QUALIFY`, `USING KEY`, `list_reduce`). Free wins; doesn't address the deeper structural issue.
- **Publish**: **DuckLake on R2** replaces `create_web_db.py`'s parquet+views.
- **Effort**: 2–3 weeks.
- **Risk**: very low. All proven tools, all coexist.
- **Wins**: kills the formula-as-string problem; introduces config-driven semantic layer with native ratio/derived types; modernizes publish layer.
- **Limits**: doesn't solve the 9-tables-rebuild-on-metric-change problem (still pre-aggregating in dbt). Code-gen burden eased but not eliminated. Event-locality structural issues untouched.

### Stack 2 — Incremental SQLMesh adoption

- **Transformation**: **SQLMesh** introduced via `sqlmesh init -t dbt`, coexists with dbt during migration. Blueprints replace `metric_table_generator.sql`. SQLMesh macros (Python+SQLGlot) replace Jinja macros where AST-awareness matters.
- **Expression**: SQLMesh Python models with sqlglot expressions for codegen-heavy paths; SQL+SQLMesh-macros for everything else.
- **Semantic**: **MetricFlow** alongside SQLMesh (compiles to SQL against SQLMesh-built tables) or **wait for SQLMesh metrics to mature** if you want one-tool-fits-all.
- **Event-locality (axis D)**: **SQL ergonomics + SQLMesh Python models with Polars** for state machines. SQLMesh's Python models are a clean home for `forward_fill().over()` style logic.
- **Publish**: **DuckLake on R2** (SQLMesh has a clean tutorial).
- **Effort**: 4–6 weeks for a clean transition.
- **Risk**: low-medium. SQLMesh is mature; the macro mental shift is real.
- **Wins**: blueprints directly express the (kind × agg_type × grouping_keys) matrix that's awkward in Jinja; column-level lineage; virtual envs for safer dev; SQL-aware compile-time validation; clean Python-model surface for Polars-based FSM rewrites.
- **Limits**: half of SQLMesh's value (incremental loading, virtual envs) doesn't apply to all-table materialization. Macro mental shift.

### Stack 3 — Python-first with Ibis

- **Transformation**: **SQLMesh** as orchestration shell, with **Python models written in Ibis** for codegen-heavy logic.
- **Expression**: **Ibis** as the primary expression layer. Metric formulas become Python expressions (`{name: lambda t: t.hits.sum() / t.at_bats.sum()}`). Stat lists become Python lists. Park factor logic ported from Jinja-templated SQL to Ibis Python (Bayesian shrinkage with rolling windows is well-handled).
- **Semantic**: **BSL** (Ibis-native, composes with the rest) OR **MetricFlow** if you want first-class derived-metric type. Run BSL OPS spike to decide.
- **Event-locality (axis D)**: **Ibis windowed expressions for declarative cases + Polars/Numba for FSMs**. Ibis handles `forward_fill`-shape via its window API. Mix Numba `@njit` for irregular FSMs.
- **Publish**: **DuckLake on R2**.
- **Effort**: 8–12 weeks (Ibis migration of 108 models is substantial; can stage by domain).
- **Risk**: medium. Ibis bus-factor concern. BSL is small.
- **Wins**: most expressive of all options. Metrics, stat lists, park factor params all become first-class Python data. Dev workflow becomes "import baseball; baseball.metrics.batting_avg" — same Python in scratch.ipynb that runs in the build.
- **Limits**: bigger commit. Ibis-generated SQL sometimes verbose. If Ibis ecosystem deteriorates further, fork or rebuild on sqlglot directly.

### Stack 4 — Malloy for the semantic layer

- **Transformation**: dbt-core (status quo) or SQLMesh.
- **Expression**: Malloy `.malloy` files for metric and source definitions. Build pipeline: Malloy compiles to SQL → dbt/SQLMesh runs the SQL → DuckDB executes.
- **Semantic**: **Malloy** itself.
- **Event-locality (axis D)**: **SQL ergonomics + DuckDB Arrow UDFs (Numba-backed) for FSMs**. Malloy doesn't have a Python escape hatch; FSM logic stays in dbt/SQLMesh-native models that wrap UDFs.
- **Publish**: DuckLake on R2.
- **Effort**: 6–10 weeks. Includes language-learning curve.
- **Risk**: medium. New language, smaller community than dbt or SQLMesh.
- **Wins**: aggregate locality (no fan-out/chasm trap mistakes), nested queries, `extend` for incremental measure additions. Cross-grain done right by construction.
- **Limits**: new language, weaker Python authoring story. Authoring workflow shifts from Jinja-in-SQL to `.malloy` files (different mental model). Less integrated with dbt orchestration than MetricFlow.

### Stack 5 — Event-locality first ("smart parser, thin analytical layer")

This stack treats the event-locality insight as primary, not ancillary. The architecture becomes a stratified pipeline where event-stream FSMs live where they belong (in `boxball-rs`) and the analytical layer focuses on cross-game aggregation and metrics.

- **Upstream (`boxball-rs`)**: settled rulebook FSMs computed during parse. Save eligibility, base-state derivation, count tracking, frame/inning state, base-out transitions all become parquet columns. The Rust parser becomes the single source of truth for per-game-stream facts.
- **Transformation**: **SQLMesh** for the analytical layer. Many of the current `intermediate/event_level/`, `intermediate/states/`, `intermediate/flags/` models disappear or become projections. The remaining transformations are genuinely cross-game (joins, aggregations, statistical models).
- **Expression**: **Ibis or sqlglot in Python**. Park factors and similar statistical models stay in the analytical layer (they need cross-game data) but become Python expressions. Metric DSL via sqlglot (Stack 1 style) or Ibis (Stack 3 style).
- **Semantic**: **MetricFlow** (cleanest YAML; pairs with SQLMesh outputs) or **BSL** (if axis B chose Ibis).
- **Event-locality (axis D)**: **Push to `boxball-rs` for settled logic; Polars/Numba for analytical FSMs that remain.** Iterate-Python-graduate-Rust progression: new FSMs start as Polars-expression Python models in SQLMesh, and once stable either graduate to (a) a Rust DuckDB extension via `quack-rs` (if they're analytical) or (b) `boxball-rs` (if they're rulebook).
- **Publish**: **DuckLake on R2**.
- **Effort**: 12–16 weeks. Largest commitment; touches the Rust upstream repo, the analytical layer, and the publish layer. Highest payoff if executed.
- **Risk**: medium-high. Cross-repo coordination (`boxball-rs` ↔ `baseball.computer`); contract changes to the parquet schema; iteration speed loss for upstream-pushed FSMs.
- **Wins**: eliminates whole classes of dbt models. Performance jumps materially (parser computes once; analytical layer doesn't redo state-machine work). Conceptual clarity: "rulebook in Rust, analysis in SQL+Python." Same FSM logic, exposed as parquet columns, becomes available to any downstream consumer of the published artifact, not just the analytical layer.
- **Limits**: iteration on rulebook flags slows (Rust + parquet rebuild loop). Two-test-surface problem (Rust unit tests + analytical-layer tests). Larger up-front design needed for the parquet schema contract.

---

## Stack 3 — phased implementation plan (chosen path)

This section assumes Stack 3 has been selected: **SQLMesh + Ibis + BSL/MetricFlow + Hamilton + DuckLake**, with axis-D event-locality handled by Polars/Numba in SQLMesh Python models. Long-format base tables with on-demand semantic-layer reshape; cached wide views for hot queries.

The plan starts with SQLMesh adoption (the single most-traveled migration path away from dbt) and layers in the rest incrementally. Each phase produces a working pipeline; you can ship between phases.

### Phase 0 — Verification spikes (1 week, parallelizable)

Run before committing time to the full migration:

1. **`sqlmesh init -t dbt` on this codebase**: does dbt-import cleanly translate `init_db`, `metric_table_generator`, and the macro suite? Identifies friction up front. (Verification spike #4.)
2. **Ibis port of `calc_park_factors_advanced.sql`**: the codebase's hardest expression. If Ibis can express Bayesian shrinkage with rolling windows cleanly, it can express anything else. (Verification spike #6.)
3. **BSL OPS-derived-tree spike**: define `obp`, `slg`, `ops` in BSL; verify v2's "first-class graph nodes" introspect cleanly without flattening. If not, fall back to MetricFlow for the semantic layer. (Verification spike #1.)
4. **Polars `forward_fill().over()` for `event_pitching_flags`**: does the save-flag FSM decompose to columnar ops? Determines whether axis-D Phase 5 needs Numba fallback. (Verification spike #7.)
5. **Polars version bump test**: bump `polars>=0.20.17,<2` → `polars>=1.31`. Run scratch.ipynb usage; verify nothing breaks.
6. **DuckLake + dbt-duckdb status check**: confirm the current state of the dbt-duckdb adapter's DuckLake support, since SQLMesh's path is cleaner. May affect Phase 4 timing.

Deliverables: spike results dictate whether to proceed, switch BSL→MetricFlow, or escalate any blockers.

### Phase 1 — SQLMesh transition (3–4 weeks)

The most-traveled migration path. Don't change semantics here; just change the engine.

1. `sqlmesh init -t dbt`. SQLMesh runs alongside dbt initially.
2. Convert `init_db.sql` macro to a SQLMesh `before_all` hook (or a Python pre-build script that issues the `CREATE TYPE` and parquet-source-registration DDL).
3. Convert `metrics_table_generator.sql` macro to a SQLMesh **blueprint** model. The blueprint key is `{kind, agg_type, grouping_keys}`; this directly replaces the 9-table Jinja codegen. Output is the same wide tables for now.
4. Convert remaining macros (`metric_calcs`, `stat_lists`, `park_factors`) to SQLMesh-native macros. Where Jinja was doing string templating, SQLMesh macros use SQLGlot AST — same logic, type-checked at compile time.
5. Run SQLMesh + dbt in parallel. Diff outputs row-by-row for one full build (use the `differ` package or `duckdb`-side row-hash comparison). Investigate any deltas.
6. Cut over: run SQLMesh as the primary build; archive the dbt project.

End of phase: SQLMesh is the engine. Output is byte-equivalent to current dbt build. Metric DSL still lives in Jinja-flavored SQLMesh macros.

### Phase 2 — Ibis expression layer (3–4 weeks)

Replace the macro DSL with Python-first composability.

1. Pick the codegen-heaviest models for Ibis ports:
   - `metric_table_generator` blueprint → SQLMesh **Python model** that builds the aggregation via Ibis.
   - `metric_calcs.sql` formula dicts → Python module of Ibis lambdas.
   - `stat_lists.sql` → Python lists.
   - `park_factors.sql` → SQLMesh Python model with Ibis (Bayesian shrinkage rolling-window logic in Ibis windowed expressions).
2. SQL-only models that don't benefit from Ibis (simple staging, simple joins) stay as SQLMesh SQL models.
3. Validate output equivalence against Phase 1 outputs.

End of phase: metric definitions live as Python objects. `import baseball; baseball.metrics.batting_avg` works. Park factors readable by anyone who knows Python + numpy. The dev workflow merges with `scratch.ipynb` — same Python that runs in the build runs interactively.

### Phase 3 — Semantic layer (2–3 weeks)

Decision point informed by Phase 0 BSL spike: BSL or MetricFlow.

1. **If BSL**: define semantic tables in Python (`semantic_table(events, ...).with_measures(...)`). Same Ibis expressions as Phase 2 power the measures. MCP server option ships for free if you want LLM-queryable metrics later.
2. **If MetricFlow**: define `semantic_models` and `metrics` in YAML. `mf query --metrics ops --group-by season --explain` emits SQL. Cleaner derived-metric type but YAML-only.
3. Decide the schema shape: long-format base tables + on-demand wide reshape via `mf query` or BSL `.execute()`. For the most-queried wide views, build SQLMesh models that materialize them as a cache on top of the long base. Consumers querying the full wide table go through the cache; ad-hoc analysis hits the semantic layer directly.
4. Update `bc/macros/metric_calcs.sql`'s former responsibilities to be the semantic-layer measure definitions.
5. Validate metric values against Phase 2 outputs for top-20 metrics.

End of phase: metrics defined once, consumed many ways. Adding a metric edits one file. Adding a grain is a query, not a model.

### Phase 4 — DuckLake publish layer (1–2 weeks)

Replace the parquet+remote-views pattern.

1. Configure SQLMesh's DuckLake target (the SQLMesh + DuckLake tutorial covers this).
2. Build pipeline writes DuckLake catalog (SQLite) + parquet data files to R2.
3. Add R2-side caching headers; configure consumer attach pattern.
4. Validate consumer experience: `ATTACH 'ducklake:https://data.baseball.computer/baseball.ducklake' AS bc;` should expose the same tables and ENUMs as today's `bc_remote.db`.
5. **Cutover is automatic** (per user decision): publish only DuckLake, retire `create_web_db.py`'s parquet+views path.

End of phase: published artifact is one DuckLake. Consumers attach once and get full schema, snapshots, time-travel.

### Phase 5 — Axis D event-locality (ongoing, 4–6 weeks for first wave)

Revisit the heaviest event-stream models with axis-D primitives.

1. Bump Polars to `>=1.31` (verified safe in Phase 0).
2. Rewrite `event_pitching_flags` as a SQLMesh Python model using `pl.col(...).forward_fill().over(...)`. Validate output equivalence; measure build-time delta.
3. Rewrite `event_baserunning_stats` — split into a Numba `@njit` function for the bitfield decoding (registered as a DuckDB Arrow UDF) + a standard SQL/Ibis projection over the decoded columns.
4. Audit other windowed-CTE models for `forward_fill`-shape opportunities.
5. SQL ergonomics: insert `QUALIFY` and `USING KEY` recursive CTEs where they shorten existing models.

End of phase: state-machine logic lives where it belongs (Python with sorted iteration, not stacked windowed SQL). Build performance improves on these models.

### Phase 6 — ML re-enablement with Hamilton (parallel/post, 4–6 weeks)

The ML branch (`bc/models/intermediate/machine_learning/`, currently `+enabled: false`) returns as a Hamilton DAG.

1. Add Hamilton (`sf-hamilton`) to dependencies.
2. Build feature-engineering DAG in Hamilton: functions take Ibis tables as input, return Ibis tables (or Polars/pandas DataFrames where vectorized libraries shine).
3. Model training pipeline: Hamilton functions consume feature tables and return fit scikit-learn / XGBoost / statsmodels objects. MLflow file-mode for tracking experiments.
4. Hamilton DAG runs as a separate build target, on top of SQLMesh outputs. Same DuckDB, different DAG; Hamilton's outputs (model artifacts, prediction tables) materialize back into DuckDB or to R2.
5. Re-enable the dbt models that were disabled, now expressed as Hamilton outputs surfaced through SQLMesh.

End of phase: ML and analytical pipelines coexist cleanly. Same DuckDB, two complementary DAGs. Adding an ML feature is a Hamilton function; adding a metric is a semantic-layer measure.

### Phase 7 — Optional graduations (deferred)

Wait until specific FSMs are stable before deciding:

- **Rust DuckDB extensions** (via `quack-rs` + new UDWFs): graduate any analytical FSM that's been stable for 6+ months and is performance-critical. Distribute via DuckDB community-extensions registry.
- **`boxball-rs` upstream pushdown**: graduate rulebook FSMs (save eligibility, base-state encoding) that haven't changed in years. Coordinated parquet-schema change.

End of phase: the most settled hot-path logic lives in native Rust, callable from SQL. The analytical layer is leaner.

### Phase summary

| Phase | Duration | Cumulative | Ship milestone |
|---|---|---|---|
| 0 — Verification spikes | 1 wk | 1 wk | Spike results decide stack defaults |
| 1 — SQLMesh transition | 3–4 wks | 5 wks | SQLMesh as primary engine |
| 2 — Ibis expression layer | 3–4 wks | 9 wks | Metric DSL in Python |
| 3 — Semantic layer | 2–3 wks | 12 wks | Metrics defined once, queryable many ways |
| 4 — DuckLake publish | 1–2 wks | 14 wks | Single attach for consumers |
| 5 — Axis D event-locality | 4–6 wks | 20 wks | Polars/Numba for FSMs |
| 6 — ML re-enablement | 4–6 wks (parallel) | 20–26 wks | ML pipelines back online |
| 7 — Optional graduations | Deferred | TBD | Settled FSMs in Rust / parser |

Total realistic timeline: **5–6 months for phases 1–6**. Phases 1–4 ship continuously; the user can stop and ship at any phase boundary.

---

## Decision matrix (fill in your own weights)

The choice depends on what you weight. Rough scoring across criteria (1=worst, 5=best, my judgment):

| Criterion | Stack 1 (dbt+sqlglot+MF) | Stack 2 (SQLMesh+MF) | Stack 3 (SQLMesh+Ibis+BSL) | Stack 4 (Malloy) | Stack 5 (Event-locality) |
|---|---|---|---|---|---|
| Effort (low better) | 5 | 4 | 2 | 3 | 1 |
| Expressiveness | 3 | 4 | 5 | 4 | 5 |
| Bus-factor safety | 5 | 4 | 3 | 4 | 3 |
| Coexists with current dbt | 5 | 4 | 3 | 3 | 2 |
| Solves metric DSL pain | 4 | 5 | 5 | 5 | 5 |
| Solves cross-grain (player-game vs season vs career) | 4 (MetricFlow entities) | 4 (MetricFlow entities) | 3 (BSL needs spike) | 5 (aggregate locality) | 4 (MetricFlow + parquet schema) |
| First-class derived metrics (OPS = OBP + SLG) | 5 (MF native) | 5 | 3 (Ibis composition) | 4 | 5 |
| Solves event-locality (state machines / bitfields) | 1 (SQL ergonomics only) | 3 (Polars in Python models) | 4 (Ibis + Polars + Numba) | 2 (UDFs only) | 5 (parser pushdown + Polars/Numba/Rust ext) |
| Build performance gains | 1 | 2 | 3 | 2 | 5 |
| Cutting-edge / interesting | 2 | 4 | 5 | 5 | 5 |
| MCP / agent-friendly | 3 (dbt MCP exists) | 3 | 4 (BSL has MCP) | 3 | 4 |

---

## Open verification spikes (before committing)

These are the load-bearing unknowns that should be answered with a 1-day spike before picking a stack:

1. **OPS derived-metric in BSL**: does BSL v2's "first-class graph nodes" let `ops = obp + slg` introspect cleanly? Or does it flatten to a fresh ratio computation? Affects Stack 3 viability.
2. **MetricFlow performance on DuckDB**: how fast does `mf query` compile + execute against your 645-column tables? Affects Stack 1 viability for ad-hoc analysis.
3. **DuckLake + dbt-duckdb**: status of first-class adapter integration. If poor, Stack 1's publish-layer story needs custom hooks.
4. **SQLMesh dbt-import on this codebase**: does it cleanly convert the `init_db` macro and the `metrics_table_generator` blueprint pattern? Affects Stack 2/3 effort estimate.
5. **Malloy multi-source joins** (event-grain + season-grain + park-factor join) in malloy-py: does it work, what's the SQL it produces? Affects Stack 4.
6. **Ibis park-factor port**: pick `calc_park_factors_advanced.sql` and port it to Ibis. The Bayesian shrinkage with rolling windows is the codebase's hardest expression. If this works, Ibis works for everything.
7. **Polars `forward_fill().over()` for `event_pitching_flags`**: does the save-flag FSM decompose cleanly to columnar ops? Or does the multi-flag interdependence (save vs blown-save vs hold uses three different `LAG IGNORE NULLS` references) force fallback to `map_groups` or Numba? Affects Stack 2/3/5 axis-D viability.
8. **`boxball-rs` upstream pushdown of base-state encoding**: smallest unit that's clearly definitionally rulebook. Add columns to parser output, drop downstream model, diff parquet outputs. Validates Stack 5 architecture.
9. **`quack-rs` Rust extension for `save_fsm`**: end-to-end build of one Rust DuckDB extension. Validates the "graduate to Rust" pattern and surfaces distribution friction (per-platform binaries) before betting on it as the long-term home.

---

## Resolved decisions

These open questions were closed during the interview:

- ✅ **Schema shape**: long-format base + on-demand wide reshape via semantic layer; cached wide views for hot queries. Consumer migration acceptable.
- ✅ **ML re-enablement**: ML drives Stack 3 choices; Hamilton is part of the initial stack, not bolted on later. Phase 6 of the implementation plan.
- ✅ **CI/CD**: manually orchestrated today, with help from sister `baseball.computer` repos. Not a constraint on tool choice.
- ✅ **Multiple authoring surfaces tolerance**: not a real concern.
- ✅ **DuckLake cutover**: automatic, no parallel-publish window required.
- ⏸️ **Stack 5 FSM upstream/downstream split**: deferred (Stack 3 chosen, but axis-D Phase 7 graduations still apply).
- ⏸️ **Two-repo parquet schema contract cadence**: deferred (relevant only for axis-D Phase 7 if `boxball-rs` pushdown happens).

### Phase 0 spike outcomes (2026-05-01)

All six 1-day verification spikes (`notes/spikes/`) completed green. Spike-driven decisions:

- ✅ **Semantic layer = BSL** (not MetricFlow): Spike 3 confirmed `ops = obp + slg` is a first-class `[calc]` graph node in BSL 0.3.12, row-equivalent to dbt across 284 (season, league) groups within 1e-9. MCP-for-LLMs path stays free. MetricFlow fallback installed but unused.
- ✅ **Axis-D Phase 5 = pure Polars** (Numba fallback not load-bearing): Spike 4 reproduced the `event_pitching_flags` save/hold/blown-save FSM via `forward_fill().shift(1).over(group, order_by=event_id)` chains. 13/13 columns row-equivalent on a 225K-row season slice. Numba (D4) stays available for genuinely row-iterative work but isn't required for the planned axis-D port.
- ✅ **Phase 4 publish = SQLMesh-driven DuckLake** (no parallel dbt-duckdb lane): Spike 6 found dbt-duckdb 1.10.1 supports DuckLake via `type: ducklake` / `is_ducklake: true` but feature-thin (no `partition_by`, no SORTED BY — open issues). SQLMesh's `catalogs.<name>.type: ducklake` config is the canonical tested path. DuckDB-native ATTACH round-trip works clean on duckdb 1.5.2.
- ✅ **Phase 1 SQLMesh adoption is bounded**: Spike 1 ran `sqlmesh init -t dbt duckdb` against a copy of `bc/`. 128/128 models parse; `calc_park_factors_advanced` and `event_pitching_flags` both render clean SQL. `metrics_table_generator` Jinja codegen fails SQLMesh's relation introspection on DuckDB ENUMs (port to a SQLMesh blueprint, ~3–4d). `init_db` macros (`graph.sources` iteration) need port to a Python pre-build hook (~2d). Total manual macro work ≤ 1 week, well under the Phase 1 budget.
- ✅ **Ibis is sufficient for Phase 2**: Spike 2 ported `calc_park_factors_advanced` (the codebase's hardest model — 7 CTEs, 2-yr RANGE window, Bayesian-shrinkage UNION, self-join, odds-ratio aggregate). 26/26 row count match; 16/24 columns exact at 1e-2; 8 columns drift on a single root-cause cast-tower issue (1 day of production-port work to fix). sqlglot-direct fallback (B6) not needed.
- ✅ **Polars version bump**: Spike 5 confirmed Polars 1.40.1 (resolved from `>=1.31`) runs all repo + spike-relevant ops clean (zero deprecation warnings). Production pin relaxed from `>=0.20.17,<2` to `>=0.20.17` (drop the upper cap) in the Phase 0 foundation commit.

The migration base branch (`next`) and Phase 0 deliverables (`phase-0-spikes`) merge cleanly. Phase 1 (`phase-1-sqlmesh`) opens off `next`.

## Verification plan (if user picks a stack and wants to execute)

For any chosen stack, validation looks like:
1. Pick one model domain (offense — best documented, most metrics) and run end-to-end: build, output, compare to current dbt output for byte-equivalence (or value-equivalence within tolerance for floats).
2. For semantic layer: `mf query` (or BSL `.execute()`) for the top 20 metrics, diff results against current `metrics_player_season_league_offense` table values.
3. For DuckLake: parallel publish; compare consumer experience attaching to DuckLake vs the current `bc_remote.db`.
4. Roll forward one slice at a time; keep the dbt path live until each slice is validated.

---

## Summary

The user's instinct that there's a "more efficient way to assemble the data" is correct, and the 2026 ecosystem has more (and more promising) options than even a year ago. The honest takeaway:

- **Stack 3 chosen**: SQLMesh + Ibis + (BSL or MetricFlow) + Hamilton + DuckLake. Starting with SQLMesh adoption is a well-traveled migration path.
- **Two highest-leverage wins, both addressed by Stack 3**: (1) replacing the Jinja metric DSL with Ibis-expressed metrics + a semantic layer, (2) embracing event-locality by mixing Polars/Numba (and eventually Rust) with declarative SQL inside SQLMesh Python models.
- **DuckLake as the publish layer** unifies the schema, snapshot, and time-travel story behind a single consumer attach.
- **Hamilton for ML** keeps the analytical and ML DAGs cleanly separated while sharing a common DuckDB. Apache governance lowers bus-factor risk vs single-vendor alternatives.
- **`boxball-rs` upstream pushdown** stays in scope as a Phase 7 graduation for settled rulebook logic. Defer the specific split decision until axis-D Phase 5 has matured a few FSMs in Python.
- **Stream processing libraries (bytewax, arroyo, pathway)** that initially looked relevant don't fit the batch-parquet shape. Right primitive isn't a stream processor; it's a state-machine UDF, a smarter parser, or columnar Polars expressions.
- **Iterate-Python-graduate-Rust** pattern maps to phases 5 → 7. Don't start in Rust.

Next concrete step: run **Phase 0 verification spikes** (1 week, mostly parallelizable). Spike outcomes determine BSL vs MetricFlow for the semantic layer and surface any SQLMesh dbt-import friction before Phase 1 begins.
