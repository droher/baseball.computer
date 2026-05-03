# Migration plan & status

**Status:** Phases 0–3 shipped. Phase 4 publish-side shipped (validation
phase — site cutover deferred to Phase 4.x; see
`notes/phase-4-followups.md`).

The original evaluation that compared 5 candidate stacks across axes A–D
lives in git history (`af2f1ee` and earlier). This doc has been pared
back to (a) the chosen path, (b) what's already landed, and (c) the
remaining phases with the detail they warrant given what we've learned.

## Stack

**Stack 3** — selected after Phase 0 spikes:

- **Transformation:** SQLMesh (Apache-2.0, DuckDB-first). dbt removed in
  Phase 1.5; nothing left of the dbt project.
- **Expression:** Ibis on top of SQLMesh Python models for
  codegen-heavy logic. Pydantic `Metric` registry under
  `bc/python_models/metrics/` is the single source of truth for the 9
  `metrics_*` tables (148 registrations across offense/pitching/fielding
  × season/event). `bc/python_models/park_factors/` ports the
  `batter_pitcher_park_factor` macro plus `calc_park_factors_{advanced,
  basic}`.
- **Semantic layer:** BSL (Boring Semantic Layer, Ibis-native). Spike 3
  confirmed `ops = obp + slg` is a first-class graph node. MetricFlow
  fallback installed but not used.
- **ML pipeline:** Hamilton (Apache project). Re-enables the disabled
  `intermediate/machine_learning/` branch in Phase 6.
- **Publish layer:** DuckLake on R2 via SQLMesh (`catalogs.<name>.type:
  ducklake`). Spike 6 ruled out the dbt-duckdb DuckLake adapter.
- **Schema shape:** long-format base + on-demand wide reshape via the
  semantic layer; cached wide views materialized for hot queries.
- **Event-locality:** Polars 1.40 `forward_fill().shift(1).over(...)` in
  SQLMesh Python models for FSMs. Spike 4 confirmed `LAG IGNORE NULLS`
  decomposes cleanly. Numba `@njit` stays available for genuinely
  row-iterative work but isn't load-bearing.

## Phase status

| Phase | Status | Branch / commits | Followups doc |
|---|---|---|---|
| 0 — Verification spikes | ✅ shipped 2026-05-01 | `phase-0-spikes` (merged) | `notes/spikes/README.md` |
| 1 — SQLMesh transition | ✅ shipped | `phase-1-sqlmesh` (merged) | `notes/phase-1-followups.md` |
| 1.5 — dbt removal | ✅ shipped | merged into 1 | (covered in 1-followups) |
| 1.6 — jinja → Python `@macro` | ✅ shipped | `phase-1.6-cleanup` (merged) | `notes/phase-1.6-followups.md` |
| 2 — Ibis expression layer | ✅ shipped | `phase-2-ibis`, audit sweep | `notes/phase-2-followups.md` |
| 3 — BSL semantic layer | ✅ shipped | `phase-3-bsl` | `notes/phase-3-followups.md` |
| 4 — DuckLake publish (publish side) | ✅ shipped | `phase-4-ducklake-publish` | `notes/phase-4-followups.md` |
| 4.x — DuckLake site cutover | deferred | — | `notes/phase-4-followups.md` |
| 4.5 — Incremental kinds for hot tables | pending | — | `notes/phase-4-followups.md` |
| 5 — Axis-D event-locality | pending | — | — |
| 6 — ML re-enablement (Hamilton) | pending | — | — |
| 7 — Optional graduations (Rust ext / `boxball-rs` pushdown) | deferred | — | — |

Each phase ships independently; can stop at any phase boundary.

## Shipped — recap

### Phase 0

Six 1-day spikes, all green. Outcomes that shape the rest of the plan:
- **Semantic layer = BSL** (not MetricFlow). Confirmed in Spike 3.
- **Axis-D = pure Polars** (Numba fallback not load-bearing). Spike 4
  reproduced the `event_pitching_flags` save/hold/blown-save FSM via
  `forward_fill().shift(1).over(...)`; 13/13 columns row-equivalent on
  225K rows.
- **Phase 4 publish = SQLMesh-driven DuckLake**, not parallel dbt-duckdb.
  dbt-duckdb 1.10.1 supports DuckLake but is feature-thin (no
  `partition_by`, no `SORTED BY`).
- **Polars pin relaxed** from `>=0.20.17,<2` to `>=0.20.17`. Polars
  1.40.1 runs all repo + spike-relevant ops with zero deprecation
  warnings.

### Phase 1 + 1.5 + 1.6

- `sqlmesh init -t dbt` translation, then full dbt removal: no
  `dbt_project.yml`, no `packages.yml`, no `dbt_packages/`, no `target/`,
  no jinja shim.
- Source loading: `bc/external_models.yaml` is the single source of
  truth for 45 parquet sources across 6 schemas. `init_db()`,
  `create_enums()`, `alter_types()`, `load_seeds()` fire from
  `config.before_all`.
- All 44 `JINJA_QUERY_BEGIN ... JINJA_END` blocks across 25 models
  removed; `for`-loops over stat lists are now `@EACH(@list_macro(), x
  -> ...)` over Python `@macro`s under `bc/macros/_*.py`.
- YAML metadata (descriptions, type contracts, audits, download_parquet
  URLs, relationships tests) all migrated into MODEL blocks.
- Custom audits: `relationships`, `bounded_range`, `sum_consistency`,
  `valid_baseball_season`, `unique_grain` (composite-key uniqueness, since
  the built-in `unique_values((a, b))` checks columns individually).

### Phase 2 (+ audit sweep)

- 17 models ported `.sql` → `.py`: 9 `metrics_*`, 6 `calc_park_factor_*`,
  2 `calc_park_factors_{advanced,basic}`.
- `bc/python_models/`: Pydantic `Metric` registry, Ibis SQL builders,
  `_doc_lookup`, ENUM helpers. Not auto-loaded by SQLMesh — imported by
  `.py` models.
- The two big macro-based codegen surfaces (`@metric_table_body`,
  `@batter_pitcher_park_factor`) and the four `calc_park_factor*`
  macro/SQL artifacts deleted.
- Diff harness (`scripts/diff_models.py` + `diff_known_flaky.json`)
  validates row-by-row against prod with per-column tolerances.
- Audit coverage broadened to 46 model files; data-integrity findings
  A–E fixed (franchise seed gaps, dashed retrosheet IDs, Lahman
  per-stat supplement for `player_team_season_pitching_stats`,
  park-factor `HAVING SUM(denom) > 0` + `bounded_max` parameter).

### Carried-forward open issues (still open)

These survive Phase 2 and should be worked across the remaining phases
when convenient:

- **Latent nondeterminism (13–14 tables).** Window-tie ordering and ENUM
  sort-order ties produce different-but-valid outputs on different
  builds. Allowlisted in `scripts/diff_known_flaky.json`. Root cause for
  the worst offender (`team_game_start_info` doubleheader handling) is
  upstream in `baseball.computer.rs`; once the parser disambiguates
  dh-status from the `game_id` suffix, several cascade tables stabilize.
- **Partial-coverage SUMs.** Lahman per-stat supplement only catches
  *all-NULL* upstream cases. Partial-coverage years (1901–07 BFP,
  1903–09 ER) need `IF(BOOL_OR(col IS NULL), NULL, SUM(col))` at both
  `box_agg` and per-season SUM sites. Probably wants a `@nullable_sum`
  helper macro.
- **Apply Lahman supplement to batting.**
  `player_team_season_offense_stats.sql` likely has the same shape bug
  for early-NA SO/CS/SH (and the post-1920s SH/SF era when SF wasn't
  separated). Mirror the pitching restructure.
- **Park-factor priors for sparse leagues.** Even with `bounded_max=20`,
  6 residual NN1/NN2 spatial-distribution outliers could be dampened
  further by per-league `prior_sample_size` (e.g. 5000 for NN1/NN2 vs
  1000 default).

---

## Phase 3 — BSL semantic layer

**Goal:** metrics defined once, consumed many ways. Adding a metric edits
one Python file. Adding a grain becomes a query, not a model.

The Pydantic `Metric` registry from Phase 2 is already
semantic-layer-shaped. `Metric` has `formula`, `numerator+denominator`,
and `derived` fields; only `derived` is unimplemented. Phase 3 turns
`derived` on and reuses the same `Metric` objects for both build-time
table generation (`build_metric_sql`) and runtime BSL semantic-table
measures.

### 3.1 Turn on `derived` composition

Replace inlined ratio-of-ratio formulas with composed metrics. The
canonical examples:

- `ops = obp + slg` (currently inlined as one formula lambda for each
  variant)
- `known_trajectory_out_hit_ratio = known_trajectory_rate_outs /
  known_trajectory_rate_hits` (Phase 2 inlines the dependency)
- `walks_per_strikeout = walks / strikeouts` etc.

`Metric` already has `derived: Callable[[TableExpr, dict[str, IbisExpr]],
IbisExpr]`. Implementation:

- Rewrite `build_metric_sql` as a two-pass evaluator: pass 1 computes
  `formula` and `numerator/denominator` metrics into an Ibis `mutate()`
  dict; pass 2 evaluates `derived` lambdas against that dict.
- Topological sort the registry so derived metrics resolve in dependency
  order (`ops → obp, slg`; `wRC+ → wRC, league_wRC, park_factor`; etc.).
  Cycle detection at registration time.
- Test invariant: rebuilt `metrics_*` tables row-equivalent to Phase 2
  output for every column.

### 3.2 BSL semantic tables

Stand up one BSL `semantic_table` per kind (offense, pitching, fielding)
on top of the long-format `event_*_stats` tables (pre-aggregation) and
one per kind on top of the `metrics_*` aggregated tables (cache). Same
`Metric` objects power measures in both — the only difference is the
backing Ibis table.

```python
from boring_semantic_layer import semantic_table
from python_models.metrics.registry import metrics_for

events = semantic_table(
    name='offense_events',
    table=ibis.table('event_offense_stats'),
    primary_key='event_id',
).with_dimensions(
    player_id=lambda t: t.player_id,
    season=lambda t: t.season,
    league=lambda t: t.league_id,
    park=lambda t: t.park_id,
).with_measures(**{
    m.name: m.evaluate for m in metrics_for('offense', source='event')
})
```

Two consumption modes:
- **Long-format ad-hoc:** `events.query(dimensions=['player_id',
  'season'], measures=['ops']).execute()`. Runs against event grain.
- **Cached wide views:** for the most-queried groupings (player-season,
  team-season, player-career), keep the `metrics_*` materializations
  from Phase 2 as a cache. BSL points its semantic table at the
  pre-aggregated table; same measure objects, faster query.

### 3.3 Phase 3 deliverables

- ✅ `Metric.derived` evaluator + cycle detection. Two-pass
  `evaluate_all` in `bc/python_models/metrics/registry.py`; topo sort
  with cycle + missing-dep diagnostics; `_DepCaptureProxy` for static
  introspection.
- ✅ BSL semantic tables for offense/pitching/fielding × event/season.
  Six factories in `bc/semantic/tables.py`. Same `Metric` objects
  power both build-time and runtime.
- ✅ Top-20 metrics validated against Phase 2 outputs (1e-9 tolerance).
  See `bc/tests/test_bsl_semantic.py` (top-50 2024 batters,
  OBP/SLG/OPS row-equivalent) and `bc/tests/test_derived_metrics.py`
  (algebraic checks on synthetic memtable).

See `notes/phase-3-followups.md` for the env-split rationale (sqlglot
27 vs 30 via xorq pin), BSL event-grain regular-season filter
discrepancy, and the schema impact of the coverage-weighted registry
expansion.

---

## Phase 4 — DuckLake publish

**Goal:** consumers attach once and get full schema, snapshots,
time-travel. Replaces the parquet-files + `bc_remote.db` views pattern.

Spike 6 settled the path: SQLMesh-driven DuckLake on R2, no parallel
dbt-duckdb lane.

### Shipped — publish side (validation phase)

The publish-and-upload pair runs alongside the existing
`scripts/create_web_db.py` flow. The original parquet+views artifact is
still the canonical one; site cutover is tracked separately
(see §4.x below and `notes/phase-4-followups.md`).

- `bc/config.py` adds a second SQLMesh catalog `bc_publish` via
  `DuckDBAttachOptions(type="ducklake", path=..., data_path=...,
  data_inlining_row_limit=0)`. Local catalog at `bc/bc_publish.ducklake`
  (DuckDB-backed), data dir `bc/bc_publish_data/`. The data path is
  recorded relative so the catalog file is portable: when uploaded to
  R2, consumers attach by URL and DuckLake resolves data files against
  the catalog URL's parent.
- `scripts/publish_ducklake.py` reads `main_models.*` + `main_seeds.*`
  out of `bc.db` and rewrites them into `bc_publish.*`. ENUM columns
  are cast to VARCHAR — DuckLake v1.0 doesn't preserve user-defined
  types (the official migration script in DuckLake's docs does the
  same cast). Catalog options set once: `parquet_compression=zstd`,
  `parquet_row_group_size=1966080`, `data_inlining_row_limit=0`. The
  smaller row groups + GZIP that `create_web_db.py` uses for
  `event_states_full` are not yet reproducible per-table in DuckLake —
  options are catalog-wide. Tracked as Phase 4.5.
- `scripts/upload_ducklake.py` uploads `bc/bc_publish.ducklake` to
  `s3://timeball/baseball/v<DATA_VERSION>/baseball.ducklake` and the
  data dir under the same prefix, preserving the
  `bc_publish_data/<schema>/<table>/<file>.parquet` layout that the
  catalog references relatively. `Cache-Control: public,
  max-age=31536000, immutable` on data files; long-lived but not
  immutable on the catalog (replaced each build); Cloudflare cache
  purge fires for the catalog URL after upload.

### Consumer attach (validation only)

```sql
INSTALL ducklake; LOAD ducklake;
ATTACH 'https://data.baseball.computer/baseball/v1/baseball.ducklake'
    AS bc (TYPE ducklake, READ_ONLY);
SELECT * FROM bc.main_models.metrics_player_season_league_offense LIMIT 10;
```

Note: when attaching by HTTPS URL, the canonical syntax is
`(TYPE ducklake)`, not the `ducklake:` URL-prefix form. The
`ducklake:` prefix is for local file / postgres / sqlite catalog
backends.

### 4.x Site cutover (deferred)

Triggers — the site team confirms on a test branch that:

- DuckLake query results match `bc_remote.db` for a representative
  sample of site queries.
- Cold-attach latency is acceptable (single catalog fetch + lazy
  parquet reads vs the current single-DB-file fetch).
- VARCHAR-instead-of-ENUM is acceptable for site code (no ENUM-typed
  filters or joins broken).
- The LLM-metadata bridge (`notes/llm-metadata.md`) consumes the
  DuckLake table layout cleanly, or works against either artifact.

When cutover lands: delete `scripts/create_web_db.py`, stop publishing
the `dbt/` R2 prefix, update README + site docs to point exclusively
at DuckLake, optionally purge the old `dbt/` prefix after a grace
window.

### 4.5 Incremental kinds for hot tables

DuckLake snapshot retention is cheap when models are
incremental — only changed partitions get new files; older snapshots
keep referencing unchanged ones. Today every model in this repo is
`kind FULL`, so retaining N snapshots costs roughly N × full table
size (~40 GB × N). Once event-grain `metrics_*` and `event_states_full`
move to incremental kinds, snapshot retention drops to a thin overlay
on the immutable base. Out of scope for the Phase 4 PR.

---

## Phase 5 — Axis-D event-locality

**Goal:** state-machine logic lives where it belongs (Python with sorted
iteration), not as stacked windowed CTEs.

Spike 4 + Spike 5 settled the path: pure Polars in SQLMesh Python
models. Numba (D4) stays available for genuinely row-iterative cases
but isn't required for the planned ports.

### 5.1 First wave

1. **`event_pitching_flags`.** Save/hold/blown-save FSM. Three CTE
   stages of `LAG/LEAD ... OVER (PARTITION BY game_id, batting_side
   ORDER BY event_id) IGNORE NULLS` collapse to
   `forward_fill().shift(1).over([...], order_by='event_id')` chains.
   Spike 4 reproduced this on a 225K-row season slice.
2. **`event_baserunning_stats`.** Bitfield decoder. Split into:
   - A pure Polars / Ibis projection over the decoded `base_state` bits
     (`t.base_state.bit_and(7)`-style), or
   - A Numba `@njit` registered as a DuckDB Arrow UDF if the multi-flag
     interdependence makes Polars expressions ugly.
3. **Audit other windowed-CTE models.** Anything matching the `LAG
   IGNORE NULLS` shape is a candidate.
4. **SQL ergonomics.** Insert `QUALIFY` (filter on a window function
   without wrapping in a CTE) and recursive CTE `USING KEY` (DuckDB May
   2025) where they shorten existing models. Free wins regardless of
   the Python-model rewrites.

### 5.2 What stays in SQL

- Aggregations (`metrics_*` Phase 2 work — Ibis on top of SQL).
- Cross-game joins (`event_states_full` and similar).
- Anything declarative.

The principle: declarative aggregation in SQL/Ibis; stateful per-event-stream
logic in Python with sorted iteration.

---

## Phase 6 — ML re-enablement (Hamilton)

**Goal:** the disabled `bc/models/intermediate/machine_learning/` branch
returns as a Hamilton DAG that consumes SQLMesh outputs.

Hamilton is Apache-governed (DAGWorks → ASF in 2024). Function-as-DAG
style; each function's parameters declare its dependencies. Outputs
materialize back into DuckDB or to R2.

### 6.1 Shape

- **SQLMesh + Ibis** owns the analytical DAG (aggregated tables,
  metrics, park factors).
- **Hamilton** owns the ML DAG (feature engineering, training,
  prediction). Functions return Ibis tables, Polars/pandas DataFrames,
  or fitted scikit-learn / XGBoost / statsmodels objects.
- **MLflow file-mode** for experiment tracking (no server).
- Hamilton's leaves materialize back into DuckDB (prediction tables) or
  to R2 (model artifacts). SQLMesh re-enables the previously disabled
  models, now reading Hamilton outputs.

### 6.2 Seam

Both DAGs share one DuckDB. Hamilton functions can call into Ibis
queries; SQLMesh Python models can call Hamilton subgraphs. Adding an
ML feature is a Hamilton function; adding a metric is a Phase 3
semantic-layer measure.

Can run in parallel with Phase 5.

---

## Phase 7 — Optional graduations (deferred)

The "iterate-Python-graduate-Rust" tail. Wait until specific FSMs are
stable for 6+ months before deciding.

### 7.1 Rust DuckDB extensions via `quack-rs`

For analytical FSMs that have stabilized in Phase 5 and are
performance-critical. `quack-rs` 0.4+ is a production-grade Rust SDK
for DuckDB extensions; combined with DuckDB's user-defined window
functions (April 2026), state-machine logic becomes a SQL-callable
function with the same parallelism as built-in windows. Distribution
via the DuckDB community-extensions registry.

### 7.2 `boxball-rs` upstream pushdown

For *rulebook* FSMs (save eligibility, base-state encoding, count
tracking, frame/inning state, base-out transitions) that haven't
changed in years. The Rust parser sees events in their natural
traversal order *before* they ever land in parquet — anything that's a
function of the per-game stream can be computed once at parse time and
emitted as parquet columns.

Trade-offs:
- *Iteration speed*: Python-side flag tweaks become Rust-side parser
  changes + parquet rebuild + R2 republish.
- *Versioning*: parquet on R2 must be re-uploaded when the FSM changes.
- *Testability*: same FSM in two test surfaces (Rust unit tests +
  analytical-layer audits). Pick one.

The choice between 7.1 and 7.2 turns on whether the logic is a
baseball-rule fact (→ `boxball-rs`) or an analytical artifact (→ DuckDB
extension).

---

## Phase summary

| Phase | Status |
|---|---|
| 0 — Spikes | ✅ |
| 1 + 1.5 + 1.6 — SQLMesh + dbt removal + Python `@macro` | ✅ |
| 2 — Ibis | ✅ |
| 3 — BSL semantic | ✅ |
| 4 — DuckLake publish (publish side) | ✅ |
| 4.x — DuckLake site cutover | deferred |
| 4.5 — Incremental kinds for hot tables | pending |
| 5 — Axis-D event-locality | pending |
| 6 — Hamilton ML | parallel/post-5 |
| 7 — Rust ext / `boxball-rs` pushdown | deferred |

Critical path: 4.x (site cutover) → 5, with Phase 6 paralleling 5. The
LLM-metadata bridge can start now that Phase 4's publish side has
landed; it just needs to point at whichever artifact (`bc_remote.db`
or DuckLake) the site cutover settles on.

The **LLM-metadata bridge** (`notes/llm-metadata.md`) is a parallel
work stream, not a migration phase. It depends on Phase 3 (the metric
registry it consumes is the BSL source of truth) and Phase 4 (the
artifact it ships into). Bridge work — `metadata/` YAML cards, JSON
Schemas, compile pipeline, `CREATE MACRO` emission, sample-value
sampling, artifact upload — can proceed in parallel with the site
cutover trigger evaluation.
