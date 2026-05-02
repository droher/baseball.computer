---
name: sqlmesh
description: Use when working with SQLMesh — writing or editing MODEL blocks, Python @model decorators, Python @macros, audits, unit tests, external_models.yaml, or seeds; running `sqlmesh plan/apply/audit/render/evaluate/test`; debugging plans, snapshots, virtual environments, or state issues; configuring `config.py`, gateways, or `before_all`/`after_all` hooks; choosing a model kind (FULL, INCREMENTAL_BY_TIME_RANGE, INCREMENTAL_BY_UNIQUE_KEY, VIEW, SEED, EMBEDDED, SCD_TYPE_2, EXTERNAL, MANAGED); migrating a project from dbt; or asking whether SQLMesh is still maintained. Trigger on these terms even when the user does not name the tool. SQLMesh changes quickly and the model's prior knowledge is often wrong — fetch the canonical docs at https://sqlmesh.readthedocs.io/en/stable/ before answering anything non-trivial.
allowed-tools: Read, Grep, Glob, LS, WebFetch, WebSearch
---

# SQLMesh

Open-source data-transformation framework. Apache 2.0 licence; governed by
the Linux Foundation since 2026-03-25 (see [LF announcement](https://www.linuxfoundation.org/press/linux-foundation-welcomes-sqlmesh-project)).
Active development continues. Verify the installed version with `pip show
sqlmesh` and check [GitHub releases](https://github.com/SQLMesh/sqlmesh/releases)
for the changelog.

This skill is **OSS-only**. Anything labelled "Tobiko Cloud" is out of scope.
Examples assume **DuckDB** as the warehouse engine; other engines work the
same way at the model level — the [engine integration index](https://sqlmesh.readthedocs.io/en/stable/integrations/engines/)
covers the small differences.

## When to use this skill

Activate as soon as any of these come up:

- A `MODEL (...)` block, `@model` decorator, `@macro`, `audit`, or
  `external_models.yaml` is being read or edited.
- The user runs (or asks about) `sqlmesh plan`, `apply`, `run`, `audit`,
  `render`, `evaluate`, `test`, `table_diff`, `lineage`, or `migrate`.
- Anything about virtual environments, snapshots, fingerprints, state, or
  promoting `dev` → `prod`.
- Choosing or switching a model kind.
- Configuring `config.py`, gateways, `before_all`/`after_all`, vars, signals.
- Migrating a dbt project.
- Doubt about whether the project is still alive.

## Always start here

SQLMesh ships fast. The model's training cutoff is usually behind a stable
release or two, and behaviour around plans, state, and incremental kinds has
changed several times. Before answering anything more than trivia:

1. WebFetch the relevant page from `https://sqlmesh.readthedocs.io/en/stable/`.
   The full URL map is in [`references/links.md`](references/links.md).
2. If the user reports an error, search [GitHub issues](https://github.com/SQLMesh/sqlmesh/issues)
   before guessing.
3. Trust the live docs over your prior knowledge when they conflict.

Skip the fetch only for stable basics already covered in this file.

## Mental model — this is not dbt

Five concepts carry most of SQLMesh's behaviour. Internalise them before
answering design questions.

**Snapshots and fingerprints.** Every model version is a snapshot identified
by a fingerprint over its rendered SQL plus its kind and properties. Two
models with the same fingerprint share the same physical table. Renaming a
column, changing a `WHERE` clause, or editing a macro a model uses changes
the fingerprint. See [snapshots](https://sqlmesh.readthedocs.io/en/stable/concepts/architecture/snapshots/).

**Virtual data environments.** `dev`, `prod`, and any feature-branch
environment are *views* over the snapshot store. Promoting `dev` to `prod`
swaps view targets — it does not rebuild data. See
[environments](https://sqlmesh.readthedocs.io/en/stable/concepts/environments/).

**Plans.** A plan is the diff between code and what is already materialised,
classified as **breaking** (downstream models must rebuild),
**non-breaking** (only the changed model rebuilds), or **forward-only** (no
backfill; new data only). The plan output lists each affected snapshot and
asks for confirmation before mutating state. See
[plans](https://sqlmesh.readthedocs.io/en/stable/concepts/plans/).

**State.** SQLMesh stores snapshot and environment metadata in a separate
state backend (default: a file alongside the warehouse, or a Postgres
database you point it at). State is the source of truth — losing it loses
all snapshot history. See [state](https://sqlmesh.readthedocs.io/en/stable/concepts/state/).
For production with DuckDB warehouses, **put state in Postgres** rather than
in the warehouse file (DuckDB is single-writer).

**Macros run at parse time.** Python `@macro` functions and built-ins like
`@EACH`, `@IF`, `@VAR` rewrite the SQL AST before plan time. They have
SQLGlot semantics — they manipulate columns and tables, not text. Jinja
macros also exist but are pure string substitution and have several known
footguns (see [`references/gotchas.md`](references/gotchas.md)).

## Canonical workflow

```bash
sqlmesh info                       # sanity-check config and connections
sqlmesh plan dev --auto-apply      # diff, build, materialise into dev env
sqlmesh audit dev                  # run audits against the dev env
sqlmesh test                       # run YAML unit tests
sqlmesh plan                       # promote dev → prod (no env arg = prod)
```

`plan dev` shows a categorised summary of changes (added / modified /
removed; breaking / non-breaking / forward-only) and a backfill window. With
`--auto-apply` the user accepts the plan inline; without it they review
interactively.

`plan` (no environment) targets `prod`. By default it is **virtual-only** —
it re-points prod views at snapshots already built in `dev`, so promotion
is fast and reversible. New physical builds happen only if `prod` sees a
snapshot it has never materialised.

To re-materialise data without a code change, use
`sqlmesh plan dev --restate-model <name>`. See
[plans](https://sqlmesh.readthedocs.io/en/stable/concepts/plans/).

## Model authoring

A model is a `MODEL (...)` block followed by exactly one query. Minimal
DuckDB example:

```sql
MODEL (
  name analytics.daily_orders,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order_date
  ),
  cron '@daily',
  grain (order_date, order_id),
  audits (
    not_null(columns := (order_id, order_date)),
    unique_values(columns := (order_id))
  )
);

SELECT
  order_id,
  order_date,
  customer_id,
  amount
FROM raw.orders
WHERE order_date BETWEEN @start_date AND @end_date;
```

`@start_date` and `@end_date` are macro variables SQLMesh injects per
interval for incremental kinds. For `TIMESTAMP` time columns, use the
`@start_ts` / `@end_ts` pair instead. See
[macro variables](https://sqlmesh.readthedocs.io/en/stable/concepts/macros/macro_variables/).

**Properties worth knowing**: `name`, `kind`, `cron`, `owner`, `grain`,
`audits`, `columns`, `tags`, `depends_on`, `description`. Full reference:
[model_configuration](https://sqlmesh.readthedocs.io/en/stable/reference/model_configuration/).

**Picking a kind.** Quick guide:

- `FULL` — small lookup tables; rebuilt every run.
- `INCREMENTAL_BY_TIME_RANGE` — append-mostly fact tables with a clear time
  column.
- `INCREMENTAL_BY_UNIQUE_KEY` — upserts keyed by a unique column (CDC-shaped).
- `VIEW` — pure SQL view, no materialisation.
- `SEED` — static CSV under `seeds/`.
- `SCD_TYPE_2` — slowly-changing dimensions with valid-from / valid-to.
- `EXTERNAL` — data SQLMesh reads but does not own (declared in
  `external_models.yaml`).
- `EMBEDDED` — inlined into downstream models, not materialised.
- `MANAGED` — engine-managed (e.g. dynamic tables).
- `CUSTOM` — user-defined materialisation strategy.

Full matrix with required props and DuckDB examples:
[`references/model-kinds.md`](references/model-kinds.md). Canonical doc:
[model kinds](https://sqlmesh.readthedocs.io/en/stable/concepts/models/model_kinds/).

**Python models.** Use the `@model` decorator and return a `pandas`,
`pyarrow`, or `ibis` table from `execute(context, ...)`. The
`ExecutionContext` exposes `fetchdf()`, `resolve_table()`, `var()`, and
`engine_adapter`. See [python_models](https://sqlmesh.readthedocs.io/en/stable/concepts/models/python_models/).

```python
from sqlmesh import ExecutionContext, model
import pandas as pd

@model(
    "analytics.summary",
    columns={"day": "DATE", "n": "BIGINT"},
    kind="FULL",
)
def execute(context: ExecutionContext, **kwargs) -> pd.DataFrame:
    upstream = context.resolve_table("analytics.daily_orders")
    return context.fetchdf(
        f"SELECT order_date AS day, COUNT(*) AS n FROM {upstream} GROUP BY 1"
    )
```

## Macros

Prefer **Python `@macro`** for anything beyond trivial substitution. They
are parsed against SQLGlot, so they manipulate real SQL nodes and the plan
can reason about lineage.

```python
# macros/_my_macros.py
from sqlmesh import macro

@macro()
def add_audit_columns(evaluator, table_alias):
    return [f"{table_alias}.created_at", f"{table_alias}.updated_at"]
```

Used as `@add_audit_columns('o')` inside a model query.

Built-ins worth knowing:

- `@EACH(items, x -> expr)` — generates a list of expressions.
- `@IF(cond, then, else)` — conditional SQL.
- `@VAR('name', default)` — read a config var.
- `@SQL(template)` — paste raw SQL safely.
- Predicate macros (`@AND`, `@OR`) and blueprinting for templated model
  families.

Reference: [SQLMesh macros](https://sqlmesh.readthedocs.io/en/stable/concepts/macros/sqlmesh_macros/).

**Avoid jinja macros.** They predate the Python macro system and exist
mostly for dbt-import compatibility. They are pure string substitution: no
SQL awareness, brittle whitespace, no lineage. The only good reason to
write one is if you're inside a dbt-imported project and need to keep
parity.

## Audits and tests

**Audits** are queries that must return zero rows. Attach inline:

```sql
MODEL (
  name analytics.daily_orders,
  audits (
    not_null(columns := (order_id, order_date)),
    unique_values(columns := (order_id)),
    accepted_range(column := amount, min_v := 0)
  )
);
```

40+ built-ins ship with SQLMesh: `not_null`, `unique_values`,
`accepted_values`, `accepted_range`, `valid_email`, `valid_url`,
`forall`, `mutually_exclusive_ranges`, etc. Full list:
[audits](https://sqlmesh.readthedocs.io/en/stable/concepts/audits/).

Custom audits live in `audits/<name>.sql`:

```sql
AUDIT (
  name only_recent_orders,
  defaults (max_age_days = 30)
);
SELECT * FROM @this_model
WHERE order_date < CURRENT_DATE - INTERVAL @max_age_days DAY;
```

Audits are **blocking by default** — a failed audit aborts the run. Pass
`blocking := false` to demote to a warning.

**Unit tests** live in `tests/<model>.yaml`. They run a model against
fixed input rows and assert on output rows. Run with `sqlmesh test`. See
[tests](https://sqlmesh.readthedocs.io/en/stable/concepts/tests/).

## Configuration (DuckDB defaults)

Minimal `config.py` for a DuckDB project:

```python
from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)

config = Config(
    gateways={
        "duckdb": GatewayConfig(
            connection=DuckDBConnectionConfig(database="warehouse.db"),
        ),
    },
    default_gateway="duckdb",
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)
```

`before_all` and `after_all` accept lists of SQL strings or Python `@macro`
calls — useful for `CREATE TYPE` enums, attaching extensions, or seeding
session settings:

```python
config = Config(
    ...,
    before_all=[
        "INSTALL httpfs; LOAD httpfs;",
        "@create_enums()",
    ],
)
```

Full reference: [configuration](https://sqlmesh.readthedocs.io/en/stable/reference/configuration/).

**State backend.** By default state lives in the same DuckDB file as the
warehouse, which is fine for local dev but breaks under concurrent writes.
For shared / CI / production setups, point state at Postgres:

```python
from sqlmesh.core.config import PostgresConnectionConfig

GatewayConfig(
    connection=DuckDBConnectionConfig(database="warehouse.db"),
    state_connection=PostgresConnectionConfig(
        host="...", user="...", password="...", database="sqlmesh_state"
    ),
)
```

See [state](https://sqlmesh.readthedocs.io/en/stable/concepts/state/) and
[`references/gotchas.md`](references/gotchas.md) for the failure modes.

## CLI cheatsheet

| Command | Purpose |
|---------|---------|
| `sqlmesh info` | Show config, gateways, connection health |
| `sqlmesh plan [env]` | Diff code vs state; build snapshots; promote |
| `sqlmesh run [env]` | Run scheduled cron-ready models |
| `sqlmesh render <model>` | Print the rendered SQL for a model |
| `sqlmesh evaluate <model>` | Run a model query and print rows |
| `sqlmesh fetchdf '<sql>'` | Run ad-hoc SQL through the engine |
| `sqlmesh audit [env]` | Run audits |
| `sqlmesh test` | Run unit tests |
| `sqlmesh table_diff <a> <b>` | Row and schema diff between two tables |
| `sqlmesh lineage <model>` | Print lineage |
| `sqlmesh dag` | Emit the model DAG |
| `sqlmesh environments` | List envs |
| `sqlmesh invalidate <env>` | Invalidate an environment |
| `sqlmesh migrate` | Migrate state schema (after upgrade) |

Full set with flags: [`references/cli.md`](references/cli.md) and the
[CLI reference](https://sqlmesh.readthedocs.io/en/stable/reference/cli/).

## Common gotchas

Short list — full annotations with citations in
[`references/gotchas.md`](references/gotchas.md).

- **State in the warehouse breaks under concurrency.** Local dev is fine;
  CI and prod want Postgres.
- **dbt incremental patterns do not port one-to-one.** SQLMesh decides what
  to backfill from the time column, not from a `WHERE` clause the user
  writes; see [`references/migration-from-dbt.md`](references/migration-from-dbt.md).
- **Forward-only plans skip backfill.** Don't use them when historical data
  needs the new logic applied.
- **Custom audits.** Reference models by their bare name —
  environment substitution happens automatically.
- **No automatic state cleanup on model removal.** Removing a model leaves
  its snapshot rows in state; clear with `sqlmesh janitor` or by
  re-creating state.
- **Browser UI is deprecated.** The [VS Code extension](https://github.com/SQLMesh/sqlmesh/tree/main/vscode/extension)
  is the current IDE surface. The legacy `sqlmesh ui` command still
  launches but is no longer recommended.

## Anti-patterns — do not generate

- Jinja macros for new code. Use Python `@macro`.
- State backend pointed at the same warehouse file in any shared setting.
- Direct ports of dbt incremental models without rewriting for SQLMesh's
  time-range model.
- `JINJA_QUERY_BEGIN` blocks in a SQLMesh-native project — that marker is
  a dbt-import compatibility shim, not idiomatic SQLMesh.
- Tobiko Cloud features (categorisation UI, managed scheduler, cost
  tracking). Out of scope for this OSS skill.

## Deeper reading

- Full documentation URL map: [`references/links.md`](references/links.md)
- Per-kind details with examples: [`references/model-kinds.md`](references/model-kinds.md)
- Full CLI surface: [`references/cli.md`](references/cli.md)
- Sharp-edges catalogue: [`references/gotchas.md`](references/gotchas.md)
- dbt → SQLMesh translation guide: [`references/migration-from-dbt.md`](references/migration-from-dbt.md)
