# Migrating from dbt to SQLMesh

Two paths exist:

1. **dbt-import compatibility mode** (`sqlmesh init -t dbt`). SQLMesh
   reads `dbt_project.yml`, runs jinja, and treats dbt models as
   first-class. Good for an incremental migration; less idiomatic.
2. **SQLMesh-native rewrite.** Models become `MODEL (...)` blocks; macros
   become Python `@macro`s; sources become `external_models.yaml`. The
   target end state.

This guide covers the rewrite. For compat mode, see the
[dbt integration docs](https://sqlmesh.readthedocs.io/en/stable/integrations/dbt/).

## Concept-by-concept translation

| dbt | SQLMesh |
|-----|---------|
| `dbt run` | `sqlmesh plan dev --auto-apply` then `sqlmesh plan` to promote |
| `dbt build` | `sqlmesh plan` (audits and tests are part of the plan) |
| `dbt test` | `sqlmesh test` (unit tests) and `sqlmesh audit` (data tests) |
| `models/foo.sql` with `{{ config(...) }}` | A `MODEL (...)` block at the top of the file |
| `{{ ref('upstream') }}` | `schema.upstream` (bare two-part name) |
| `{{ source('s', 't') }}` | A `schema.t` reference, with `s.t` declared in `external_models.yaml` |
| `{{ var('x') }}` | `@VAR('x')` |
| Jinja macro under `macros/` | Python `@macro` under `macros/_*.py` |
| `dbt_project.yml` | `config.py` (Python; preferred) or `config.yaml` |
| `profiles.yml` | The `gateways:` block in `config.py` |
| `seeds/*.csv` | Same path; declared as `MODEL (... kind SEED ...)` or autoloaded |
| `snapshots/` | `MODEL (... kind SCD_TYPE_2 ...)` |
| `dbt deps` | No equivalent — SQLMesh has no package manager. Copy the macros you actually use. |

## Incremental models — the biggest pitfall

dbt's recommended pattern looks like:

```sql
{{ config(materialized='incremental', unique_key='id') }}

SELECT *
FROM {{ source('raw', 'events') }}
{% if is_incremental() %}
WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
```

Do **not** translate that literally. SQLMesh tracks materialised intervals
in state — it knows which time windows are missing without consulting the
target table. Rewrite as:

```sql
MODEL (
  name analytics.events_daily,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_time
  ),
  cron '@daily',
  grain (event_id)
);

SELECT
  event_id,
  event_time,
  user_id,
  payload
FROM raw.events
WHERE event_time BETWEEN @start_ts AND @end_ts;
```

The `WHERE` filter uses SQLMesh-injected interval bounds, not a
self-referential watermark. Backfills, gap-filling, and late-arriving data
are all driven by state; you don't have to express any of it in SQL.

For upsert-by-key models (no clean time column), use
`INCREMENTAL_BY_UNIQUE_KEY` and merge:

```sql
MODEL (
  name analytics.users,
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key user_id),
  cron '@hourly'
);

SELECT user_id, email, updated_at
FROM raw.user_changes
WHERE updated_at > @start_ts;
```

## Sources → external models

dbt's `sources.yml`:

```yaml
sources:
  - name: raw
    tables:
      - name: events
        columns:
          - name: event_id
            data_type: bigint
```

becomes SQLMesh's `external_models.yaml`:

```yaml
- name: raw.events
  columns:
    event_id: BIGINT
    event_time: TIMESTAMP
    user_id: BIGINT
```

Bootstrap from the engine's information schema:

```bash
sqlmesh create_external_models
```

In model files, refer to the source as `raw.events` directly — no
`{{ source() }}` wrapper.

## Macros

A dbt jinja macro:

```jinja
{% macro stars_to_dim_columns(table_alias) %}
  {{ table_alias }}.created_at,
  {{ table_alias }}.updated_at
{% endmacro %}
```

becomes a Python macro:

```python
# macros/_dim_columns.py
from sqlmesh import macro

@macro()
def dim_columns(evaluator, table_alias):
    return [f"{table_alias}.created_at", f"{table_alias}.updated_at"]
```

Used as `@dim_columns('o')` in SQL.

For loops use the built-in `@EACH`:

```sql
SELECT
  user_id,
  @EACH(['gold', 'silver', 'bronze'], tier ->
    SUM(CASE WHEN segment = tier THEN amount END) AS @CONCAT(tier, '_total')
  )
FROM raw.purchases
GROUP BY user_id;
```

## Tests

dbt's generic tests (`unique`, `not_null`, `accepted_values`) become
**audits**, attached inline to the model:

```sql
MODEL (
  name analytics.users,
  audits (
    not_null(columns := (user_id, email)),
    unique_values(columns := (user_id))
  )
);
```

Run with `sqlmesh audit`. Full audit list:
https://sqlmesh.readthedocs.io/en/stable/concepts/audits/.

dbt's singular SQL tests become custom audits in `audits/<name>.sql`.
dbt's `dbt-unit-testing` package becomes SQLMesh's first-class
`sqlmesh test` with YAML fixtures.

## Workflow translation

Where dbt has one pipeline (`dbt build`), SQLMesh has two flows worth
keeping straight:

1. **Build & validate in dev**: `sqlmesh plan dev --auto-apply`,
   `sqlmesh audit dev`, `sqlmesh test`.
2. **Promote to prod**: `sqlmesh plan` — virtual swap, no rebuild needed
   if dev already built the snapshots.

That two-step is the payoff of SQLMesh's snapshot model: you build once,
promote zero-cost, and roll back by re-pointing prod views.

## What not to bring over

- `dbt deps` and dbt packages. There's no SQLMesh equivalent. Copy the
  small bits of `dbt_utils` etc. you actually use into Python `@macro`s.
- `target/manifest.json` consumers. SQLMesh's equivalent is the state
  backend and `sqlmesh dag` / `sqlmesh lineage` output.
- Run-time hooks (`on-run-start`, `on-run-end`). Use `before_all` /
  `after_all` in `config.py`.
- `dbt docs serve`. Use the [VS Code extension](https://github.com/SQLMesh/sqlmesh/tree/main/vscode/extension)
  for lineage and column-level docs.

## Reference reading

- [SQLMesh dbt integration docs](https://sqlmesh.readthedocs.io/en/stable/integrations/dbt/)
- [Harness: Transitioning from dbt to SQLMesh](https://www.harness.io/blog/from-dbt-to-sqlmesh) (practical playbook)
- [Breaking and Non-Breaking Changes](https://davidsj.substack.com/p/breaking-and-non-breaking-changes) (mental model for the virtual layer)
