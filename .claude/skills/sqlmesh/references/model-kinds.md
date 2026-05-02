# Model kinds reference

Canonical doc: https://sqlmesh.readthedocs.io/en/stable/concepts/models/model_kinds/

A model's kind controls how it is materialized, how SQLMesh decides what to
backfill, and which properties are required. Pick the simplest kind that
matches the data shape — over-specifying a kind is a common source of
unnecessary backfills.

## Decision shortcut

| Question | Pick |
|----------|------|
| Small reference / lookup table, cheap to rebuild | `FULL` |
| Append-mostly fact table with a clear time column | `INCREMENTAL_BY_TIME_RANGE` |
| Upsert by primary key (CDC-shaped source) | `INCREMENTAL_BY_UNIQUE_KEY` |
| Just a `CREATE VIEW` | `VIEW` |
| Static CSV checked into the repo | `SEED` |
| Type-2 SCD with valid_from / valid_to | `SCD_TYPE_2` |
| Read-only declaration of an upstream table | `EXTERNAL` |
| Inline this model into its consumers; do not materialize | `EMBEDDED` |
| Engine has its own materialization primitive (Snowflake dynamic table, etc.) | `MANAGED` |
| None of the above fits — write your own materialization | `CUSTOM` |

## FULL

Rebuilt from scratch every run. No backfill machinery; every run is a full
refresh.

```sql
MODEL (
  name analytics.team_seasons,
  kind FULL,
  cron '@daily',
  grain (team_id, season)
);

SELECT team_id, season, wins, losses
FROM raw.standings;
```

Use for small dimensional tables, lookup tables, or anywhere recompute is
trivial. Avoid for tables larger than a few million rows on cron-driven
schedules — every run pays the full cost.

## INCREMENTAL_BY_TIME_RANGE

The default for fact tables. SQLMesh tracks materialized time intervals per
snapshot and only re-runs gaps. The query must filter by `@start_date` and
`@end_date` (or their `*_ds` / `*_ts` siblings).

```sql
MODEL (
  name analytics.events_daily,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_time,
    batch_size 30
  ),
  cron '@daily',
  grain (event_id),
  audits (not_null(columns := (event_time, event_id)))
);

SELECT
  event_id,
  event_time,
  user_id,
  payload
FROM raw.events
WHERE event_time BETWEEN @start_ts AND @end_ts;
```

Required: `time_column`. Useful: `batch_size` (limit interval per backfill
batch), `lookback` (re-process the trailing N intervals each run for late
data), `forward_only` (don't backfill on code change).

## INCREMENTAL_BY_UNIQUE_KEY

Upsert by one or more keys. Each run merges new rows into the existing
table on the unique key.

```sql
MODEL (
  name analytics.users,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key user_id
  ),
  cron '@hourly',
  grain (user_id)
);

SELECT user_id, email, updated_at
FROM raw.user_changes
WHERE updated_at > @start_ts;
```

Use for CDC-shaped or slowly-mutating dimension data. The `WHERE` clause
typically filters by an updated-at column to keep batches small, but
SQLMesh's correctness guarantees come from the merge — not the filter.

## VIEW

Materialised as `CREATE VIEW`. Cheap to update; nothing is stored.

```sql
MODEL (
  name analytics.active_users,
  kind VIEW
);

SELECT * FROM analytics.users WHERE deleted_at IS NULL;
```

Use for small projections / filters on top of materialized tables. A view
re-runs every time downstream queries hit it, so don't put expensive joins
behind one.

## SEED

Static CSV. The path is relative to the model file by default.

```sql
MODEL (
  name analytics.country_codes,
  kind SEED (
    path 'seeds/country_codes.csv'
  )
);
```

CSV header is mandatory. Reload with
`sqlmesh plan --restate-model analytics.country_codes`.

## SCD_TYPE_2

Slowly-changing dimension. SQLMesh manages `valid_from` / `valid_to` rows
automatically.

```sql
MODEL (
  name analytics.customer_history,
  kind SCD_TYPE_2 (
    unique_key customer_id,
    updated_at_name updated_at
  )
);

SELECT customer_id, name, region, updated_at
FROM raw.customers;
```

Required: `unique_key`. Required for the `*_BY_TIME` flavour:
`updated_at_name`. SQLMesh closes the previous row's `valid_to` and inserts
a new row whenever any non-key column changes.

## EXTERNAL

Declares a table SQLMesh does **not** own — typically registered in
`external_models.yaml` rather than in a SQL file.

```yaml
# external_models.yaml
- name: raw.events
  columns:
    event_id: BIGINT
    event_time: TIMESTAMP
    user_id: BIGINT
    payload: JSON
```

External models give SQLMesh column types and lineage without trying to
build the table. Use for source tables loaded by another system, parquet
sources, or registered DuckDB attachments.

Discover automatically with
`sqlmesh create_external_models` (queries the engine's information schema).

## EMBEDDED

The model's query is inlined into every consumer at parse time; never
materialized. Use for filters or projections that several downstream
models share verbatim and you don't want to pay to store.

```sql
MODEL (
  name shared.active_users,
  kind EMBEDDED
);

SELECT * FROM analytics.users WHERE deleted_at IS NULL;
```

Downstream models reference `shared.active_users` like any other model;
SQLMesh splices its query inline at compile time. Behaves like a CTE —
not like a parameterised macro.

## MANAGED

The engine owns the materialization (dynamic tables on Snowflake, materialized
views with auto-refresh, etc.). SQLMesh records the snapshot but does not
schedule the refresh.

```sql
MODEL (
  name analytics.realtime_summary,
  kind MANAGED
);

SELECT user_id, COUNT(*) AS events
FROM raw.events
GROUP BY 1;
```

Engine support is uneven — check
[managed models](https://sqlmesh.readthedocs.io/en/stable/concepts/models/managed_models/)
for the per-engine matrix.

## CUSTOM

Plug-in materialization. Subclass `CustomMaterialization` in Python and
register it in `config.py`. Use for unusual write patterns (e.g. writing
parquet partitions, calling an API).

See the [custom materialization](https://sqlmesh.readthedocs.io/en/stable/concepts/models/model_kinds/#custom)
section of the kinds page for the latest API.
