# SQLMesh sharp edges

Things that bite users — including ones with prior dbt experience —
because the underlying behavior differs from what the syntax suggests.

## State

**State in the warehouse breaks under concurrency.** SQLMesh's default
state backend is whatever gateway you configured for the warehouse. With
DuckDB, that's a single file with one writer at a time. Two simultaneous
`sqlmesh plan` runs (CI parallelism, two engineers, etc.) corrupt or
deadlock state.

Fix: configure `state_connection` separately, pointed at Postgres or
another OLTP store. See [state docs](https://sqlmesh.readthedocs.io/en/stable/concepts/state/).

**No automatic state cleanup on model removal.** Deleting a model file
leaves its snapshot rows in state. Run `sqlmesh janitor` to garbage-collect
or rebuild state from scratch with `sqlmesh state export` / `import`. Open
issue: https://github.com/SQLMesh/sqlmesh/issues/4464.

**State is the source of truth.** Losing state is unrecoverable — the
warehouse tables become orphaned, and rebuilding history requires a full
backfill. Back up state alongside the warehouse.

## Plan / apply mental model

**`plan` (no env) targets prod and is virtual-only by default.** It does
not rebuild data; it re-points prod views at snapshots already built in a
non-prod environment. If the snapshot has never been materialized
anywhere, prod build runs. Most surprises here are users expecting
"`plan` rebuilds prod" — it does not.

**Forward-only plans skip backfill.** Applying a `--forward-only` plan
does not re-process history. Use only when historical data should keep the
old logic and only new intervals get the new code.

**Breaking vs non-breaking is determined automatically.** SQLMesh diffs
the rendered SQL plus model properties to decide. A change you consider
"safe" (renamed CTE, comment added) may show as breaking if it materially
changes the SQL fingerprint. Use `sqlmesh render` to compare.

**Categorisation can be overridden.** If SQLMesh decides a change is
breaking but you know it's a no-op for downstream models, the prompt lets
you choose `non-breaking` interactively. Don't reflexively accept the
default — read the diff.

## dbt migration sharp edges

**dbt incremental patterns do not port cleanly.** dbt's recommended
pattern (filter by `{{ this }}` and a max-loaded-at watermark) is
incompatible with SQLMesh's interval tracking. SQLMesh decides what to
backfill from `time_column`, not from a `WHERE` clause the user writes.
Rewrite incremental models to filter by `@start_ts` / `@end_ts` and let
SQLMesh manage intervals. See
[`migration-from-dbt.md`](migration-from-dbt.md).

**`{{ ref() }}` becomes a bare model name.** SQLMesh resolves
`schema.model` references through its parser; no Jinja indirection needed.

**Sources become external models.** dbt `sources.yml` maps to
`external_models.yaml`. Use `sqlmesh create_external_models` to bootstrap.

## Macros

**Jinja macros are pure string substitution.** They have no SQL awareness,
no lineage, and break in subtle ways around whitespace and quoting. Use
Python `@macro` for new code. Background reading: [Tobiko on jinja
pitfalls](https://tobikodata.com/traps-and-pitfalls-of-using-sql-with-jinja.html).

**Python `@macro` runs at parse time.** It cannot read warehouse state.
For "compute X from a query, then template it in," use a [signal](https://sqlmesh.readthedocs.io/en/stable/guides/signals/)
or push the logic into the model's SQL itself.

**`@EACH` produces a list, not a string.** Forgetting that and trying to
concatenate it as text gives confusing errors. Wrap with `@SQL` if you
genuinely need a string.

## Audits

**Custom audits use bare model names.** Do not include the environment
prefix — SQLMesh substitutes the right physical name automatically. A
custom audit that hard-codes `dev.foo` or `prod.foo` will break in the
other environment. Tracking issue:
https://github.com/SQLMesh/sqlmesh/issues/3665.

**Audits are blocking by default.** A failed audit aborts the whole run,
including downstream models. Use `blocking := false` for warnings only;
use a unit test or a separate validation model when you want a check that
doesn't gate the pipeline.

## Engine-specific

**ClickHouse delete-then-insert is broken at scale.** SQLMesh's
incremental strategy uses delete-and-insert; ClickHouse async deletes do
not optimize on clusters and cause concurrency issues. Issue:
https://github.com/SQLMesh/sqlmesh/issues/3186. For ClickHouse-heavy
workloads, prefer `FULL` or move to DuckDB / Databricks.

**DuckDB single-writer.** A running `sqlmesh plan` holds an exclusive
lock on the DuckDB file. A second `sqlmesh` invocation will fail until
the first releases. Plan around this in CI; for parallel envs, give each
its own warehouse file.

## Tooling

**Browser UI (`sqlmesh ui`) is deprecated.** The current IDE surface is
the [VS Code extension](https://github.com/SQLMesh/sqlmesh/tree/main/vscode/extension).
The legacy UI still launches but is no longer the recommended tool.

**`sqlmesh migrate` is required after upgrades.** Most version bumps add
migrations to the state schema. Forgetting to run `migrate` produces
cryptic errors on the next `plan`.

## Performance

**Large projects (1000+ models) used to be slow.** Performance improved
substantially in the 0.118+ line and again post-LF donation. If you see
slow `info` / `plan` / parser invocations, upgrade first. See [Tobiko's
"Making SQLMesh Faster"](https://www.tobikodata.com/blog/making-sqlmesh-faster).

**Backfills on 100M+ row incremental models can be long.** Use
`batch_size` to bound interval batches; consider `forward_only` for
metadata-only changes.
