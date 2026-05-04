# Phase 4 follow-ups

Phase 4 shipped the DuckLake publish side as a parallel artifact alongside
`scripts/create_web_db.py`. The original parquet+`bc_remote.db` flow
continues to publish on every build; the DuckLake artifact is for site-team
validation. Local end-to-end validation completed 2026-05-03 against the
`dev` schema (see "4.0 Local validation receipt" below). Validation against
a real `main_models` build is blocked on the items in "Prod build blockers"
below; site cutover (4.x) stays deferred behind that.

## 4.0 Local validation receipt (2026-05-03)

Validated by running the publish path against `bc.db` and comparing all
rows table-for-table back to the source. The canonical
`scripts/publish_ducklake.py` only publishes from `main_models` +
`main_seeds`; that build hasn't been promoted to prod yet (see "Prod
build blockers"), so the receipt below was produced from a one-shot
ad-hoc script that publishes from `main_models__dev` + `main_seeds`
into a separate `bc_publish_dev.ducklake` catalog. The script otherwise
mirrors `publish_ducklake.py` (same ENUM→VARCHAR cast, same compression
+ row group + inlining knobs, same snapshot-retention logic). When the
real `main_models` is built, re-running `scripts/publish_ducklake.py
--reset` should reproduce the same shape.

Numbers from the dev validation:

- 163 objects published (141 from `main_models__dev`, 22 from `main_seeds`).
- 736,919,764 rows total, 75.8s wall.
- 24 ENUM types in `bc.db`; cast to VARCHAR on publish per the existing
  `select_with_enum_casts` path.
- Catalog 4 KB, data path 6.7 GB on disk (zstd, 1.97M-row groups,
  inlining disabled).
- Snapshot retention: 161 transient snapshots from per-table commits
  expired down to the last 5 via `ducklake_expire_snapshots`.
- Smoke check on 5 sample tables passes; consumer-side `DESCRIBE` returns
  expected column counts and types.

Parity vs `bc.db`:

- 0 / 163 row-count mismatches.
- Aggregate parity (COUNT, MIN, MAX, SUM-as-HUGEINT, COUNT(DISTINCT
  text_col)) on 4 representative tables — `event_states_full`
  (18.1M rows, ENUM-heavy), `team_game_results` (478K rows, Phase 5
  Polars FSM port), `event_pitching_flags` (18.1M rows, Phase 5 first
  wave), `seed_franchises` (365 rows, ENUM-free) — exact match between
  `bc.<src_schema>.<table>` and `bc_pub_dev.<dst_schema>.<table>`.

The R2 upload step (`scripts/upload_ducklake.py`) is unchanged and
unrun; it will be exercised when the real `main_models` build is
available and the catalog is ready to push.

## Prod build blockers (uncovered 2026-05-03)

A fresh `sqlmesh plan --auto-apply` (no env = prod) against the current
`bc.db` does not run cleanly. Three real bugs surface during backfill;
all need fixes before `main_models` exists for `publish_ducklake.py` to
read from. Tracked here so the followups doc holds the short list.

1. **`relationships` audit macro polarity (FIXED)** — `bc/macros/_env_to_model.py`
   short-circuited with `exp.true()` when the FK target view didn't
   exist, but the audit body returns rows the WHERE matches as
   violations, so TRUE flagged every non-null source row as failing.
   Switched to `exp.false()` and renamed `_table_exists` → `_table_populated`
   so the short-circuit also fires when the target exists but is still
   empty (fresh prod plan: staging audit may run before referent
   backfills, since `to_model` deps aren't in the DAG to avoid cycles).
   Once the target is populated on a subsequent plan, the real check
   runs. No standalone unit test — add one when the SQLMesh
   `MacroEvaluator` fixture story for custom macros is figured out.

2. **`team_game_start_info` MAP cast** — fresh prod backfill fails with
   `Conversion Error: Unimplemented type for cast (STRUCT("key" INTEGER,
   "value" VARCHAR)[] -> MAP(UTINYINT, VARCHAR))` on column
   `lineup_map_away`. The Phase 5 wave-2 Polars FSM port emits a
   `STRUCT[]` shape that DuckDB can't auto-cast to the model contract's
   `MAP(UTINYINT, TEXT)`. Fix: emit the column as a real DuckDB MAP
   from Polars (or relax the contract to STRUCT[] if the consumer can
   handle either).

3. **Six third-wave ML predictions models reference missing artifact
   JSONs** — `predictions_batted_trajectory_cat`,
   `predictions_batted_location_cat`, `predictions_baserunning_cat`,
   `predictions_runs_following_num`, `predictions_is_win_bin`,
   `predictions_has_batting_bin` each fail in `load_scorer` with
   `FileNotFoundError: bc/python_models/ml/artifacts/<name>.json`.
   Already known per `CLAUDE.md` ("shipped code + tests but need a real
   `scripts/train_<name>.py` run"). Either run training for each (long)
   or set those `@model`s to `enabled FALSE` until the artifacts land.

4. **`stg_box_score_pinch_hitting_lines` not_null violations** — 2255
   rows fail `not_null((game_id, pinch_hitter_id, inning))`. Either
   real source-data gaps that need a `WHERE` filter at the staging
   level or an audit `condition` exclusion. Investigate before
   declaring the audit non-blocking.

## 4.x Site cutover (deferred)

Tracked here so the followup is visible. Trigger: the site team confirms
on a test branch that DuckLake is a drop-in replacement for `bc_remote.db`
for site queries. Specifically:

- **Query parity** — for a representative sample of site queries, rows and
  values are identical between
  `ATTACH 'https://data.baseball.computer/baseball/v1/baseball.ducklake' (TYPE ducklake, READ_ONLY)`
  and the existing `ATTACH 'https://.../dbt/bc_remote.db'`. The
  Verification step from the Phase 4 plan §6 is a starting checklist.
- **Cold-attach latency** — single catalog fetch + lazy parquet reads is
  acceptable vs the current single-DB-file fetch. Worth measuring on the
  site's actual edge.
- **VARCHAR-not-ENUM acceptable** — site code that filters / joins on ENUM
  columns (`event_type`, `park_id`, etc.) keeps working with VARCHAR
  semantics. DuckLake v1.0 stores ENUMs as VARCHAR regardless of the
  source type — the publish script does the cast explicitly so column
  metadata reflects reality.
- **LLM-metadata bridge** is either ready to consume the DuckLake table
  layout, or works against both artifacts.

When the cutover lands:

1. Delete `scripts/create_web_db.py`.
2. Stop publishing the `dbt/` R2 prefix (don't delete the prefix
   immediately — leave a grace window for any external consumer pinned to
   it).
3. Update `README.md`, `CLAUDE.md`, and the site's data-access docs to
   reference only the DuckLake URL.
4. After the grace window, purge the `dbt/` R2 prefix.

## 4.5 Incremental kinds — shelved

Decision (2026-05-03): not pursuing. The motivation was DuckLake
snapshot-retention storage savings, but we have no current need to
retain snapshots — both SQLMesh (`snapshot_ttl="in 1 hour"` + janitor)
and DuckLake (`expire_snapshots()` keeping the last 5) prune
aggressively, so a fixed working set already bounds cost. Adding
`INCREMENTAL_BY_TIME_RANGE` would add coordination complexity (interval
config, late-arriving data, partition replacement on the publish side)
without paying off until we want long history. Revisit only when we
actually want to retain N>>5 snapshots.

## Per-table compression / row-group settings

`scripts/create_web_db.py` writes `event_states_full` at
`COMPRESSION GZIP, ROW_GROUP_SIZE 262144` and everything else at
`ZSTD, ROW_GROUP_SIZE 1966080`. DuckLake exposes
`parquet_compression` / `parquet_row_group_size` only as catalog-wide
options (`ducklake_set_option`), not per-table. The publish script
currently sets them catalog-wide to ZSTD + 1966080, so
`event_states_full` doesn't get its tuned settings in the DuckLake
artifact. Workarounds to evaluate when DuckLake adds richer write
options:

- Per-table options at the DuckLake spec level.
- COPY-then-`ducklake_add_data_files` pattern (write parquet with the
  desired knobs, then register the file as a DuckLake data file
  manifest entry — usable but bypasses normal commits).

## Snapshot retention API

Resolved. `scripts/publish_ducklake.py:expire_snapshots()` queries
`ducklake_snapshots('bc_publish')` ordered by `snapshot_id DESC`,
keeps the first `KEEP_LAST_N_SNAPSHOTS` (= 5), and passes the rest
to `ducklake_expire_snapshots(versions => [...])`. Switched from the
30-day window because every snapshot is full-size while all models
are FULL kind — a fixed working set bounds R2 storage cost regardless
of dispatch cadence.

## ENUM degradation

DuckLake v1.0 doesn't preserve user-defined types — ENUM columns land as
VARCHAR in the published artifact. This is the official position
(DuckLake's own DuckDB→DuckLake migration script casts ENUMs to VARCHAR
in `TYPE_MAPPING`). When DuckLake adds richer type support, the publish
script's `select_with_enum_casts` step becomes a pass-through and we can
drop the `data_type IN <enum_set>` branch.

## Cloudflare cache-purge prerequisite

`scripts/upload_ducklake.py` requires `CLOUDFLARE_API_TOKEN` and
`CLOUDFLARE_ZONE_ID` env vars at upload time. Token needs Zone:Cache
Purge scope on the `data.baseball.computer` zone. Set up before the
publish flow goes onto every build.

## DATA_VERSION bumping

`bc/data_version.txt` controls the R2 prefix
(`baseball/v<DATA_VERSION>/`). Bump on schema-breaking changes (new
ENUM values are not breaking — they hit consumers as new VARCHAR
values; renamed/removed columns or tables are). Old prefixes stay
attachable until manually purged. No automation around this yet — bump
manually as part of the change that breaks the schema.

## R2 / Cloudflare upload concurrency

`upload_ducklake.py` uploads files sequentially through boto3. Fine
for the catalog file but the data dir is many parquet files. If
upload time becomes the bottleneck, switch to
`concurrent.futures.ThreadPoolExecutor` around `client.upload_file`.
Not urgent — `create_web_db.py` is sequential too.
