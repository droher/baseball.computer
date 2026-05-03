# Phase 4 follow-ups

Phase 4 shipped the DuckLake publish side as a parallel artifact alongside
`scripts/create_web_db.py`. The original parquet+`bc_remote.db` flow
continues to publish on every build; the DuckLake artifact is for site-team
validation. This file captures the work remaining before the DuckLake
artifact can become canonical and the old flow can retire.

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
