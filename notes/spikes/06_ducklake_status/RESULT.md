# Spike 6 — DuckLake + dbt-duckdb adapter status

**Verdict:** Use SQLMesh as the primary publish driver in Phase 4. dbt-duckdb has DuckLake support, but it's docs-grade, not feature-rich (no partitioning, no SORTED BY). The DuckDB-native ATTACH path works cleanly with R2-shaped storage.

## DuckLake fundamentals

- **Spec status**: DuckLake v1.0 published 2026-04-13. Reference implementation = `ducklake` extension shipped with DuckDB 1.5.2 (current pin in this repo). v1.1 expected Sept 2026.
- **Native ATTACH path**: confirmed working in this repo's `spikes-sqlmesh` env (DuckDB 1.5.2). See `proof.py`.

```
INFO duckdb version: 1.5.2
INFO rows in demo: 2
INFO storage contents: ['demo', 'main']
INFO post-second-write rowcount: 3
INFO re-attach rowcount: 3
```

The proof script ATTACHes a local DuckLake catalog with file-system DATA_PATH, writes, re-attaches in a fresh connection, and reads back. Round-trip clean.

For R2 the recipe matches the S3 pattern (R2 is S3-compatible):

```sql
CREATE PERSISTENT SECRET r2_creds (
  TYPE s3,
  KEY_ID '<r2 access key>',
  SECRET '<r2 secret>',
  ENDPOINT '<account>.r2.cloudflarestorage.com',
  URL_STYLE 'path'
);

ATTACH 'ducklake:metadata.ducklake' AS bc (DATA_PATH 's3://<bucket>/bc/');
```

(Equivalent to the doc's S3 pattern with R2 endpoint substituted; not exercised in this spike — that belongs in Phase 4 with real creds.)

## dbt-duckdb DuckLake support — release archaeology

Authoritative timeline from `gh release view` against `duckdb/dbt-duckdb`:

| Version | Date | DuckLake change |
|---|---|---|
| 1.9.4 | 2025-06-25 | PR #557: macros for dropping schema/relations on DuckLake |
| 1.9.5 | 2025-09-08 | PR #620: `type: ducklake` profile for managed DuckLake. PR #625: `is_ducklake: true` flag for self-hosted DuckLake in primary database |
| 1.9.6 | 2025-09-08 | (no DuckLake-specific changes) |
| 1.10.0 | 2025-11-05 | PR #632: "DuckLake connection instructions" — docs only, +20/-0 |
| 1.10.1 | 2026-02-17 | (no DuckLake-specific changes — bugfix release) |

Open issues at spike time:

- #581 partition_by for ducklake tables (open since Jun 2025)
- #617 [ducklake] postfix for tables (open since Nov 2025)
- #724 Ducklake SORTED BY support (open Apr 2026)

Translation: DuckLake works in dbt-duckdb 1.10.1 for plain CREATE/INSERT/REPLACE flows, but the table-tuning surface (partitioning, sort keys, postfix) is missing. For an R2-published consumer DB where we want partition pruning by `season` or `game_type`, this is a real gap.

## SQLMesh DuckLake support — current state

- DuckLake first-class in SQLMesh's DuckDB adapter via the `catalogs.<name>.type: ducklake` config (per the Tobiko tutorial). Tutorial profile shape:

```yaml
gateways:
  local_gateway:
    connection:
      type: duckdb
      catalogs:
        my_lakehouse:
          type: ducklake
          path: data/catalog.ducklake
          data_path: data/storage/
      extensions:
        - ducklake
    state_connection:
      type: duckdb
      database: data/sqlmesh_state.db
default_gateway: local_gateway
```

- Known constraint: SQLMesh state cannot live in a DuckLake table because DuckLake doesn't yet support UPDATE — state goes in a separate DuckDB file (the `state_connection` block above).
- Tobiko's tutorial is the de-facto canonical path. Marketed as a tutorial but the codepaths are exercised in their CI per the repo (see `tobikodata/sqlmesh` config samples).

## Phase 4 recommendation

**Use SQLMesh exclusively for the DuckLake publish step.** Two reasons:

1. SQLMesh's `type: ducklake` catalog config is the canonical, tested path for DuckLake-managed writes. dbt-duckdb works but is feature-thin and on a slow update cadence (bugfix-only since the Sep 2025 main work).
2. By Phase 4 we'll already be on SQLMesh per Phase 1. Adding a parallel dbt-duckdb publish lane is incremental burden with no upside — both engines write the same parquet files; the catalog format is the contract.

If we hit a SQLMesh-specific gap during Phase 4 (e.g., partition_by support landed in dbt-duckdb but not SQLMesh), we revisit then. dbt-duckdb stays as a known fallback.

## Verdict

**GO** with SQLMesh-driven publish in Phase 4. No custom hooks needed for the basic case; the Tobiko tutorial config is reproducible. Open follow-ups for Phase 4 actual work:

- R2 SECRET creation needs to be in a pre-publish hook (DuckLake doesn't auto-create the secret).
- Partition strategy decision deferred (DuckLake 1.0 lacks declarative partition_by; Phase 4 builds may need to predicate-partition tables manually for read perf).
