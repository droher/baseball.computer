# DuckLake production publication

The site at https://baseball.computer/query-engine runs DuckDB-WASM in the browser and attaches the public DuckLake 1.0 catalog at `https://data.baseball.computer/baseball/v1/baseball.ducklake`. `catalog.json` in the same prefix supplies the schema sidebar. Native DuckDB clients can use the same catalog without credentials.

## Publish

1. Build production through the SQLMesh recipes in `justfile` when model changes require it.
2. Run `uv run --group build python scripts/publish_ducklake.py`. The source `bc.db` is read-only. The publisher checks estimated-table population and source/export row-count parity, casts ENUMs to VARCHAR, and writes ZSTD Parquet with a 128 MB target file size. Use `--smoke-only` to read an existing export. `--reset` rebuilds the derived local export and cannot be combined with smoke-only.
3. Run `uv run --group build python scripts/upload_ducklake.py`. Supply the documented R2 and Cloudflare environment variables, or use `--wrangler` with an existing Wrangler OAuth login. Wrangler uploads preflight its 300 MiB file limit. Data uploads default to four at a time (`--workers 1..16`); metadata and catalog publish last. Use `--resume` after interruption: it skips immutable data files only after matching the public size and strong ETag to a local streaming MD5. Catalog and schema are always uploaded.
4. For a fresh version prefix, `--skip-purge` avoids requiring a separate cache-purge token. For previously cached catalogs, use the cache-purge credentials. Parquet objects are immutable; catalog and schema metadata use `max-age=0, must-revalidate`.
5. Rebuild/deploy the site after schema changes. Its `src/lib/io/data-source.ts` version must match `bc/data_version.txt`.

The GitHub workflow requires a configured self-hosted runner, R2 secrets, cache-purge secrets, and `BC_STATS_ARTIFACTS_ROOT`; local Wrangler publication is also supported. Never commit credentials or local data artifacts.

## Cloudflare metadata caching

The active `DuckLake metadata revalidation` cache rule bypasses edge caching and respects the origin browser TTL for `data.baseball.computer` paths under `/baseball/` ending in `.ducklake` or `/catalog.json`. It overrides the legacy month-long cache-everything page rule for metadata only. Keep this exception when changing Cloudflare configuration; otherwise even `max-age=0` can become a month-long browser cache lifetime. Public metadata responses must retain `max-age=0, must-revalidate`, CORS, and range support. Parquet keeps immutable caching.

## Verify

- Run publisher/upload tests in `bc/tests/test_publish_ducklake.py` and `bc/tests/test_upload_ducklake.py`.
- Query the public catalog with native DuckDB, reading actual table values rather than only catalog row counts.
- Verify public catalog and Parquet GET/HEAD responses allow the site's Origin and HTTP Range requests.
- In the site repository run `pnpm check`, `pnpm lint`, and `pnpm test`. After deployment run `PLAYWRIGHT_BASE_URL=https://baseball.computer pnpm test:integration` against production.
- Browser acceptance includes a read-only `ducklake` attachment, real Parquet requests, baseball joins/aggregation, CSV exports, and joins to locally uploaded files. The site must make no query requests to `/dbt/`.

## Retention and rollback

Retain existing `/dbt/` objects for external consumers and rollback. Restore the preceding Vercel deployment if the frontend cutover fails. Keep Parquet files referenced by any retained catalog or open browser session; uploading a newer catalog does not authorize deleting old objects. Breaking schema changes require a new data-version prefix and a corresponding site change.

## Production acceptance: 2026-09-06

[Site PR 5](https://github.com/droher/baseball.computer.site/pull/5) deployed verified squash commit `5002dfa569414495ead15ddc36656e8352669273` to production. All 33 Playwright checks passed on `https://baseball.computer` across Chromium, Firefox, and WebKit, including the default full-statistics query, read-only DuckLake attachment, real-data CSV export, local-file joins, schema insertion, history, and EXPLAIN. Chromium/WebKit network checks confirmed Parquet requests and no legacy `/dbt/` query requests.

The release contains 169 tables and 1,605,595,144 rows with source/export row-count parity. All 696 published objects match local sizes and strong checksums: 694 Parquet files, the DuckLake catalog, and schema metadata, totaling 11,166,176,348 bytes. Native public event aggregates matched the source and legacy release: 331,404 home runs, 3,649,508 hits, and 14,024,857 at-bats. Catalog HTTP responses return 206 for byte ranges, allow the site's Origin, and retain `max-age=0, must-revalidate`.

Publisher/upload validation passed 25 tests, Ruff, and strict script type checking. Site validation passed 44 unit tests, type checking, lint, and the Vercel build.
