# DuckLake production publication

The site at https://baseball.computer/query-engine runs DuckDB-WASM in the browser and attaches the public DuckLake 1.0 catalog at `https://data.baseball.computer/baseball/v1/baseball.ducklake`. `catalog.json` in the same prefix supplies the schema sidebar, and `baseball.lsf` in the same prefix is the LSF-1 context packet the site's text-to-SQL feature loads. Native DuckDB clients can use the same catalog without credentials.

## Publish

1. Build production through the SQLMesh recipes in `justfile` when model changes require it.
2. Run `uv run --group build python scripts/publish_ducklake.py`. The source `bc.db` is read-only. The publisher checks estimated-table population and source/export row-count parity, casts ENUMs to VARCHAR, and writes ZSTD Parquet with a 128 MB target file size. After the tables it creates the `semantic` views and `metrics` macros (below), then regenerates `docs/llm/baseball.lsf` by running `scripts/generate_llm_context.py --validate`, so the packet always describes the catalog it ships with. Use `--smoke-only` to read an existing export. `--reset` rebuilds the derived local export and cannot be combined with smoke-only. `--semantic-only` recreates the views and macros on an existing export without copying tables.
3. Run `uv run --group build python scripts/upload_ducklake.py`. Supply the documented R2 and Cloudflare environment variables, or use `--wrangler` with an existing Wrangler OAuth login. Wrangler uploads preflight its 300 MiB file limit. Data uploads default to four at a time (`--workers 1..16`); metadata, the packet, and the catalog publish last. Use `--resume` after interruption: it skips immutable data files only after matching the public size and strong ETag to a local streaming MD5. Catalog, schema, and packet are always uploaded; the uploader refuses to start when the packet fails LSF-1 validation.
4. For a fresh version prefix, `--skip-purge` avoids requiring a separate cache-purge token. For previously cached catalogs, use the cache-purge credentials. Parquet objects are immutable; catalog, schema metadata, and the packet use `max-age=0, must-revalidate` and the site's CORS origin.
5. Rebuild/deploy the site after schema changes. Its `src/lib/io/data-source.ts` version must match `bc/data_version.txt`.

The GitHub workflow requires a configured self-hosted runner, R2 secrets, cache-purge secrets, and `BC_STATS_ARTIFACTS_ROOT`; local Wrangler publication is also supported. Never commit credentials or local data artifacts.

## Cloudflare metadata caching

The `DuckLake metadata revalidation` cache rule bypasses edge caching and respects the origin browser TTL for `data.baseball.computer` paths under `/baseball/` ending in `.ducklake`, `/catalog.json`, or `.lsf`. It overrides the legacy month-long cache-everything page rule for metadata only. Without it even `max-age=0` can become a month-long browser cache lifetime. Public metadata responses must retain `max-age=0, must-revalidate`, CORS, and range support. Parquet keeps immutable caching.

`upload_ducklake.py` owns that rule. Before uploading anything it reads the zone's cache-settings ruleset through the Cloudflare API, finds the rule by name, and creates or updates it so its expression lists every metadata object the script publishes. The expression is built from the same object names the uploader uses, so a new metadata object extends the rule on the next run. Only that one rule is written; other rules in the ruleset are untouched, and two rules with that name stop the run. After the purge the script HEADs each metadata URL and fails on a `cf-cache-status` of `HIT`. `--skip-cache-rule` skips the check when no Cloudflare token is available, for example with `--skip-purge` on a fresh prefix from a machine without credentials.

The `CLOUDFLARE_API_TOKEN` used by the workflow and by local runs therefore needs the zone's Cache Rules edit permission in addition to Cache Purge. The rule's settings live in the script as `CACHE_RULE_ACTION_PARAMETERS`; change them there, not in the dashboard, or the next upload will put them back.

Locally the script fills any missing or empty variable from `~/.config/baseball.computer/cloudflare.env` (override the path with `BC_CREDENTIALS_FILE`); the file holds `KEY=value` or `export KEY=value` lines, the environment always wins, and a missing file is not an error. The token needs Zone → Cache Rules Edit, Cache Purge, and Zone Read on the `baseball.computer` zone, alongside `CLOUDFLARE_ZONE_ID`. `just metadata-cache` repairs the rule and purges the current version's catalog, `catalog.json`, and packet URLs without uploading anything, which is how an upload that ran without Cloudflare credentials gets fixed. Cloudflare keys cached responses by request `Origin`, so each purge sends the plain URL plus one entry per origin: `https://baseball.computer` and `http://localhost:4173` by default, and repeated `--purge-origin` flags replace that default list. `--skip-purge` and `--skip-cache-rule` still apply.

## Semantic views and metric macros

The catalog carries two schemas beyond `main_models` and `main_seeds`:

- `semantic` holds one view per BSL semantic table: `offense_seasons`, `offense_events`, `pitching_seasons`, `pitching_events`, `fielding_seasons`, `fielding_events`. Their SQL comes from `bc/semantic/_views.py` and matches the ibis definitions in `bc/semantic/_tables_common.py` row for row: season views join `main_seeds.seed_franchises` for `league` and keep only regular-season game types; event views join `main_models.team_game_start_info` for the game columns. The packet's `TABLE` records name these views as their `physical_name`.
- `metrics` holds one aggregate macro per metric name in the registry, callable as `metrics.<name>(<base columns>)` inside a `GROUP BY` query, for example `metrics.earned_run_average(earned_runs, outs_recorded)`. Bodies are rendered from the registry lambdas by `bc/python_models/metrics/sql_render.py`; derived metrics are inlined down to base columns because DuckDB macros cannot call other macros. Every division becomes `x / nullif(y, 0)`, so a macro returns NULL where the stored rate column on a `metrics_*` table holds inf or NaN. Each packet `METRIC` record's description carries the exact call.

Both are DuckLake catalog objects, so the published `baseball.ducklake` file carries them and DuckDB-WASM sees them after a read-only attach under any alias. Table references inside the views are schema-qualified without a catalog name for that reason. Recreate them alone with `publish_ducklake.py --semantic-only` when only the registry or view SQL changed; the tables are untouched and the packet is regenerated afterwards.

Tests: `bc/tests/test_semantic_views.py` (slow, needs `bc.db`) diffs each view against the ibis expression on a sample of seasons and games; `bc/tests/test_metric_macros.py` checks every macro compiles and equals the packet expression; `bc/tests/test_verified_queries.py` parses every verified query and, under `-m slow`, runs each one against the local export.

## Verify

- Run publisher/upload tests in `bc/tests/test_publish_ducklake.py` and `bc/tests/test_upload_ducklake.py`.
- Query the public catalog with native DuckDB, reading actual table values rather than only catalog row counts.
- Verify public catalog and Parquet GET/HEAD responses allow the site's Origin and HTTP Range requests.
- `curl -sI https://data.baseball.computer/baseball/v1/baseball.lsf` returns 200 with `cache-control: max-age=0, must-revalidate`, `content-type: text/plain; charset=utf-8`, and the site's `access-control-allow-origin`. `just gen-llm-context` on the same commit must produce a byte-identical file.
- From a native DuckDB attach of the public catalog, `SELECT count(*) FROM semantic.offense_seasons` returns rows and `SELECT metrics.batting_average(hits, at_bats) FROM semantic.offense_seasons WHERE season = 2019` resolves; the site's DuckDB-WASM attach sees the same objects.
- In the site repository run `pnpm check`, `pnpm lint`, and `pnpm test`. After deployment run `PLAYWRIGHT_BASE_URL=https://baseball.computer pnpm test:integration` against production.
- Browser acceptance includes a read-only `ducklake` attachment, real Parquet requests, baseball joins/aggregation, CSV exports, and joins to locally uploaded files. The site must make no query requests to `/dbt/`.

## Retention and rollback

Retain existing `/dbt/` objects for external consumers and rollback. Restore the preceding Vercel deployment if the frontend cutover fails. The local catalog keeps only its latest snapshot: after every publish the script expires every earlier snapshot and deletes the local data files nothing references, so `bc/bc_publish_data/` holds exactly one release (about 11 GB). Parquet files from earlier releases stay on R2 because a catalog copy someone retained or a browser session may still read them; uploading a newer catalog does not authorize deleting old objects. Breaking schema changes require a new data-version prefix and a corresponding site change.

The packet, views, and macros roll back with the catalog: re-uploading an earlier `baseball.ducklake` restores that version's views and macros, and its data files are still on R2 even though the local export no longer holds them. `baseball.lsf` is gitignored, so keep the packet from each publish next to the catalog copy you retain for rollback, or regenerate it with `just gen-llm-context` from the same commit and `bc.db`. Re-upload both and purge both URLs. Renaming a metric or a semantic view changes what the packet tells the LLM to call, so treat it as a breaking change for the site's text-to-SQL feature even though the base tables are unchanged.

## Production acceptance: 2026-09-06

[Site PR 5](https://github.com/droher/baseball.computer.site/pull/5) deployed verified squash commit `5002dfa569414495ead15ddc36656e8352669273` to production. All 33 Playwright checks passed on `https://baseball.computer` across Chromium, Firefox, and WebKit, including the default full-statistics query, read-only DuckLake attachment, real-data CSV export, local-file joins, schema insertion, history, and EXPLAIN. Chromium/WebKit network checks confirmed Parquet requests and no legacy `/dbt/` query requests.

The release contains 169 tables and 1,605,595,144 rows with source/export row-count parity. All 696 published objects match local sizes and strong checksums: 694 Parquet files, the DuckLake catalog, and schema metadata, totaling 11,166,176,348 bytes. Native public event aggregates matched the source and legacy release: 331,404 home runs, 3,649,508 hits, and 14,024,857 at-bats. Catalog HTTP responses return 206 for byte ranges, allow the site's Origin, and retain `max-age=0, must-revalidate`.

Publisher/upload validation passed 25 tests, Ruff, and strict script type checking. Site validation passed 44 unit tests, type checking, lint, and the Vercel build.
