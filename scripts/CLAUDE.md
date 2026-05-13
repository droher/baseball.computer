# Publish pipeline

- `publish_ducklake.py` copies `main_models.*` + `main_seeds.*` out of `bc.db` into the `bc_publish` DuckLake catalog (`bc/bc_publish.ducklake` + `bc/bc_publish_data/`). ENUM columns cast to VARCHAR — DuckLake v1.0 doesn't preserve user-defined types.
- `upload_ducklake.py` ships catalog + data dir to `s3://timeball/baseball/v<DATA_VERSION>/` with long-lived `Cache-Control` and a Cloudflare cache purge.
- `create_web_db.py` publishes `bc_remote.db` + per-table parquet under the `dbt/` R2 prefix as the canonical site artifact. DuckLake site cutover is tracked in `notes/followups.md`.
- `preload_sources.py` is the only ad-hoc script allowed to write `bc.db` directly (only `CREATE TABLE IF NOT EXISTS`). Everything else goes through SQLMesh.
