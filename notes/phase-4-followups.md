# Phase 4 follow-ups

Phase 4 shipped the DuckLake publish side as a parallel artifact alongside
`scripts/create_web_db.py`. The original parquet+`bc_remote.db` flow
continues to publish on every build; the DuckLake artifact is for site-team
validation. Local end-to-end validation completed 2026-05-04 against a
fresh prod build of `main_models`; the publish artifact is ready for
the R2 upload step. Site cutover (4.x) stays deferred until the site
team confirms parity on a test branch.

## 4.0 Local validation receipt (2026-05-04)

Wiped `bc.db` + `bc/bc_state.db`, ran `preload_sources` then
`sqlmesh plan --auto-apply` (no env = prod) end-to-end, then
`scripts/publish_ducklake.py --reset -v`. Parity script attaches the
generated catalog and the source database side-by-side.

Numbers:

- 132 tables published (110 `main_models` + 22 `main_seeds`).
- 683,246,606 rows total, 42.8s wall.
- 24 ENUM types in `bc.db`; cast to VARCHAR on publish per the existing
  `select_with_enum_casts` path.
- Catalog 4.2 MB, data path 6.3 GB on disk (zstd, 1.97M-row groups,
  inlining disabled).
- Snapshot retention: 130 transient snapshots from per-table commits
  expired down to the last 5 via `ducklake_expire_snapshots`.
- Smoke check on 5 sample tables passes; consumer-side `DESCRIBE` returns
  expected column counts and types.

Parity vs `bc.db`:

- 0 / 132 row-count mismatches.
- Aggregate parity (COUNT, MIN, MAX, SUM-as-HUGEINT, COUNT(DISTINCT
  text_col)) on 6 representative tables — `event_states_full`
  (18.1M rows, ENUM-heavy), `team_game_results` (478K rows, Phase 5
  Polars FSM port), `event_pitching_flags` (18.1M rows, Phase 5 first
  wave), `metrics_player_career_offense` (20K rows, Ibis-built),
  `predictions_is_in_play_bin` (16.3M rows, Phase 6 ML),
  `seed_franchises` (365 rows, ENUM-free) — exact match between
  `bc.<schema>.<table>` and `bc_pub.<schema>.<table>`.

The R2 upload step (`scripts/upload_ducklake.py`) is unchanged and
unrun. The user runs it when ready to push to R2.

## Prod build fixes (2026-05-04)

A fresh `sqlmesh plan --auto-apply` against an empty `bc.db` surfaced
four real bugs that had to be fixed before `main_models` materialized
end-to-end. Each landed on `next`:

1. **`relationships` audit macro polarity (`bc/macros/_env_to_model.py`)** —
   the env-aware short-circuit returned `exp.true()` when the FK target
   was missing, but the audit body uses that as a WHERE predicate, so
   TRUE flagged every non-null source row as a violation. Switched to
   `exp.false()` and renamed `_table_exists` → `_table_populated` so
   the short-circuit also fires when the target exists but is still
   empty (fresh prod plan: a staging audit can run before its
   `to_model` referent backfills, since those references aren't in
   the DAG to avoid cycles). The first plan after a clean wipe now
   passes the audits vacuously; subsequent plans run the real check.

2. **`team_game_start_info` MAP cast** — the Phase 5 wave-2 Polars FSM
   port couldn't survive the round-trip. DuckDB MAP becomes Polars
   `list[struct]`, then `to_pandas()`, and SQLMesh's contract
   enforcement does `CAST(struct[] AS MAP(...))` — DuckDB has no
   implementation for that direction. Reverted the model to its
   original SQL (window-function `forward_fill` /
   `cum_count` equivalents) and dropped the `.py` wrapper, the
   `python_models/game_level/team_game_start_info.py` Polars helper,
   and the matching `tests/test_team_game_start_info.py`. Phase 5
   wave-2 still covers `team_game_results`; only the MAP-shaped
   sibling fell back to SQL.

3. **Third-wave ML predictions gated by artifact presence** —
   `bc/python_models/ml/__init__.py` exposes `artifact_exists(target)`,
   and the six third-wave `predictions_<target>.py` wrappers
   (`baserunning_cat`, `batted_location_cat`, `batted_trajectory_cat`,
   `has_batting_bin`, `is_win_bin`, `runs_following_num`) pass it to
   `@model(enabled=...)`. SQLMesh skips the model entirely when the pin
   JSON isn't on disk, so a missing training artifact no longer breaks
   a fresh prod plan. The two trained targets (`is_in_play_bin`,
   `plate_appearance_cat`) materialize as before. Drop or remove the
   `enabled=` line on each wrapper after its `scripts/train_<target>.py`
   run lands the pin.

4. **`stg_box_score_pinch_hitting_lines` not_null** — 2255 early-1900s
   pinch-hit rows (mostly 1908-09 NL games) record an appearance with
   no inning. inning is part of the grain so the rows can't be retained
   without breaking unique_grain. Filtered them at the staging CTE
   with `WHERE inning IS NOT NULL`; recover when the source parser
   fills the gap.

5. **`metrics_*` not_null no longer requires the volume column** —
   `python_models/metrics/registration.py` previously included
   `outs_played` / `plate_appearances` / `batters_faced` in the
   `not_null` audit alongside the grain. Negro Leagues (NN1, NN2) and
   pre-1900s rows don't carry an `outs_played` figure, producing
   75 + 169 nulls in `metrics_player_career_fielding` /
   `metrics_player_season_league_fielding`. Audit now checks the
   grain only; downstream rate metrics already tolerate null volume.

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
