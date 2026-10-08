# Compare processed Retrosheet sources before promotion

`just compare-sources` runs SQLMesh's installed `TableDiff` API against a read-only baseline and frozen processed parquet. It does not load a SQLMesh project, change environment state, or promote production. No extra diff package is required. [SQLMesh TableDiff guide](https://sqlmesh.readthedocs.io/en/stable/guides/tablediff/).

```sh
just compare-sources --baseline-db bc.db \
  --source-root https://data.baseball.computer/event \
  --source-version 20261008 \
  --output-dir outputs/retrosheet-diff-20261008 --game-limit 25

just compare-sources --baseline-db bc.db \
  --source-root https://data.baseball.computer/event \
  --source-version 20261008 \
  --output-dir outputs/retrosheet-diff-20261008 --offline
```

Choose a fresh output directory and version for each capture. The source root is the shared parquet directory for `event`, `game`, and `box_score`; those logical schemas do not correspond to separate R2 directories. The version is a cache-busting query parameter, not a guarantee that R2 retains historical versions.

The smoke limit selects a deterministic prefix of baseline play-by-play game IDs. It limits comparison rows, while source downloads and inventory counts still cover every raw table. A smoke is an operational check, not a representative estimate of historical changes. Full comparison uses every baseline play-by-play game.

The first capture downloads with checked `curl`, records HTTP response headers and SHA256 for every file, and checks ETag/Last-Modified/length before and after each download. A complete manifest freezes the capture; future invocations verify every cached file and refuse a changed root, version, table inventory, or byte digest. `--offline` requires a complete frozen cache. The output path must be outside the baseline and may contain no symlinks or hardlinks to the baseline. The baseline is attached with `READ_ONLY`; file size, inode, and nanosecond modification time must be unchanged after closing the comparison.

## Interpretation

The compared source families are raw processed play-by-play, game context, and box score tables. These are not published modeled tables. Changes can come from a Retrosheet release, correction inputs, or parser behavior; a parquet timestamp cannot identify the deployed parser commit. Local upstream context for the October 2026 capture is parser commit `0b490d73249b1aa26fbcde1d9079d8f48fe72322`, titled “Ingest October 2026 Retrosheet release,” with correction changes. This is context, not proof that R2 was built from that commit.

Game identity uses `game_id`. Global `game_key` and `event_key` are excluded from semantic comparisons. Event correspondence uses `(game_id,event_id)` only after the entire core stream agrees: inning, frame, batter, pitcher, PA result, no-play flag, outs, base state, event counts, and unique event IDs. Corrected or reordered games retain game-level coverage comparisons but are excluded from exact event change claims. ENUM and VARCHAR values are normalized before native row comparison.

The seed `bc/seeds/misc/seed_plate_appearance_result_types.csv` defines batted-event eligibility, including home runs. No-play rows are excluded. Complete count coverage uses completed PA rows with no-play excluded, and requires balls 0–3 and strikes 0–2. A recorded count at a runner-only event is outside that denominator.

Trajectory excludes null and `Unknown`. Fielder detail requires position 1–9. General location excludes null and `Unknown`. Depth, angle, and contact strength report explicit detail: `Default` and `Unknown` do not establish measured detail. The observation ledger separately counts null, unknown-coded, default detail, and known values; those categories conserve each eligible denominator. `Default` can be a legitimate parser default, so explicit-detail percentages are not estimates of physical completeness.

Fielding coverage is conditional on fielding child rows, with position 0 unknown. Baserunner outcome presence is conditional on baserunner child rows: an explicit out, a scored run, or a nonnull ending base establishes an outcome. A null ending base for an out is valid. Non-out/non-scoring rows without an ending base are flagged for review; they are not proven `Unknown` source outcomes. Pitch statuses report `Resolved`, `Unavailable`, and `Unresolved` among existing status rows. Entirely absent child-table rows remain absent, not known zeros. Numeric weather fields report nonnull presence; they do not establish that zero or another numeric value is physically observed. Attendance and time-of-game require positive values. Context is a recorded-metadata comparison, not verification of actual weather or scoring personnel.

For holistic raw content, each table is normalized to a canonical per-game row payload and multiplicity, excluding surrogate keys and filename/line-number provenance. SQLMesh diffs this multiset on `(game_id,SHA256(canonical row payload))`, preserving duplicate rows without invented child-row pairings. Added and removed identities are content changes under the standard SHA256 identity assumption; they do not imply a particular old row became a particular new row. Original native schema diffs are exported separately. The inventory's XOR fingerprints are normalized diagnostics; SHA256 file digests identify the frozen new bytes, and native diffs establish content changes.

## Artifacts

The output contains `source-manifest.json`, `new/*.parquet`, HTTP headers, and separate `smoke-N/` and `full/` runs:

- `table-inventory.json`: all raw schema and row-count changes.
- `game-changes.csv`, `game-year-inventory.csv`: additions, removals, account transitions, and game types.
- `event-alignment.csv`: exact comparison eligibility for each old play-by-play game.
- `coverage-game.csv`, `coverage-year.csv`: old/new eligible and detailed counts, including unaligned games.
- `observation-status-game.csv`, `observation-status-year.csv`: eligible/null/unknown/default/known partitions.
- `native-*.json`: SQLMesh authoritative row-diff counts; native column statistics and added/removed/changed CSV samples accompany them.
- `schema-*.json`: SQLMesh original schema comparisons.
- `aligned-coverage-changes.json`: gross known-value gains and losses in aligned events, derived from native boolean mismatches and independent known totals.
- `raw-play-changes.csv`: exact raw-play differences only in aligned games.
- `summary.json`, `comparison.duckdb`, and redirected logs: completion evidence and reusable staged comparison.

Native raw-table samples identify canonical row digests and multiplicity; their counts refer to distinct content identities, while the inventory records total raw rows. Native samples are bounded examples, not complete change ledgers. The full game coverage ledger and native counts provide the aggregate evidence. The isolated comparison database can grow substantially while native diff tables spill to disk. The routine uses two threads and a 4 GB DuckDB cap; output storage is separate from `bc.db`.

`--supplement-existing --offline` regenerates domain observation/known-transition reports and original schema exports from a completed staged comparison, without repeating the raw native diffs. It still validates the frozen sources and baseline integrity. This is useful when refining ledger interpretation while the captured data are unchanged.

## SQLMesh modeled comparison afterward

Raw-source comparison does not establish that published models rebuild correctly. After review, build a separate SQLMesh dev candidate using the frozen parquet as the `event`, `game`, and `box_score` source roots; then use SQLMesh's environment/model `table_diff` against the baseline modeled environment. An already initialized dev database may retain old raw source tables because initialization uses `CREATE TABLE IF NOT EXISTS`.

Do not use `BC_DB_PATH=bc_dev.db just preload --force-reload`: `scripts/preload_sources.py` currently hardcodes `bc.db`, so that command would replace production source tables. Prepare a fresh isolated dev SQLMesh database through its configured initialization path before candidate materialization. Promotion is a separate authorized action.
