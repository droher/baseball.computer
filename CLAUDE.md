# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

`baseball.computer` is the dbt project that builds the SQL database powering [baseball.computer](https://baseball.computer). It is the **middle tier** of a three-tier pipeline; resumption order matters when refreshing data:

1. **`boxball-rs`** (separate repo) — Rust parser that turns Retrosheet raw data into Parquet files and uploads them to Cloudflare R2 (`https://data.baseball.computer/event/*.parquet`, etc.).
2. **`baseball.computer`** (this repo) — dbt-duckdb pipeline that reads those Parquet files via `httpfs`, builds the analytics database, and exports the result back to R2.
3. **`baseball.computer.site`** (separate repo) — DuckDB-WASM frontend that consumes the exported `bc_remote.db` + Parquet views.

The dbt project lives in `bc/`. The Python at the repo root is glue (export/upload scripts).

## Commands

All commands assume you are in the repo root unless stated otherwise.

### Environment

The project uses **uv** — the lockfile is `uv.lock` and `pyproject.toml` uses PEP 621 `[project]`. Migrated off Poetry in the 2026 refresh; `poetry.lock` removed.

```bash
uv sync                # install + create .venv from uv.lock
uv run <cmd>           # run inside the env
```

Python pinned `>=3.12,<3.13`. Lift the upper bound once we confirm 3.13 wheels for the full dep stack.

**ML layer is disabled.** `bc/models/intermediate/machine_learning/` (`ml_event_outcomes`, `ml_features`) is excluded from `dbt run` via `+enabled: false` in `bc/dbt_project.yml`. The JAX/Keras dep stack was removed in the 2026 refresh (PLE-383/PLE-384). SQL kept in tree; re-enable by flipping the flag and reinstating the deps.

### dbt profile

The dbt profile is **not** in `bc/`; it lives at `~/.dbt/profiles.yml` under the profile name `bc`. The default `dev` target writes to `/Users/davidroher/Repos/baseball.computer/bc.db` (absolute path, hard-coded). Settings: `httpfs` + `parquet` extensions, `threads: 6`, `disable_transactions: true`. DuckDB tuning: `enable_fsst_vectors`, `enable_http_metadata_cache`, `parquet_metadata_cache`, `preserve_insertion_order: false`, `checkpoint_threshold: 1GB`. No `memory_limit` — DuckDB uses its default (80% of system RAM). No explicit `temp_directory` — multi-thread connection inits errored on "Cannot switch temporary directory after the current one has been used".

### Build

```bash
cd bc
dbt deps                      # first time only — installs dbt_utils, codegen
dbt run-operation init_db     # bootstrap: pulls all sources from R2 into local DuckDB
dbt run-operation create_enums  # create DuckDB enum types from the loaded sources
dbt run-operation alter_types   # apply column types declared in source YAML
dbt run                       # build all models
dbt test
```

`init_db` accepts `--args '{sample_factor: 10, seed: 0}'` to load a 1-in-N sample of `event` rows for fast iteration. The macro reads from `https://data.baseball.computer/<prefix>/<source>.parquet` based on each source's schema.

### Single-model workflows

```bash
dbt run --select model_name              # one model
dbt run --select +model_name             # model + all upstream
dbt run --select model_name+             # model + all downstream
dbt build --select tag:metrics           # if tags are used
dbt test --select model_name
```

### Lint

```bash
sqlfluff lint bc/models/intermediate/states/event_states_full.sql
sqlfluff fix bc/models/...
```

`.sqlfluff` is at the repo root, dialect `duckdb`, templater `dbt`, max line length 120, uppercase keywords. The dbt templater is configured with `project_dir = ./bc`, `profiles_dir = ~/.dbt`.

### Publish to R2 (web export)

```bash
uv run python scripts/create_web_db.py
```

Requires env vars: `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`. Iterates every schema/table in `bc.db`, exports to Parquet (ZSTD, ~1.9M-row groups; **GZIP + 262K row groups for `event_states_full`** because it's much wider), uploads to R2 bucket `timeball` under `dbt/`, then writes a fresh `bc_remote.db` with views pointing at the R2 URLs and uploads that too. The site loads `bc_remote.db` to lazy-fetch Parquet on demand.

## Architecture

### Layered dbt pipeline (`bc/models/`)

All models materialize as **tables** by default (`+materialized: table` in `dbt_project.yml`), into the `models` schema. Seeds go to `seeds`.

```
sources (R2 Parquet)         -- schemas: event, game, box_score, baseballdatabank, misc
   │
   ▼
staging/                     -- 1:1 normalization of sources; `stg_<source>` naming
   │   event/, game/, box_score/, baseballdatabank/, misc/
   ▼
intermediate/                -- the bulk of the logic
   ├─ event_level/           -- per-event derivations: batting, pitching, fielding,
   │                            baserunning, batted-ball type, pitch-sequence stats
   ├─ states/                -- in-game state tracking (heaviest models)
   │     event_states_full   -- complete base/out/score/personnel snapshot per event
   │     event_base_out_states, event_count_states, event_score_states,
   │     event_states_batter_pitcher, personnel_lineup_states,
   │     personnel_fielding_states, event_personnel_lookup, event_fielders_flat
   ├─ expectancy/            -- run/win expectancy, linear weights, leverage,
   │                            event_transition_values, unknown_fielding_play_shares
   ├─ park_factors/          -- aggregate + batter-pitcher matched-pair methodology
   ├─ flags/                 -- event_pitching_flags
   ├─ player_game_level/, player_season_level/, season_level/, game_level/
   ├─ bio/people.sql         -- player biographical data (Retrosheet + Baseball Databank)
   └─ machine_learning/      -- ml_event_outcomes, ml_features (disabled; see Environment)
   │
   ▼
metrics/                     -- 9 user-facing aggregate tables + standings
       metrics_player_career_{offense,pitching,fielding}
       metrics_player_season_league_{offense,pitching,fielding}
       metrics_team_season_{offense,pitching,fielding}
       standings
```

### Things to know before editing

- **`event_states_full` is the workhorse.** It's wide enough that the export script special-cases it (smaller row groups, GZIP). Window functions partitioned by `(game_id, frame, inning)` show up everywhere downstream — keep that partitioning consistent.
- **`run_expectancy_matrix` and `linear_weights`** split `DoublePlay` from `InPlayOut` and union batting + baserunning plays. If you're adding a new event type, check both branches.
- **Park factors fall back gracefully.** The advanced batter-pitcher-matched-pair method needs sufficient sample; the model uses the basic aggregate methodology when matched-pair data is too sparse for a given year.
- **`init_db.create_enums`** builds DuckDB enums from `SELECT DISTINCT` over the loaded sources (game type, sky, plate-appearance result, pitch sequence item, etc.). `player_id` and `game_id` are intentionally `VARCHAR` rather than enums — see commented-out blocks in `macros/init_db.sql`.
- **`alter_types`** reads `data_type` from source YAML and applies it post-load. Sources are loaded as raw Parquet types first, then upcast/downcast.
- **Macros worth knowing:**
  - `metric_calcs.sql` + `stat_lists.sql` — central definitions of stat formulas; the `metrics_*` tables are generated by `metrics_table_generator.sql` consuming these lists. Edit the macros, not the metric SQL by hand, when adding a stat.
  - `park_factors.sql` — shared park-factor logic.
  - `event_id_to_key.sql` — composite-key encoding used throughout.

### Data flow caveats

- The pipeline does **not** parse Retrosheet itself — that's `boxball-rs` (now publishing via `bin/fetch_retrosheet.py`, replacing `alldata.zip`). If event-level columns are missing or wrong, the fix usually belongs upstream.
- There is **no CI** yet (PLE-394 in flight); refreshes are manual (`dbt run` then `python scripts/create_web_db.py`).
- Source layout: per-schema R2 prefixes (`event/`, `misc/`, `baseballdatabank/`, `biodata/`) under `https://data.baseball.computer/`. Override via `vars: source_roots` in `dbt_project.yml` to point at local boxball-rs output. `init_db` macro appends `?v=<run-started-at>` cache-bust querystrings to HTTPS sources to bypass stale Cloudflare range caches.
- Biodata bundle: `teams`, `coaches`, `relatives`, `ejections`, `managers0`, `umpires0` masters live in the `biodata` schema, replacing per-year `umpires/UMPIRES{YYYY}.txt` and `teams/team{YYYY}*.csv` files no longer mirrored.

## Other directories

- `bc/analyses/` — ad-hoc SQL queries (BABIP by count, fielder advance expectancy, completeness QA, etc.). Not part of `dbt run`.
- `bc/seeds/` — small CSV taxonomies (baserunning, batted_ball, scorekeeping, misc). Reference data, not derived.
- `bc/tests/` — dbt tests (currently empty/light; most tests are `*.yml` schema tests inline with models).
- `docs/` — generated dbt docs site (catalog.json, manifest.json, index.html). Hosted at https://docs.baseball.computer (CNAME in `docs/`).
- `scripts/add_doc_block_refs.py`, `scripts/generate_seed_yaml.py` — codegen helpers.
- `scratch.ipynb` — exploratory notebook, not part of the pipeline.
- `bc/qa_notes.md` — invariants the pipeline expects (e.g. plate appearances must have a pitch sequence; outs must have a putout). Treat as authoritative when adding event-level logic.
