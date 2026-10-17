# Compare fully processed Retrosheet results

A Corpus revision is evaluated with two isolated builds that differ only in their sources:

| Build | Code and seeds | Sources |
| --- | --- | --- |
| prod (`bc.db`) | Prod | Prod |
| A | Branch | Prod, frozen from `bc.db` |
| B | Branch | Every schema from one sources folder |

Three comparisons answer the questions a revision raises:

- **A→B** shows what the revision changes, with code held fixed.
- **prod→A** shows what the branch's code and seed changes do, with sources held fixed.
- **prod→B** shows everything promotion would change.

When the branch's SQLMesh fingerprints and seed contents equal prod's, A is prod and the builder skips it. Prod's fingerprints come from `bc/bc_state.db`, which matches `bc.db` only after a completed `just promote-prod` or `just rebuild-prod`; a bare `sqlmesh plan` against prod advances the state without rebuilding the tables. Pass `--always-build-a` when that is in doubt.

## Build the candidates

Build the sources folder from the Corpus revision with baseball.computer.rs's `bin/build_sources.py --corpus <corpus> --out <sources>`. It writes the five folders R2 serves (`event/`, `misc/`, `baseballdatabank/`, `ngl_stats/`, `biodata/`) and a `sources.json` naming the revision with the SHA-256 and size of every file; CI publishes the same folders. Then run the builder from your own terminal, since a full run outlasts an agent session:

```sh
just build-source-candidate \
  --sources <sources> \
  --output outputs/<revision>/candidates \
  > outputs/<revision>/candidates.log 2>&1
```

Side B reads `event`, `game` and `box_score` from `event/`, `misc` from `misc/`, `baseballdatabank` and `biodata` from their folders, and `nlb_stats` from `ngl_stats/`. From a worktree, pass `--prod-db` and `--prod-state` for the main checkout's `bc.db` and `bc/bc_state.db`.

The builder:

1. Exports every external source table from `bc.db` to `sources/prod/` and records its digest. `bc.db` is opened read-only, and the run stops if its size, modification time or inode changes.
2. Verifies every external source's file against `sources.json`, refusing a file that is missing from the manifest or differs from it. A resumed run checks each file's size, modification time and inode instead of rehashing it.
3. Copies `bc/`, `scripts/preload_sources.py` and `scripts/audit_report.py` into `snapshot/` and records the git commit and status. Branch seeds are used as they are.
4. Builds B in `B/bc.db` with a fresh SQLMesh state: preload, then a bare `sqlmesh plan`. A fresh state makes this the prod environment, so models land in `main_models.*` and blocking audits stop the build exactly as they would in production. When the plan fails, the builder still writes `audit-report.json` for the partial build before stopping.
5. Builds A the same way with prod's Retrosheet sources, unless A equals prod.
6. Runs `scripts/audit_report.py` on each build and writes `audit-report.json` beside it.
7. Writes `candidates.json`: the revision, and for each side, database identity, Retrosheet and other source digests, SQLMesh fingerprint digest and seed digest.

Each step is checkpointed in `checkpoint.json`, so rerunning the same command resumes at the first unfinished step. The rerun is refused if `bc.db`, `bc/bc_state.db`, the sources folder or the source list changed, and a run stops if `bc.db` or its state changes before `candidates.json` is written. A failed SQLMesh plan resumes from its own state on rerun. Each build needs roughly 16 GB for the database plus up to 33 GB of DuckDB temp space, and takes about 15 minutes at the default settings.

## Accept a revision for promotion

B's `audit-report.json` is the acceptance check. It runs every model audit with its configured blocking status and separates blocking failures, warnings and errors. A table that cannot be read counts as an error, not a pass. `just audit-report --db bc.db --output <path>` gives the same report for prod.

## Compare

Comparison is optional. Event alignment comes from `just compare-sources` run with `--source-root file://<sources>/event`.

```sh
just compare-models --candidates outputs/<capture>/candidates/candidates.json \
  --old A --new B \
  --alignment outputs/<capture>/full/event-alignment.csv \
  --output outputs/<capture>/model-diff/a-to-b \
  > outputs/<capture>/model-diff-a-to-b.log 2>&1
```

`--old` is `prod` or `A`, and `--new` is `A` or `B`, so the old side always holds prod's Retrosheet sources and existing play-by-play cohort. `--alignment` is required when the two sides read different Retrosheet sources. It is accepted only when it was computed from this `bc.db` and its `source-manifest.json` gives the same Retrosheet files as B. Before comparing, each side's database must match its recorded identity. `inputs.json` records an attribution label derived from the recorded digests: source change only, code change only, or code and source change.

Start with `--limit-games 25` and a few `--table` flags. After that succeeds, omit both for the default result and coverage tables. Event batting, offense, pitching and player-fielding details are optional and must be requested with `--table`. The player-fielding table expands each event to nine positions and can need substantial comparison storage. `--sample-rows` controls exported examples, not the population used for counts. The run checkpoints after each table, and `summary.json` confirms completion. Use a fresh output directory for each run.

## Scope and identity

`cohort.csv` fixes game membership using the old side's `game.games` with `account_type = PlayByPlay`, raw event existence and baseline `game_start_info`. The old-or-new Negro Leagues classifier removes games in either classification. Baseline season labels drive `cohort-year.csv`. Candidate-only games never enter this cohort.

Game, team-game and player-game results include games with edited event streams. Event-level comparisons pair `(game_id, event_id)` only in aligned games. When both sides read the same Retrosheet sources (prod→A), every game is aligned. Otherwise alignment comes from the source comparison's `event-alignment.csv`. That alignment checks stable core event information and unique event IDs; it does not invent matches when events were inserted, removed or reordered. Edited games remain in both sides' per-game coverage totals and example extracts.

Keys come from an explicit registry and are validated for uniqueness and nulls before comparison. Global `event_key`, `game_key`, personnel keys and other `*_key` fields are excluded from row equality. Event tables that expose only `event_key` resolve it against each database's own `event_states_full`, then compare stable game/event identifiers. ENUM values are normalized to VARCHAR for row comparison; original types remain in the schema report. HUGEINT values, including nested lists/maps/structs, travel through parquet as exact text and are restored to their original DuckDB type before comparison; parquet otherwise silently converts them to DOUBLE.

The registry covers processed game/results/context, team-game and player-game offense/pitching/fielding, event stats and states, transitions, and published completeness analyses. `inventory.json` lists every `main_models` table in either database, with availability and selection status. Season and career metrics are deliberately excluded: their published values incorporate newly added accounts and some career tables mix Negro League contributions. A change there cannot be labeled an existing-account correction. To study those effects, aggregate additive game-level counts over `cohort` first and recompute rate statistics from numerators and denominators; never sum or average stored rates.

## Reading the artifacts

| Artifact | Interpretation |
| --- | --- |
| `inputs.json`, `integrity.json` | Sides compared, attribution label, database identities, alignment digest and before/after identity check |
| `inventory.json` | Complete processed-table inventory; missing tables are explicit |
| `cohort.csv`, `cohort-year.csv` | Existing-PBP population, baseline year, event-alignment eligibility |
| `<table>/schema.json` | Original scoped schemas plus native SQLMesh schema diff over comparison projections (parquet decodes candidate ENUM types) |
| `<table>/native.json`, `columns.csv` | Native SQLMesh TableDiff counts and per-column equality |
| `<table>/changed.csv`, `added.csv`, `removed.csv` | Bounded native row samples; not full change ledgers |
| `<table>/coverage.json` | Full-cohort known totals plus exact matched-row gains, losses and changed-value counts; column additions/removals have explicit presence flags |
| `<table>/old-coverage-game.csv`, `new-coverage-game.csv` | Full-cohort per-game coverage, including edited streams |
| `<table>/player-season-count-changes.csv` | Complete player/season deltas for selected additive counts, aggregated only from baseline-PBP cohort game results; includes edited games, excludes added accounts |
| `<table>/numeric-magnitude.csv` | Exact numeric changes split into NULL transitions, absolute differences at most 1e-9, and larger changes; descriptive tolerance distinguishes tiny float drift from count changes |
| `<table>/largest-numeric-changes.csv` | Largest individual numeric changes in game/player-game results; counts and rates remain individual row values |
| `<table>/old-example-games.csv`, `new-example-games.csv` | All scoped rows for BOS197405290, ATL197905192 and PHI197109041 |
| `checkpoint.json`, `summary.json` | Per-table progress and completed result metadata |

Coverage is field-specific. Unknown, Default, `?`, empty strings and NULL are missing for descriptive values. Counts use baseball-valid bounds; attendance must be positive and a recorded fielder must be 1–9. `has_*` flags mean the flag is true, and `*_known` counters mean positive known exposure. Ordinary false-valued outcome flags remain observed values. Predicates are written into the coverage ledger. A positive error/status flag such as `has_unresolved_pitch_appearance` records positive status, not an improvement; interpret its name before calling a change a gain. Completeness analyses inherit the current model's definitions and eligibility, including cases where absent fielding credit is considered complete because no credit was required.

For event tables, `old_known`/`new_known` describe all existing-PBP games, while `gained`/`lost` describe matched rows in aligned games. The latter do not include rows appearing on only one side. Compare row counts and native added/removed results alongside coverage; a shrinking denominator can improve a percentage without recovering information. Numeric deltas identify changed results, not which source is correct.

Player-season ledgers sum available modeled counts over the fixed cohort. SQL SUM skips NULL contributions; a wholly unavailable side is displayed as zero in this delta ledger. A falling pitch sum can therefore mean that formerly populated counters became unavailable, not that the player faced fewer actual pitches. Keep NULL transitions, availability and pitch-resolution status alongside these sums. Game completeness flags require all eligible observations to satisfy the model's definition; they do not mean that the game has any recorded count or pitch information.

## Interpreting contextual estimates

Contextual expectancy estimates use their full upstream estimation populations. The unchanged win-expectancy model has no league or comparison-cohort filter; excluding Negro League games from comparison rows does not remove their contribution to that matrix. Run expectancy groups league/season but also uses its full qualifying population. Interpret these estimates separately from direct scoring and observation counts.

SQLMesh's native table comparison is documented in the [Table Diff guide](https://sqlmesh.readthedocs.io/en/stable/guides/tablediff/). This script uses the installed `TableDiff` API so schema and row findings are produced by the same engine as the project.
