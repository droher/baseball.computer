# Open follow-ups

Operational items that don't block deployment but deserve a home.

## Publish path (DuckLake)

### Site cutover

Deferred until the site team confirms parity on a test branch. Specifically:

- **Query parity** — for a representative sample of site queries, rows
  and values match between
  `ATTACH 'https://data.baseball.computer/baseball/v1/baseball.ducklake' (TYPE ducklake, READ_ONLY)`
  and the existing `ATTACH 'https://.../dbt/bc_remote.db'`.
- **Cold-attach latency** — single catalog fetch + lazy parquet reads
  acceptable vs the current single-DB-file fetch. Worth measuring on
  the site's actual edge.
- **VARCHAR-not-ENUM acceptable** — site code that filters / joins on
  ENUM columns (`event_type`, `park_id`, etc.) keeps working with
  VARCHAR semantics. DuckLake v1.0 stores ENUMs as VARCHAR; the publish
  script does the cast explicitly so column metadata reflects reality.
- **LLM-metadata bridge** is either ready to consume the DuckLake table
  layout, or works against both artifacts.

When cutover lands:

1. Delete `scripts/create_web_db.py`.
2. Stop publishing the `dbt/` R2 prefix (leave a grace window for any
   external consumer pinned to it).
3. Update `README.md`, `CLAUDE.md`, and the site's data-access docs to
   reference only the DuckLake URL.
4. After the grace window, purge the `dbt/` R2 prefix.

### Cloudflare cache-purge prerequisite

`scripts/upload_ducklake.py` requires `CLOUDFLARE_API_TOKEN` and
`CLOUDFLARE_ZONE_ID` env vars at upload time. Token needs Zone:Cache
Purge scope on the `data.baseball.computer` zone.

### DATA_VERSION bumping

`bc/data_version.txt` controls the R2 prefix
(`baseball/v<DATA_VERSION>/`). Bump on schema-breaking changes (new
ENUM values are not breaking — they reach consumers as new VARCHAR
values; renamed/removed columns or tables are). Old prefixes stay
attachable until manually purged. No automation around this — bump
manually as part of the change that breaks the schema.

### Per-table compression / row-group settings

`scripts/create_web_db.py` writes `event_states_full` at
`COMPRESSION GZIP, ROW_GROUP_SIZE 262144` and everything else at
`ZSTD, ROW_GROUP_SIZE 1966080`. DuckLake exposes
`parquet_compression` / `parquet_row_group_size` only as catalog-wide
options (`ducklake_set_option`), not per-table. The publish script
sets them catalog-wide to ZSTD + 1966080, so `event_states_full`
doesn't get its tuned settings in the DuckLake artifact. Workarounds
when DuckLake adds richer write options:

- Per-table options at the DuckLake spec level.
- COPY-then-`ducklake_add_data_files` (write parquet with desired
  knobs, register the file as a DuckLake data file manifest entry —
  bypasses normal commits).

### R2 / Cloudflare upload concurrency

`upload_ducklake.py` uploads files sequentially through boto3. Fine
for the catalog file, but the data dir is many parquet files. If
upload time becomes the bottleneck, switch to
`concurrent.futures.ThreadPoolExecutor` around `client.upload_file`.

### Incremental kinds — shelved

Decision (2026-05-03): not pursuing. Motivation was DuckLake
snapshot-retention storage savings, but no current need to retain
snapshots — both SQLMesh (`snapshot_ttl="in 1 hour"` + janitor) and
DuckLake (`expire_snapshots()` keeping the last 5) prune aggressively,
so a fixed working set already bounds cost. Adding
`INCREMENTAL_BY_TIME_RANGE` would add coordination complexity
(interval config, late-arriving data, partition replacement on the
publish side) without paying off until we want long history. Revisit
only when we want to retain N>>5 snapshots.

## Data quality

### Partial-coverage SUMs

For pre-1900s + Negro-League seasons the per-game SUMs are
biased-low (retrosheet has partial coverage; Lahman fills only when
retrosheet returns NULL). Right fix needs per-stat per-row gating:
choose Lahman when the per-game data has at least one NULL contributor
AND Lahman's value is strictly greater than retrosheet's partial SUM.
Sketched but not shipped — gets fiddly because SQLMesh's
`EXCLUDE`/`REPLACE` clauses don't expand `@EACH` macros, so the
per-stat block has to be emitted via a Python-side macro returning a
string (or every stat enumerated by hand). Defer until a real consumer
asks.

Pre-1920 SB/CS override (databank_running) preserved as a separate
REPLACE — distinct semantic (override vs fill-on-NULL).

### Park-factor priors for sparse leagues

Even with `bounded_max=20`, the residual NN1/NN2 spatial-distribution
outliers represent real data but extreme park factors. Could bump
`prior_sample_size` per-league (e.g. 5000 for NN1/NN2 vs 1000 default)
to dampen further if downstream use cases need it.

## Machine learning

### Artifact backfill

Six third-wave targets shipped code + tests but their `predictions_*`
`@model`s gate on `python_models.ml.artifact_exists(target)` —
`enabled=False` until the pin JSON lands. Run the matching
`scripts/train_<name>.py --epochs 1 --rows-per-batch 100000` once each
to land the artifact JSONs:

- `outcome_baserunning_cat`
- `outcome_batted_location_cat`
- `outcome_batted_trajectory_cat`
- `outcome_has_batting_bin`
- `outcome_is_win_bin`
- `outcome_runs_following_num`

`outcome_baserunning_cat` left with `filter_zero_weight=False` since
it has a meaningful `'Other'` label for non-baserunning events; flip
the flag if downstream metrics get noisy on plate-appearance rows.

### MLflow → R2 artifact upload

Deferred until multi-target. When multiple models need to be loaded
by the prediction `@model`, push fitted-model artifacts to R2 and
have `load_scorer` fetch by run_id.

### Calibration pass

Single-epoch baseline produces argmax probabilities clustered around
the majority-class prior (avg p ≈ 0.48 for `InPlayOut`). After more
epochs, validate calibration with a reliability diagram before
reporting accuracy.

### Sklearn pipeline option

A regression baseline (logistic regression with one-hot + target-
encoded categoricals) would be a cheap sanity check against the Keras
model — useful when a target has too few examples to justify deep
embeddings.

### `run_id` propagation into the audit

`model_run_id` is uniform per scoring run (one column value across
the whole table). Adding an audit that asserts this uniformity would
catch accidental multi-run mixing if the scorer is ever called more
than once per `execute()`.

### Predictions parquet snapshot

If a downstream wants predictions, add `download_parquet` to the
predictions `@model` and re-run publish. No code changes to
consumers.

### Hamilton dependency upgrade path

`apache-hamilton` 1.90 resolves alongside SQLMesh 0.234 cleanly. Watch
for sqlglot pin conflicts on future Hamilton upgrades — the same
constraint story as `boring-semantic-layer` could appear.

## Audits

### Custom `relationships` audit under DEV_ONLY

Resolved 2026-05-03. The audit body's `@to_model` text substitution
was replaced with a Python `@macro` `relationships_check(@column,
@to_column, @to_model)` (`bc/macros/_env_to_model.py`). The macro
reads `evaluator.locals['this_model']` (env-aware: under DEV_ONLY
dev, schema is `sqlmesh__<canonical>`, table name is
`<canonical>__<model>__<hash>__<env>`), strips the env suffix off
the table name, and rewrites `to_model` to the env-suffixed view
(`main_models__dev.X`). Declaring `to_model` as a real `depends_on`
was rejected because the project DAG has 12 cyclic FK pairs (e.g.
`main_models.people` is supplemented from `stg_box_score_*` lines
that themselves FK-check `people`). The macro additionally probes
the engine adapter for the rewritten target — when a transitively-
referenced model hasn't materialized yet, the predicate collapses to
`TRUE`, surfacing as a 0-row audit. After a full plan dev the env is
complete and audits run their real predicate. Prod runs (canonical
schema) are unaffected.

## Tests

### `bc/tests/test_bsl_semantic.py`

Runs only under the `bsl` uv group (the build env can't
import BSL because xorq pins sqlglot <28). pytest collects-and-skips
cleanly under the build env via `pytest.importorskip`.

## Synthetic box scores

### Catcher wrong-defender rate (1.07 / game)

Tried relaxing the MILP `default_slot` exclusion at `slot.fielding_position
== 2` so any C-eligible candidate could occupy the C slot regardless of
modal-lineup status (proposed as Step 2 of the synthetic-lineup-algorithm-
improvements plan). It is a no-op: the existing predicate
`default_slot != lineup_position AND fielding_position != slot.fielding_
position` already lets backup catchers and dual-eligibility modal players
into the C slot via the second clause, and the modal-default bonus (0.01)
is two orders of magnitude smaller than the position-target slack penalty
(1.0) so it can't be dominating.

The high C error rate is a date-allocation problem, not a slot-eligibility
one. Per-game C choice between the modal C and the backup C has no signal
beyond starting pitcher and DH, so the MILP's date assignment within the
season-long position target is effectively arbitrary. Real fixes would
need a per-game C signal (e.g. caught-stealing/pitch-framing prior tied
to the gamelog's starting pitcher) or transaction-driven stint windows
(Step 4 of that plan).

The `synthetic_box_score.*` schema fills lineup skeletons for the
~25K games that exist only in `misc.gamelog` (mostly pre-1901 MLB,
plus a few NLB cases). Game-level metadata, default
seasonal lineups, optimized non-pitcher starters, listed starting
pitchers, and parsed line scores ship today; the items below extend
the coverage.

### Event-shaped synthetic tables

Out of scope for the initial cut. The gamelog gives no per-event
signal, so HBP / HR / SB / CS / DP / TP / comments / pinch_* /
team_*_lines tables stay unwritten. Fabricating per-PA outcomes is
a separate decision (statistical priors conditioned on park /
season / batter and pitcher) and not under consideration yet.

### Project gamelog winning / losing / save pitcher

`misc.gamelog` carries `winning_pitcher`, `losing_pitcher`,
`save_pitcher`, and `game_winning_rbi` columns that
`stg_gamelog.sql` does not currently project. Once those columns
land in staging, populate them on
`synthetic_box_score.box_score_games` and remove their NULL stubs
from the column list.

### Fold synthetic rows into season-level stats

`player_team_season_offense_stats` and
`player_position_team_season_fielding_stats` currently treat
gamelog-only games as silently missing. Plumbing the synthetic
lineups in would credit each modal regular with one game per
gamelog-only game — but since stat columns stay NULL, the season
totals would not move. Decide whether the model layer should prefer
"real but missing" over "synthetic but inferred" before touching
the existing season models.

### NLB coverage gap

Team-seasons with no `baseballdatabank.appearances` rows are
dropped silently by `team_season_modal_lineups`. For Negro-League
seasons in scope, this means a gamelog game with no synthetic
shell. Either log the dropped team-seasons prominently or attempt
a roster-only fallback (use `misc.roster` to pick nine players,
without the modal-fielder ranking).

### Pre-1973 DH assumption

`stg_databank_appearances` skips the `g_dh` column on the
assumption that every gamelog-only game in scope is pre-DH. If a
gamelog-only DH game ever surfaces, restore `g_dh` in the pivot
(maps to `fielding_position = 10`) and teach the modal-lineup
picker how to handle the DH slot.

### Lahman/Databank ↔ Retrosheet team_id mismatches

Resolved. The four Databank stagings
(`stg_databank_appearances`, `stg_databank_batting`,
`stg_databank_fielding`, `stg_databank_pitching`) now translate
`team_id` via `baseballdatabank.teams.team_id_retro` (joined on
`(year_id, team_id)`) and project `team_id_retro` directly with no
fallback. The `not_null(team_id)` audit on each staging fails the
build loudly if a row ever lacks a crosswalk match.
