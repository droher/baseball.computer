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

### `earned_runs > runs` residue, ~10 cases per modern season

The `bounded_range(earned_runs ≤ runs)` audit on
`player_game_pitching_stats` (and the planned sweep onto
`team_game_pitching_stats` / `player_team_season_pitching_stats`) was
spec'd with a `season >= 1948` Lahman-supplement carve-out, but the
audit still surfaces ~816 rows distributed evenly across 1948→2025
(roughly 4–23 per season) — not the 1,031 pre-1948 Lahman residue
the plan expected. Spot-check `LAN202505300` `friem001`:
`stg_game_earned_runs.earned_runs = 6` against event-derived
`runs = 5`. The likely cause is Retrosheet's bequeathed/inherited
runner accounting in the official ER files diverging from the run-
assignment logic in `event_pitching_stats.runs`: a runner who was
on base when the pitcher left and later scored gets charged ER to
the original pitcher, but the run-assignment logic credits the run
to whichever pitcher was on the mound when it scored.

Validated 2026-05-07: every offending team-game has matching team
totals (team R = team ER), with one pitcher's ER>R offset by another
pitcher's R>ER on the same team. The per-pitcher invariant
`earned_runs ≤ runs` simply doesn't hold by construction — ER and R
follow different attribution rules. The audit was dropped from
`player_game_pitching_stats`. The team-game-grain version (team R ≥
team ER, modulo Lahman-supplement era) is still candidate for
`team_game_pitching_stats`; spec'd, not added in this change.

Pre-1948, the carve-out target was Lahman-supplemented ER exceeding
Retrosheet partial-game R sums — separate root cause, same shape.
Real fix is the per-stat per-row gate sketched under "Partial-
coverage SUMs". Both eras converge once that gate exists.

### Umpire FK audits dropped — 4 unknown umpires not in `main_models.people`

Dropped 2026-05-07. The 6 `relationships(umpire_*_id → main_models.people.person_id)` audits I tried to add to `game_start_info` fail on 18 rows (7 home, 8 first, 3 third) caused by 4 umpires absent from `main_models.people`: `wasnu90`, `Gockle`, `fambu091`, `harrm201`. Two of these (`Gockle`, `wasnu90`) don't even match the standard 8-char Retrosheet person-id shape, so they look like upstream parser errors. Fix path is in `baseball.computer.rs` (or a manual people-supplement seed) so the audits land cleanly when re-added.

### Box-score within-row issues (operationalized via `box_score_data_issues`)

Resolved 2026-05-07. The 26 known box-score within-row violations
(15 `hits_gt_at_bats`, 3 each `home_runs_gt_hits` / `strikeouts_gt_batters_faced`,
2 `strikeouts_gt_plate_appearances`, and one each of
`hits_gt_batters_faced` / `extra_base_hits_gt_hits` / `earned_runs_gt_runs`,
all 1899–1948 box scores) are now enumerated by
`main_models.box_score_data_issues`. The new
`bounded_excluding_data_issues` audit lets game-grain stat models
add the same definitional bound checks while carving out the listed
rows, so audits land cleanly today and any *new* violation
introduced post-staging fails the build. Fix path for the 26 rows
themselves is still in the parser at
[baseball.computer.rs](https://github.com/droher/baseball.computer.rs)
or a manual override seed; once those land, drop them from
`box_score_data_issues` and the audits tighten automatically.

### Scratched starting pitchers (operationalized via `team_game_data_issues`)

Resolved 2026-05-07. 59 PlayByPlay-source team-games where `game_start_info` records a starting pitcher who never threw a pitch (scratched at the last minute, still the SP per MLB rules) are enumerated by `main_models.team_game_data_issues` with `issue_type = 'starting_pitcher_no_appearance'`. The `team_game_has_one_starter` audit and the `bounded_range(complete_games, 0, games_started)` audit on `player_game_pitching_stats` consume the carve-out via `@team_game_data_issue_match`. The 5 pitchers with `CG=1, GS=0` are the relievers who covered all 27 outs after the SP was scratched (Ernie Shore-style); they sit naturally inside the carve-out. New scratched-SP cases are picked up by the issues model on each plan; new audit failures outside the listed team-games will fail the build.

## Tests

### `bc/tests/test_bsl_semantic.py`

Runs only under the `bsl` uv group (the build env can't
import BSL because xorq pins sqlglot <28). pytest collects-and-skips
cleanly under the build env via `pytest.importorskip`.

## Synthetic box scores

### Date-independent metric (Round 2 Idea A) — date allocation dominates

`scripts/backtest_synthetic_lineups.py` now reports two date-independent
recall rates next to the headline `wrong_starters_per_game`:

- `set_miss_rate = 1 − Σ_p min(syn_starts, real_starts) / Σ_p real_starts`
- `pos_set_miss_rate` = same on (player, fielding_position) buckets.

Pitcher rows are dropped on the (syn_pos, real_pos) axis, not on the
season-player axis, so a two-way player still scores on his non-P bucket.

Full 1871-1910 numbers (post-Round-1):

- `wrong_starters_per_game = 3.999`
- `set_miss_rate = 1.43%`
- `pos_set_miss_rate = 1.91%`

Per-bucket decomposition:

| churn | wrong_starters_per_game | set_miss_rate_pct | pos_set_miss_rate_pct |
|---|---|---|---|
| multi-stint | 0.421 | 1.5 | 2.12 |
| single-stint full | 0.725 | 1.52 | 1.89 |
| single-stint partial | 2.852 | 1.29 | 1.9 |

Diagnosis: player selection is essentially right (set_miss flat at
~1.3-1.5% across all buckets); the dominant remaining error is which
date the optimizer assigns each chosen player to. The single-stint
partial bucket carries 2.85 of the 4.00 headline wrong-starters with
the *lowest* set_miss — pure date-axis error.

Round 2 sequence still calls for shipping Idea B (retire modal prior)
behind a flag and re-judging; given how flat set_miss is, B is unlikely
to move it materially, in which case the remaining ceiling is structural
and the next moves are D1 (bench-game accounting), D2 (catcher pair
detection), D3 (rest-day priors).

### Per-position fair-share scaling already implemented

Step 3 of the synthetic-lineup-algorithm-improvements plan proposes
scaling `position_target` down by `team_games_F / sum_p games_at_position[p, F]`
and using `sum_F games_at_position[p]` as the total target. Both already
exist in `game_lineups.py` — `_scale_position_targets` (called at line
346) does the position scaling, and `_build_milp_problem` already
derives `total_targets` from `sum_F non_pitcher_fielding` over the
scaled candidates. No code change available.

The remaining over-allocation (e.g. raubt101 1903 CHN: syn=26, real=15)
is driven by individual fielding-position appearance counts that exceed
real starts (Lahman `fielding.g` includes relief / defensive subs),
not by team-level position over-count. Closing it needs a real-vs-
appearance signal — `Appearances.GS` would do it but is NULL for
non-pitchers pre-1904. No clean fix without new data.

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

### Modal-prior retire flag (Round 2 Idea B) — confirmed no-op

`build_synthetic_lineup_assignments(disable_modal=True)` (also
`BC_OPTIMIZER_NO_MODAL=1`, also `--disable-modal` on the backtest CLI)
drops the modal-default cost bonus and the `default_slot` slot-pin from
the eligibility predicate, then assigns `lineup_position` post-hoc per
game-side by ranking non-pitcher starters by season PA/G (pitcher pinned
at 9, pre-1973 NL no-DH convention).

Full 1871-1910 backtest, modal-on vs modal-off:

| metric | modal-on | modal-off | delta |
|---|---|---|---|
| wrong_starters_per_game | 3.999 | 3.996 | -0.003 |
| wrong_positions_per_game | 1.449 | 1.453 | +0.004 |
| set_miss_rate | 1.43% | 1.43% | 0.00 |
| pos_set_miss_rate | 1.91% | 1.91% | 0.00 |
| C wrong_per_game | 1.072 | 1.072 | 0.00 |

Every delta sits inside the MILP tiebreak noise floor. The modal prior
is dead weight at the metric level — keeping it costs ~0.01% in cost
bonus that the position-target slack penalty (1.0) overrides. The flag
ships as a diagnostic; we don't delete the modal path since
`compute_modal_lineups` is also exported and used as the no-optimizer
fallback for orphan team-seasons and fallback infeasible sides. A future
cleanup commit could drop the modal-bonus cost term unconditionally
without removing the modal lineup itself.

The full 1871-1910 result confirms the Round 2 Idea A diagnosis: the
remaining ~4 wrong starters / game is dominated by date-allocation
error, not player-set or position-set selection. The structural ceiling
is reached without external data (per-game C signal, scheduled-rest
priors, or transaction logs beyond what tranDB provides). Round 2 Ideas
C, D1, D2, D3 are deferred — see plan for the full menu.

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
