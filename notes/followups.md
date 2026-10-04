# Open follow-ups

Operational items that don't block deployment but deserve a home.

## Seed corrections — 2026-09-11

**Franchise seed and game-type overrides** (the Guardians row, the 1925/1927 Memphis, 1929 American Negro League, and 1931 Louisville league spans in `seed_franchises`, and `seed_game_type_overrides` for ten 1941 to 1942 barnstorming and all-star games) landed in `bc.db` with the 2026-09-12 full rebuild.

**Two league gaps left alone:** the 1932 Washington Pilots vs Pittsburgh Crawfords game (Crawfords independent that year) and the 1946 Indianapolis Clowns vs Cincinnati Crescents game are labeled regular season with one side having no league. Neither classification is certain enough to override; report to Retrosheet or resolve with a source.

## Publish path (DuckLake)

### Site cutover

The DuckLake release publishes 169 tables and 1,605,595,144 rows with source/export row-count parity. All 696 public objects (694 Parquet files, catalog, and schema metadata) match local sizes and checksums. The site migration is squash-merged in [site PR 5](https://github.com/droher/baseball.computer.site/pull/5); production verification is recorded in `docs/ducklake-production.md`.

The legacy `/dbt/` objects and `scripts/create_web_db.py` remain available for external consumers and rollback. They are not part of the current site publication workflow. Delete them only after confirming consumers and rollback no longer need them.

### Cloudflare metadata cache exception

The active `DuckLake metadata revalidation` Cache Rule bypasses edge cache and respects origin browser TTL for DuckLake catalog and schema URLs under `/baseball/`. It overrides the old month-long cache-everything Page Rule for metadata. Preserve this exception; the origin's `max-age=0` header alone was insufficient. See `docs/ducklake-production.md` for the match and verification requirements.

### DATA_VERSION bumping

`bc/data_version.txt` controls the R2 prefix (`baseball/v<DATA_VERSION>/`). Bump on schema-breaking changes (new ENUM values are not breaking; renamed / removed columns or tables are). Old prefixes stay attachable until manually purged. No automation; bump manually as part of the change that breaks the schema.

### Per-table compression / row-group tuning

DuckLake 1.0 supports global, schema, and table-scoped options. The publisher currently uses uniform ZSTD compression, 1966080-row groups, and a 128 MB file-size target. Further per-table tuning remains optional and should follow measured browser query behavior.

### R2 upload operations

The uploader now supports four workers by default, `--workers 1..16`, transient retries, and checksum-verified `--resume`. Data files publish before schema metadata and catalog. See `docs/ducklake-production.md`.

### Incremental kinds — shelved

Decision (2026-05-03): not pursuing. Motivation was DuckLake snapshot-retention storage savings, but there is no current need to retain snapshots. Both SQLMesh (`snapshot_ttl="in 1 hour"` + janitor) and DuckLake (`expire_snapshots()` keeping the last 5) prune aggressively, so a fixed working set already bounds cost. Adding `INCREMENTAL_BY_TIME_RANGE` would add coordination complexity (interval config, late-arriving data, partition replacement) only when we want to retain N>>5 snapshots.

## Data quality

### QA-fix branch residue (2026-07-05)

The `qa-fixes` branch closed the confirmed QA findings (test hermeticity, seed franchise/pitch-type gaps, doc corrections). Left open:

- ~~**Prod restatement.**~~ Done 2026-07-05 via full `rebuild-prod` (user-chosen over a 33-model restate). First attempt surfaced a real defect in the QA seed extensions: carrying the seven Negro-league franchises' single-league rows into 1949 attributed KCM's 1949 games to NN1 (folded 1931), creating a sparse `(KAN05, 1949, NN1)` park cell at factor 10.47 that failed `calc_park_factor_trajectory_outs`'s bounded_range audit. Fixed by splitting 1949 NAL rows off the six non-NAL rows (all seven clubs played 1949 in the NAL; NN2 folded after 1948). Second rebuild passed everything except the known `model_input_fielding_credit` audit OOM, cleared by resuming the plan at `BC_DUCKDB_THREADS=4`.
- **Era-stale league codes on spanning Negro-league rows.** The single-row-per-franchise convention still attributes e.g. KCM 1937–48 (NAL years) to NN1 and CAG 1932–48 to NN1. Pre-existing, systemic, and now the only remaining league-attribution imprecision for these franchises; fixing it properly means era-splitting more rows with documented membership years.
- **Remaining NULL-league event rows.** The seed fixes cover 17,354 of the 41,738 `league_group='Other'` event rows caused by failed franchise lookups; the remaining 24,384 stem from other franchises/date gaps not in the QA findings. A full uncovered-franchise sweep (join `stg_games` to `seed_franchises` on team_id + date range) is the discovery query.
- **PH6 and CI1, 1936.** 5 game-sides each fall outside their seed rows (PH6 is 1934-only, CI1 is 1937-only). Plausibly independent-club years; a second row with empty `league` may be the right shape, but 1936 status needs historical evidence before editing.

### Partial-coverage SUMs

For pre-1900s + Negro-League seasons the per-game SUMs are biased-low (retrosheet has partial coverage; Lahman fills only when retrosheet returns NULL). Right fix needs per-stat per-row gating: choose Lahman when the per-game data has at least one NULL contributor AND Lahman's value is strictly greater than retrosheet's partial SUM. Sketched but not shipped. Gets fiddly because SQLMesh's `EXCLUDE` / `REPLACE` clauses don't expand `@EACH` macros, so the per-stat block has to be emitted via a Python-side macro returning a string (or every stat enumerated by hand). Defer until a real consumer asks.

Pre-1920 SB/CS override (`databank_running`) is preserved as a separate REPLACE — distinct semantic (override vs fill-on-NULL).

### Park-factor priors for sparse leagues

Even with `bounded_max=20`, the residual NN1/NN2 spatial-distribution outliers represent real data but extreme park factors. Bump `prior_sample_size` per-league (e.g. 5000 for NN1/NN2 vs 1000 default) to dampen further if downstream use cases need it.

## Machine learning

### Calibration pass

Single-epoch baseline produces argmax probabilities clustered around the majority-class prior (avg p ≈ 0.48 for `InPlayOut`). After more epochs, validate calibration with a reliability diagram before reporting accuracy.

### Sklearn pipeline option

A regression baseline (logistic regression with one-hot + target-encoded categoricals) would be a cheap sanity check against the Keras model. Useful when a target has too few examples to justify deep embeddings.

### `run_id` propagation into the audit

`model_run_id` is uniform per scoring run (one column value across the whole table). Adding an audit that asserts this uniformity would catch accidental multi-run mixing if the scorer is ever called more than once per `execute()`.

### Predictions parquet snapshot

If a downstream wants predictions, add `download_parquet` to the predictions `@model` and re-run publish. No code changes to consumers.

### Hamilton dependency upgrade path

`apache-hamilton` 1.90 resolves alongside SQLMesh 0.236 cleanly. Watch for sqlglot pin conflicts on future Hamilton upgrades. Same constraint story as `boring-semantic-layer` could appear.

## Audits

### Custom `relationships` audit under DEV_ONLY

Resolved 2026-05-03. The audit body's `@to_model` text substitution was replaced with a Python `@macro` `relationships_check(@column, @to_column, @to_model)` (`bc/macros/_env_to_model.py`). The macro reads `evaluator.locals['this_model']` (env-aware: under DEV_ONLY dev, schema is `sqlmesh__<canonical>`, table name is `<canonical>__<model>__<hash>__<env>`), strips the env suffix off the table name, and rewrites `to_model` to the env-suffixed view (`main_models__dev.X`). Declaring `to_model` as a real `depends_on` was rejected because the project DAG has 12 cyclic FK pairs (e.g. `main_models.people` is supplemented from `stg_box_score_*` lines that themselves FK-check `people`). The macro additionally probes the engine adapter for the rewritten target; when a transitively-referenced model hasn't materialized yet, the predicate collapses to `TRUE`, surfacing as a 0-row audit. After a full plan dev the env is complete and audits run their real predicate. Prod runs (canonical schema) are unaffected.

### `earned_runs > runs` residue, ~10 cases per modern season

The `bounded_range(earned_runs ≤ runs)` audit on `player_game_pitching_stats` (and the planned sweep onto `team_game_pitching_stats` / `player_team_season_pitching_stats`) was spec'd with a `season >= 1948` Lahman-supplement carve-out, but the audit still surfaces ~816 rows distributed evenly across 1948 → 2025 (roughly 4 – 23 per season), not the 1,031 pre-1948 Lahman residue the plan expected. Spot-check `LAN202505300` `friem001`: `stg_game_earned_runs.earned_runs = 6` against event-derived `runs = 5`. Likely cause is Retrosheet's bequeathed / inherited runner accounting in the official ER files diverging from the run-assignment logic in `event_pitching_stats.runs`: a runner who was on base when the pitcher left and later scored gets charged ER to the original pitcher, but the run-assignment logic credits the run to whichever pitcher was on the mound when it scored.

Validated 2026-05-07: every offending team-game has matching team totals (team R = team ER), with one pitcher's ER>R offset by another pitcher's R>ER on the same team. The per-pitcher invariant `earned_runs ≤ runs` does not hold by construction. ER and R follow different attribution rules. The audit was dropped from `player_game_pitching_stats`. The team-game-grain version (team R ≥ team ER, modulo Lahman-supplement era) is still candidate for `team_game_pitching_stats`; spec'd, not added in this change.

Pre-1948, the carve-out target was Lahman-supplemented ER exceeding Retrosheet partial-game R sums (separate root cause, same shape). Real fix is the per-stat per-row gate sketched under "Partial-coverage SUMs". Both eras converge once that gate exists.

### Umpire FK audits dropped — 4 unknown umpires not in `main_models.people`

Dropped 2026-05-07. The 6 `relationships(umpire_*_id → main_models.people.person_id)` audits I tried to add to `game_start_info` fail on 18 rows (7 home, 8 first, 3 third) caused by 4 umpires absent from `main_models.people`: `wasnu90`, `Gockle`, `fambu091`, `harrm201`. Two of these (`Gockle`, `wasnu90`) don't even match the standard 8-char Retrosheet person-id shape, so they look like upstream parser errors. Fix path is in `baseball.computer.rs` (or a manual people-supplement seed) so the audits land cleanly when re-added.

### Box-score within-row issues (operationalized via `box_score_data_issues`)

Resolved 2026-05-07. The 26 known box-score within-row violations (15 `hits_gt_at_bats`, 3 each `home_runs_gt_hits` / `strikeouts_gt_batters_faced`, 2 `strikeouts_gt_plate_appearances`, and one each of `hits_gt_batters_faced` / `extra_base_hits_gt_hits` / `earned_runs_gt_runs`, all 1899 – 1948 box scores) are now enumerated by `main_models.box_score_data_issues`. The new `bounded_excluding_data_issues` audit lets game-grain stat models add the same definitional bound checks while carving out the listed rows, so audits land cleanly today and any *new* violation introduced post-staging fails the build. Fix path for the 26 rows is still in the parser at [baseball.computer.rs](https://github.com/droher/baseball.computer.rs) or a manual override seed. Once those land, drop them from `box_score_data_issues` and the audits tighten automatically.

### Scratched starting pitchers (operationalized via `team_game_data_issues`)

Resolved 2026-05-07. 59 PlayByPlay-source team-games where `game_start_info` records a starting pitcher who never threw a pitch (scratched at the last minute, still the SP per MLB rules) are enumerated by `main_models.team_game_data_issues` with `issue_type = 'starting_pitcher_no_appearance'`. The `team_game_has_one_starter` audit and the `bounded_range(complete_games, 0, games_started)` audit on `player_game_pitching_stats` consume the carve-out via `@team_game_data_issue_match`. The 5 pitchers with `CG=1, GS=0` are the relievers who covered all 27 outs after the SP was scratched (Ernie Shore-style); they sit naturally inside the carve-out. New scratched-SP cases are picked up by the issues model on each plan; new audit failures outside the listed team-games will fail the build.

## Tests

### `bc/tests/test_bsl_semantic.py`

Runs only under the `bsl` uv group (the build env can't import BSL because xorq pins sqlglot <28). pytest collects-and-skips cleanly under the build env via `pytest.importorskip`.

## Synthetic box scores

### Date-independent metric (Round 2 Idea A) — date allocation dominates

`scripts/backtest_synthetic_lineups.py` now reports two date-independent recall rates next to the headline `wrong_starters_per_game`:

- `set_miss_rate = 1 − Σ_p min(syn_starts, real_starts) / Σ_p real_starts`
- `pos_set_miss_rate` = same on (player, fielding_position) buckets.

Pitcher rows are dropped on the (syn_pos, real_pos) axis, not on the season-player axis, so a two-way player still scores on his non-P bucket.

Full 1871 – 1910 numbers (post-Round-1):

- `wrong_starters_per_game = 3.999`
- `set_miss_rate = 1.43%`
- `pos_set_miss_rate = 1.91%`

Per-bucket decomposition:

| churn | wrong_starters_per_game | set_miss_rate_pct | pos_set_miss_rate_pct |
|---|---|---|---|
| multi-stint | 0.421 | 1.5 | 2.12 |
| single-stint full | 0.725 | 1.52 | 1.89 |
| single-stint partial | 2.852 | 1.29 | 1.9 |

Diagnosis: player selection is essentially right (`set_miss` flat at ~1.3 – 1.5% across all buckets); the dominant remaining error is which date the optimizer assigns each chosen player to. The single-stint partial bucket carries 2.85 of the 4.00 headline wrong-starters with the lowest set_miss — pure date-axis error.

Round 2 sequence still calls for shipping Idea B (retire modal prior) behind a flag and re-judging; given how flat set_miss is, B is unlikely to move it materially, in which case the remaining ceiling is structural and the next moves are D1 (bench-game accounting), D2 (catcher pair detection), D3 (rest-day priors).

### Per-position fair-share scaling already implemented

Step 3 of the synthetic-lineup-algorithm-improvements plan proposes scaling `position_target` down by `team_games_F / sum_p games_at_position[p, F]` and using `sum_F games_at_position[p]` as the total target. Both already exist in `game_lineups.py`: `_scale_position_targets` (called at line 346) does the position scaling, and `_build_milp_problem` already derives `total_targets` from `sum_F non_pitcher_fielding` over the scaled candidates. No code change available.

The remaining over-allocation (e.g. `raubt101` 1903 CHN: syn=26, real=15) is driven by individual fielding-position appearance counts that exceed real starts (Lahman `fielding.g` includes relief / defensive subs), not by team-level position over-count. Closing it needs a real-vs-appearance signal. `Appearances.GS` would do it but is NULL for non-pitchers pre-1904. No clean fix without new data.

### Catcher wrong-defender rate (1.07 / game)

Tried relaxing the MILP `default_slot` exclusion at `slot.fielding_position == 2` so any C-eligible candidate could occupy the C slot regardless of modal-lineup status (proposed as Step 2 of the synthetic-lineup-algorithm-improvements plan). It is a no-op: the existing predicate `default_slot != lineup_position AND fielding_position != slot.fielding_position` already lets backup catchers and dual-eligibility modal players into the C slot via the second clause, and the modal-default bonus (0.01) is two orders of magnitude smaller than the position-target slack penalty (1.0) so it can't be dominating.

The high C error rate is a date-allocation problem, not a slot-eligibility one. Per-game C choice between the modal C and the backup C has no signal beyond starting pitcher and DH, so the MILP's date assignment within the season-long position target is effectively arbitrary. Real fixes would need a per-game C signal (e.g. caught-stealing / pitch-framing prior tied to the gamelog's starting pitcher) or transaction-driven stint windows (Step 4 of that plan).

### Modal-prior retire flag (Round 2 Idea B) — confirmed no-op

`build_synthetic_lineup_assignments(disable_modal=True)` (also `BC_OPTIMIZER_NO_MODAL=1`, also `--disable-modal` on the backtest CLI) drops the modal-default cost bonus and the `default_slot` slot-pin from the eligibility predicate, then assigns `lineup_position` post-hoc per game-side by ranking non-pitcher starters by season PA/G (pitcher pinned at 9, pre-1973 NL no-DH convention).

Full 1871 – 1910 backtest, modal-on vs modal-off:

| metric | modal-on | modal-off | delta |
|---|---|---|---|
| wrong_starters_per_game | 3.999 | 3.996 | -0.003 |
| wrong_positions_per_game | 1.449 | 1.453 | +0.004 |
| set_miss_rate | 1.43% | 1.43% | 0.00 |
| pos_set_miss_rate | 1.91% | 1.91% | 0.00 |
| C wrong_per_game | 1.072 | 1.072 | 0.00 |

Every delta sits inside the MILP tiebreak noise floor. The modal prior is dead weight at the metric level. Keeping it costs ~0.01% in cost bonus that the position-target slack penalty (1.0) overrides. The flag ships as a diagnostic; we don't delete the modal path since `compute_modal_lineups` is also exported and used as the no-optimizer fallback for orphan team-seasons and fallback infeasible sides. A future cleanup commit could drop the modal-bonus cost term unconditionally without removing the modal lineup.

The full 1871 – 1910 result confirms the Round 2 Idea A diagnosis: the remaining ~4 wrong starters / game is dominated by date-allocation error, not player-set or position-set selection. The structural ceiling is reached without external data (per-game C signal, scheduled-rest priors, or transaction logs beyond what tranDB provides). Round 2 Ideas C, D1, D2, D3 are deferred — see plan for the full menu.

The `synthetic_box_score.*` schema fills lineup skeletons for the ~25K games that exist only in `misc.gamelog` (mostly pre-1901 MLB, plus a few NLB cases). Game-level metadata, default seasonal lineups, optimized non-pitcher starters, listed starting pitchers, and parsed line scores ship today; the items below extend the coverage.

### Event-shaped synthetic tables

Out of scope for the initial cut. The gamelog gives no per-event signal, so HBP / HR / SB / CS / DP / TP / comments / pinch_* / team_*_lines tables stay unwritten. Fabricating per-PA outcomes is a separate decision (statistical priors conditioned on park / season / batter and pitcher) and not under consideration yet.

### Project gamelog winning / losing / save pitcher

`misc.gamelog` carries `winning_pitcher`, `losing_pitcher`, `save_pitcher`, and `game_winning_rbi` columns that `stg_gamelog.sql` does not currently project. Once those columns land in staging, populate them on `synthetic_box_score.box_score_games` and remove their NULL stubs from the column list.

### Fold synthetic rows into season-level stats

`player_team_season_offense_stats` and `player_position_team_season_fielding_stats` currently treat gamelog-only games as silently missing. Plumbing the synthetic lineups in would credit each modal regular with one game per gamelog-only game, but since stat columns stay NULL, the season totals would not move. Decide whether the model layer should prefer "real but missing" over "synthetic but inferred" before touching the existing season models.

### NLB coverage gap

Team-seasons with no `baseballdatabank.appearances` rows are dropped silently by `team_season_modal_lineups`. For Negro-League seasons in scope, this means a gamelog game with no synthetic shell. Either log the dropped team-seasons prominently or attempt a roster-only fallback (use `misc.roster` to pick nine players, without the modal-fielder ranking).

### Pre-1973 DH assumption

`stg_databank_appearances` skips the `g_dh` column on the assumption that every gamelog-only game in scope is pre-DH. If a gamelog-only DH game ever surfaces, restore `g_dh` in the pivot (maps to `fielding_position = 10`) and teach the modal-lineup picker how to handle the DH slot.

### Lahman/Databank ↔ Retrosheet team_id mismatches

Resolved. The four Databank stagings (`stg_databank_appearances`, `stg_databank_batting`, `stg_databank_fielding`, `stg_databank_pitching`) now translate `team_id` via `baseballdatabank.teams.team_id_retro` (joined on `(year_id, team_id)`) and project `team_id_retro` directly with no fallback. The `not_null(team_id)` audit on each staging fails the build loudly if a row ever lacks a crosswalk match.
