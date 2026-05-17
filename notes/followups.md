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

### Phase-3 v6 rearchitecture follow-ups

**Status: in flight on `phase3-rearch-v6` branch (supersedes v2/v3/v4/v5).**
v6 lands (a) `validate_pre_event` deny-list on every supplement
layout, (b) leak-input strip from pitch_summary (`result_family`) and
fielding_credit (`gap_class`, `fielding_evidence_status`), (c) 11
non-redundant pretrain heads (drops `result_family` and `hit_or_out` —
both deterministic from `pa_result`; remaining heads still correlate),
(d) the v5-A8 winning config retained (split optimizers, embed LR
5e-3, single-stage joint fit with stock val_loss EarlyStopping), (e)
new `advancement_r1/_r2/_r3` deep specs registered against
`model_input_advancement`, (f) `--time-forward` flag on
`scripts/permutation_importance_generic.py` so gate eval matches the
production temporal slice.

Open items the v6 PR does not fix:

- **`model_input_advancement` SQL gaps.** The dataset does not yet
  emit the dependent variable `advancement_class` or a
  `time_forward_fold` column. Specs in `targets/advancement.py` no
  longer register on import — call `_register()` explicitly once the
  SQL gap closes. Apply the same 7-class derivation logic the pretrain
  heads use in `model_input_event_universe.sql` (`r1/r2/r3_advancement`
  pivots). Also restore the registry-dependent tests in
  `test_advancement_targets.py` (`test_advancement_layout_registered`,
  `test_specs_publish_to_advancement_manifest`,
  `test_specs_resolve_via_get_target`).
- **Pretrain emit: persist + load stage-1 vocab.json.**
  `scripts/pretrain_emit_offsets.py` currently re-derives vocabularies
  from the dataset via `_collect_input_stats`. If the dataset parquet
  is regenerated or `BC_PRETRAIN_DATASET_LIMIT` changes between stage-1
  fit and emit, embeddings silently map to wrong tokens. Fix: load
  stage-1 vocab.json (already persisted by `pretrain/artifacts.py`) and
  fail loud if any TRAIN token is missing.
- **Promote pretrain encoding helpers to public API.**
  `scripts/pretrain_emit_offsets.py` reaches into module-private
  `_apply_all_remaps` / `_collect_input_stats` / `_encode_inputs` in
  `bc/python_models/statistical/deep/pretrain/training.py`. Either
  promote those to a public `pretrain/encoding.py` module or move the
  emitter inside the package.
- **Advancement r2/r3 home-plate fallback.** The runners_pivot CASE
  in `model_input_event_universe.sql` and the future
  `advancement_class` derivation in `model_input_advancement.sql`
  rely on `run_scored_flag` for the `Scored` arm. Verify upstream
  `stg_event_baserunners.run_scored_flag` is non-NULL whenever
  `base_end = 'Home'`; if not, add a `base_end = 'Home'` fallback.
- **Park-factors / run-values DL specs deferred.** Per
  `04-deep-learning-supplements.md`, park factors and run values are
  hierarchical-Bayes territory in Phase 4, not Phase-3 DL targets.
  Skipped intentionally.
- **Bootstrap dev DB before next `just plan` on this branch.** The
  current branch env has no `main_models` snapshot because no upstream
  models have been planned yet against `phase3_rearch_v6`. v6 reused
  the existing `phase3-pretrain-v2-prep` dataset (event_universe SQL
  diff for `time_forward_fold` is dormant). Run `just bootstrap-dev`
  + `just plan` before any new dataset prep on this branch.
- **`bc/python_models/statistical/deep/feature_layout.validate_pre_event`
  deny-list maintenance.** Add new suffix/prefix patterns whenever a
  new dataset surfaces a leak shape not covered by the deny-list. The
  test in `bc/tests/statistical/deep/test_pre_event_layout.py` is
  parametrized — add the new column there too.

#### Historical (Pretrain v2 design tweaks, shipped)

v2 landed the shared 13-slot player Embedding (single `embed_player`),
13 pretext heads, UW loss + split optimizers, two-stage schedule,
hard-head EarlyStopping, `SlashLineProbe` callback, `SpatialDropout1D(0.1)`
+ trunk `LayerNormalization`, `EMBEDDING_L2=1e-7`. v6 retains the v5-A8
single-stage config and reduces v4's 13 heads to 11 non-redundant heads.

Original v1→v2 notes (now historical):

- Drop `runners_count_start` from inputs — derivable from `base_state_start` (8-class).
- Drop `leverage_index` — pure function of `score_margin`, `inning_start`, `outs_start`, `base_state_start`, all already inputs.
- Add separate pretext heads for `batted_location` (general/depth/edge dims), `batted_contact_strength`, `batted_to_fielder` (10-class fielder position 0-9). These are NULL-when-non-batted; NULL-mask handles it. Reason: per-target downstream Phase-4 models predict these so the shared embedding should capture them.
- Add low-card game-context inputs (drive through `event_observation_context` if missing): day/night flag, `doubleheader_status`, `precipitation`, `sky`, `wind_direction`, `field_condition`.
- Add numeric weather: `temperature_fahrenheit`. **Exclude attendance** (confounded with score/team-performance — not a leak-safe context feature).
- Add `day_of_year` integer as a numeric input.
- Add fielder + runner embeddings via **shared player Embedding (option B)** — one `Embedding(num_players, dim)` matrix, looked up from 12 per-event high-card cols: `batter`, `pitcher` (the pitcher already covers defensive `fielder_pos_1`, same player_id, same row), `fielder_pos_2..9` (C/1B/2B/3B/SS/LF/CF/RF = 8 cols), `runner_on_1b`, `runner_on_2b`, `runner_on_3b`. Runners NULL when base unoccupied → OOV slot. Plus a per-slot one-hot role tag so model knows which seat each player occupies. Gives cross-role transfer: a player's defensive gradient updates the same row their batting / baserunning gradients update.
- Skip per-position credit-prediction pretext head. Without batted-ball context (location / trajectory / strength) the head learns positional priors not skill; with batted-ball context it leaks into the trajectory head (inputs are shared across heads). Defensive + baserunning signal still flows into the shared embedding via `hit_or_out` / `outs_on_play` / `runs_on_play` / `pa_result` heads — every outcome head conditions on the full 13-slot player roster.
- Implementation: `_build_backbone` gets a `shared_embedding_groups: tuple[tuple[str, ...], ...]` arg. Each tuple = cols sharing one `Embedding`. Pretrain layout passes the 12-col shared player group plus standalone `park_id` / `scorer`. `set_pretrained_embeddings` learns shared-group semantics so per-target downstream models (which only consume a subset of player slots, e.g. trajectory uses batter + pitcher) load aligned rows from the shared matrix automatically.
- Add 3 per-runner-slot advancement pretext heads sourced from `stg_event_baserunners` (one row per `(event_key, baserunner)`): `r1_advancement`, `r2_advancement`, `r3_advancement`. Multiclass over advancement-outcome enum derived from `(base_start, base_end, is_out, baserunning_play_type)` — roughly Stayed / Advanced1 / Advanced2 / Scored / OutAdvancing / OutCaughtStealing / OutPickoff. NULL-mask when base unoccupied. Forces shared player Embedding rows to absorb baserunning skill (speed, read, judgment) — not just "did they score" but the full per-slot outcome. Skip a batter_advancement head — `pa_result` already covers batter outcome.

**v2 training-schedule changes (from web research on multi-task pretrain + long-tail embedding pretrain, 2024–2026 literature):**

1. **Separate optimizer for sparse embeddings.** Trunk LR 1e-3 stays, embedding-layer LR 5e-3 to 1e-2 (5–10× higher). Rationale: Adam moment for rare rows accumulates slowly; initial step scale needs to be larger because each row sees only a handful of gradients per epoch. Linear warmup over ~5–10% of total steps before cosine decay; bump CosineDecay alpha from 0.01 → **0.1** so the floor doesn't kill rare-row updates late in training. Implementation: split into two `Adam` instances over disjoint `trainable_variables` partitions via a custom `train_step`.
2. **Uncertainty-weighted multi-task loss (Kendall & Gal 2018).** Replace `loss_weights={head: 1.0}` with one learnable `log_sigma` per head; loss = Σ (1/(2σ_h²)) L_h + log σ_h. One `self.add_weight` per head + custom `train_step`. Directly addresses easy-head gradient dominance (v1's `runs_on_play` loss 0.24 vs `pa_result` 0.97 — equal weights starve the hard heads).
3. **Batch size 1024–2048** (down from 4096). At bs=4096 a rare entity (~200 appearances) sees ~3 batches/epoch; embedding signal buried. Halving batch → 2–4× more update events per rare entity. DLRM / RecSys Challenge 2025 norms.
4. **Embedding regularization tweaks for long-tail.** Drop `embeddings_regularizer` from L2(1e-6) → **L2(1e-7)** (heavier L2 amplifies embedding collapse on rare rows). Add `SpatialDropout1D(0.1)` on each Embedding output (drops whole dims across the batch — vanilla Dropout fragments the vector). Add LayerNorm on the concatenated embedding branch before the cross/deep trunk so tables with wildly different effective scales don't dominate.
5. **EarlyStopping target = embedding artifact quality, not joint val_loss.** Joint val_loss is dominated by easy heads. Two options: (a) monitor weighted avg of *hard* heads (`val_pa_result_accuracy + val_result_family_accuracy + val_trajectory_remapped_accuracy`), patience 5; (b) linear-probe callback — every N epochs freeze embeddings, fit logistic-regression probe on a held-out time-forward slice (e.g. last season), monitor probe AUC. (b) is directly the artifact-quality metric we ship.
6. **Two-stage pretrain.** Stage 1 (3–5 epochs): freeze trunk to identity-ish, train embeddings + heads only at high embedding LR — forces signal into embeddings before trunk absorbs it. Stage 2 (10–15 epochs): unfreeze trunk, drop embedding LR by 3–5×, full joint training with UW. "Don't Freeze Your Embedding" (ICLR) confirms freezing embeddings *last* underperforms when embeddings are the shipped artifact.

Top-3 changes likely to materially move entity perm-imp gates: (1) separate higher embedding LR + warmup, (2) uncertainty-weighted loss, (3) smaller batch + linear-probe EarlyStopping.

**v2 per-epoch slash-line probe (qualitative training diagnostic).** Keras callback. Per epoch, build a fixed `(10, n_features)` input batch: 5 batter probes (each row varies `batter_id`, fixes `pitcher_id` at a chosen neutral-average pitcher) + 5 pitcher probes (mirrored). All other inputs held at training-set defaults (modal categoricals, mean numerics). Run `model.predict` → `pa_result` 16-class probabilities. Derive expected slash line per row via `seed_plate_appearance_result_types.csv` flags:
- `AVG = Σ P(r) [is_hit] / Σ P(r) [is_at_bat]`
- `OBP = Σ P(r) [is_on_base_success] / Σ P(r) [is_on_base_opportunity]`
- `SLG = Σ P(r) × total_bases(r) / Σ P(r) [is_at_bat]`

Log per row: `epoch=N tier=<tier> player=<id> AVG=.XXX OBP=.XXX SLG=.XXX`. Trajectory across epochs shows when (and if) the embedding starts encoding player quality. HOF row should drift toward .300+ / .400+ / .500+; replacement toward .220 / .280 / .330.

Canonical 10-player roster (Retrosheet ID lookup from `stg_people` at impl time; placeholder names below):

Batters: HOF=Babe Ruth, All-Star=Mike Trout, Average=∼100 wRC+ regular (e.g. Justin Turner), Below-Avg=∼85 wRC+ regular (e.g. Andrelton Simmons), Replacement=career ∼70 wRC+ part-timer.

Pitchers: HOF=Pedro Martinez, All-Star=Clayton Kershaw, Average=∼100 ERA+ innings-eater (e.g. Mark Buehrle), Below-Avg=∼85 ERA+ swingman (e.g. Edwin Jackson), Replacement=AAAA SP/swingman with brief MLB stints.

Implementation: `deep/pretrain/probes.py` housing a `SlashLineProbe(keras.callbacks.Callback)`. Constants for tier labels + player names live next to the class; ID resolution + neutral-counterpart selection done once at callback construction by querying `stg_people` / `event_observation_context` for a modal pitcher and modal batter. Probe also informs human "is pretrain still warming up at epoch N" decisions for EarlyStopping tuning.

### Wire pretrained_embeddings_artifact_id into remaining deep targets

**Status: fixed 2026-05-17.** Geometry non-trajectory specs
(location_side / depth / edge) were missing
`pretrained_embeddings_artifact_id` despite the prose claim in
`bc/python_models/statistical/CLAUDE.md`. `_maybe_load_pretrained_embeddings`
early-returns when the field is `None`, so the
`BC_DEEP_PRETRAIN_ARTIFACT_OVERRIDE` env never fired for those targets
and the perm-imp sweep silently ran them with no pretrain at all.
Geometry specs now all set the field; regression guarded by
`test_every_geometry_spec_declares_pretrain_artifact`. Park-factors /
run-values remain out of DL scope.

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

## Phase-3 pretrain rename pass

The active pretrain architecture (described in `bc/python_models/statistical/CLAUDE.md` and the [[pretrain-architecture]] memory) still carries iteration-era version labels in file and spec names:

- `EVENT_UNIVERSE_V8_SPEC` / `EVENT_UNIVERSE_V8_CONTEXT_SPEC` (and the `..._V8_LAYOUT`, `..._V8_HEADS`, `V8_PLAYER_GROUP_COLS`, `V8_ROW_FILTER_PREDICATE` constants) in `bc/python_models/statistical/deep/pretrain/targets.py`.
- Spec name strings `"event_universe_v8"` / `"event_universe_v8_context"`, which become artifact directory names under `artifacts/statistical/deep/`.
- Orchestrator script `scripts/run_pretrain_v8_residual.sh` plus `BC_V8_*` env-var prefix.
- Stale `EVENT_UNIVERSE_SPEC` / `EVENT_UNIVERSE_CONTEXT_SPEC` + `EVENT_UNIVERSE_LAYOUT` (and the v6/v7 prior `event_universe_context` artifact tree) still registered in tree as back-compat.
- Test files `test_pretrain_v8_layout.py`, `test_pretrain_head_offset.py`, `test_pretrain_context_spec.py`.

Rename pass: collapse to the canonical names (`EVENT_UNIVERSE_SPEC` / `event_universe` / `run_pretrain.sh` / `BC_PRETRAIN_*`) and delete the legacy specs once nothing on the branch consumes them. Existing artifact directories under `artifacts/statistical/deep/event_universe_v8*/` will need to either move or stay (gitignored anyway). Republish the pointer under the new spec name.

Pre-rename TODOs that block it:
- Replace `pretrain_eval_pretrain.py` with a sidecar that targets the current head set (no `pa_result`).
- Decide whether to drop the legacy `event_universe` / `event_universe_context` registrations or keep one as the "historical" archive.

## Phase-3 pretrain — embedding LR + regularization

Full-corpus stage-2 best epoch was epoch 1 (out of 8) — embeddings overshoot the residual past the first pass under current schedule (split optimizer, trunk Adam 1e-3, embed Adam 5e-3 with warmup + CosineDecay α=0.1). Worth trying lower embed LR (1.5e-3) and / or modest L2 (1e-5 to 1e-4) on `embed_player` to extend useful training and see if a longer fit lifts hard-head accuracy further. Current pretrained-vs-baseline gates already clear by ~5×, so this is a tightening, not a blocker.
