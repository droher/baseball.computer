## Reproducibility

Most tables in this paper are queries against `bc.db`, a DuckDB database built
entirely through SQLMesh — `MODEL` blocks and Python `@model` decorators, no
ad hoc writes — from 45 source parquet files and the coverage pipeline's
`model_input_*` datasets. <!-- src: CLAUDE.md --> <!-- src: .claude/rules/sqlmesh.md -->
The queries and outputs behind every number are checked in alongside the
prose: `notes/paper/queries/<name>.sql` is the exact query, and
`notes/paper/tables/<name>.md` is its output against the published database,
for each of the SQL-query-backed tables cited in §7. A further set of tables —
the ones behind §5 and §7's Validation subsection — are recipe-driven rather
than single-query outputs, and each carries its own regeneration command inline
in `notes/paper/tables/<name>.md`: `just validate-gates` (read-only; sweeps
every published artifact pointer through `validate_artifact` and reports the
gate-status table and the state-transition and run-expectancy
predictive-coverage numbers; `--write` is the only path that stamps a
manifest's `validation_status`, gate version, and weak-identification flag),
`uv run --group stats python scripts/mnar_anchor.py --run-id <id>` (reads the
frozen geometry dataset and the published trajectory export read-only and writes
the per-era derived-slice bounds and the MAR-on-derived diagnostic), `just
sensitivity-ribbon --bound <anchor-run-dir>` (the per-era ribbon and the offset
at which it reaches the bound), and `just mnar-backtest --model geometry --smoke
--mask-design {w_class_intensity,covariate_joint,scorer_blocked,era_graded}`
(the masked backtest across the four designs — the recorded numbers are
smoke-budget runs, the flag is part of the command that reproduces them, and no
run artifacts are checked in, so a reader must rerun to re-check them). None of
these writes to `bc.db` or mutates any published artifact.

Two game-level partitions are in use and a reader reproducing a held-out number
must pick the right one. Every Bayes fit and every coverage gate holds out fold 0
of `blake2s(game_id) % 10` (`splits.game_hash_fold`); the deep supplements
train, early-stop, and report on the `HASH(game_id) % 100` `TRAIN` / `VALIDATE`
/ `TEST` partition the modeling datasets carry as `primary_fold`, with their
out-of-fold predictions assigned by `blake2s(game_id) % 5` inside `TRAIN`. The
two hashes are unrelated. <!-- src: bc/python_models/statistical/splits.py --> <!-- src: bc/python_models/statistical/deep/training.py -->

Provenance runs deeper than the query. Every `estimated`-tier row carries an
`artifact_id` that resolves to an `ArtifactManifest` — a JSON document
recording the source snapshot, package versions, and, for Bayesian fits, a
`sampler_config` with draw count, tuning steps, chain count, target-accept
rate, maximum tree depth, backend, and the integer random seed the fit ran
on. <!-- src: bc/python_models/statistical/schemas.py --> Fits are
reproducible by seed and sampler settings, not by re-running with whatever
configuration happens to be current. This provenance layer is deliberately
not MLflow: the deep-learning training pipeline under `bc/python_models/ml/`
does use MLflow, but the statistical and Bayesian layer that publishes the
tables in this paper excludes it by construction — an artifact-id directory
plus `manifest.json` plus structured logs is the whole backend, and a test
asserts that importing the Bayes/deep-supplement modules never pulls MLflow
into the process. <!-- src: bc/tests/statistical/deep/test_no_mlflow_import.py -->

`bc.db` itself is not the distribution artifact end users query. The publish
pipeline copies `main_models.*` and `main_seeds.*` into a DuckLake catalog,
uploads catalog and data to object storage, and mirrors the result as a
standalone `bc_remote.db` plus per-table parquet — the artifact a downstream
consumer without SQLMesh actually reads. <!-- src: scripts/CLAUDE.md -->
Reproducing a published number end to end means three things: the SQLMesh
plan that built the table, the manifest the `artifact_id` points to, and the
query in `notes/paper/queries/` that reads it back — all three are version
controlled, and none of them is this paper's private copy of the truth.
