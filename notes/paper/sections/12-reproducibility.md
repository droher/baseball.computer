## Reproducibility

Most tables in this paper are queries against `bc.db`, a DuckDB database built
entirely through SQLMesh — `MODEL` blocks and Python `@model` decorators, no
ad hoc writes — from 45 source parquet files and the coverage pipeline's
`model_input_*` datasets. <!-- src: CLAUDE.md --> <!-- src: .claude/rules/sqlmesh.md -->
The queries and outputs behind every number are checked in alongside the
prose: `notes/paper/queries/<name>.sql` is the exact query, and
`notes/paper/tables/<name>.md` is its output against the published database,
for each of the eleven SQL-query-backed tables cited in this section. A
further set of tables — the ones behind §7's Validation subsection — are
recipe-driven rather than single-query outputs, and each carries its own
regeneration command inline in `notes/paper/tables/<name>.md`: `just
validate-gates` (read-only; sweeps every published artifact pointer through
`validate_artifact` and reports the gate-status table, the state-transition
and run-expectancy predictive-coverage numbers, and the
`geometry_location_depth` disposition), `just mnar-backtest --mask-design
{w_class_intensity,covariate_joint,scorer_blocked,era_graded}` (the masked
backtest across the four robustness designs), `just sensitivity-ribbon
--joint <anchor-run-dir>` (the joint anchored sensitivity ribbon), and the
anchor driver that feeds it, `uv run --group stats python
scripts/mnar_anchor.py --run-id <id>` (reads `bc.db` read-only and writes the
per-era GroundBall selection offset). None of these four writes to `bc.db` or
mutates any published artifact; they are read-only sweeps and re-derivations
against the current published pointers, and together with the queries above
they reproduce every number in §7's Validation subsection.

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
