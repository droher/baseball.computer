# SQLMesh CLI reference

Canonical doc: https://sqlmesh.readthedocs.io/en/stable/reference/cli/

All commands are run from the project root (the directory containing
`config.py` or `config.yaml`). Run `sqlmesh <cmd> --help` for current
flags — the surface evolves quickly.

## Project lifecycle

| Command | Use |
|---------|-----|
| `sqlmesh init [-t <template>]` | Scaffold a new project. Templates include `default`, `dbt`, `duckdb`, `empty`. |
| `sqlmesh info` | Print config, gateway, connection health. Run first when debugging connection issues. |
| `sqlmesh migrate` | Migrate the state schema after upgrading SQLMesh. Required after most version bumps. |
| `sqlmesh rollback` | Roll back the last `migrate`. |
| `sqlmesh clean` | Remove temporary build artefacts. |
| `sqlmesh destroy` | Drop all SQLMesh-managed tables and views. Destructive — confirm before running. |

## Plan / apply / run

| Command | Use |
|---------|-----|
| `sqlmesh plan [env]` | The main verb. Diffs code vs state, classifies changes, prompts for backfill, applies. With no env arg, targets `prod` and is virtual-only by default. |
| `sqlmesh plan dev --auto-apply` | Build to `dev` non-interactively. |
| `sqlmesh plan --restate-model <name>` | Force re-materialization of a model without a code change. |
| `sqlmesh plan --forward-only` | Apply changes without backfill. New data picks up the new logic; history keeps the old. |
| `sqlmesh apply` | Apply a saved plan. Rarely run directly — `plan --auto-apply` is the normal path. |
| `sqlmesh run [env]` | Run any cron-ready models in the env. Useful for scheduled jobs. |
| `sqlmesh check_intervals` | Show which intervals are missing per model. |

## Inspection / debugging

| Command | Use |
|---------|-----|
| `sqlmesh render <model>` | Print fully-rendered SQL for a model (after macro expansion). First stop when a query is producing unexpected SQL. |
| `sqlmesh evaluate <model> [--start --end]` | Execute a model's query and print the result rows without materialising. |
| `sqlmesh fetchdf '<sql>'` | Run ad-hoc SQL through the configured gateway. |
| `sqlmesh table_diff <a> <b>` | Row and schema diff between two physical tables. |
| `sqlmesh table_name <model>` | Print the physical snapshot table name a model resolves to. |
| `sqlmesh dag` | Emit the model DAG (Graphviz). |
| `sqlmesh lineage <model>` | Print upstream and downstream models for a name. |

## Quality

| Command | Use |
|---------|-----|
| `sqlmesh audit [env]` | Run audits attached to models in the env. |
| `sqlmesh test [test_name]` | Run YAML unit tests under `tests/`. |
| `sqlmesh create_test <model>` | Generate a unit-test scaffold by sampling the model's current output. |
| `sqlmesh lint` | Run lint rules over models. |

## Environments and state

| Command | Use |
|---------|-----|
| `sqlmesh environments` | List environments. |
| `sqlmesh invalidate <env>` | Mark an environment for janitor cleanup. |
| `sqlmesh janitor` | Garbage-collect invalidated envs and orphaned snapshots. |
| `sqlmesh state export -o <path>` | Dump state to a file for backup or transfer. Confirm flag spelling with `--help` — has shifted across versions. |
| `sqlmesh state import -i <path>` | Load state from a file. Confirm flag spelling with `--help`. |

## External models / sources

| Command | Use |
|---------|-----|
| `sqlmesh create_external_models` | Probe the engine's information schema and write/update `external_models.yaml`. |
| `sqlmesh dlt_refresh` | Refresh dlt-loaded sources, if dlt integration is configured. |

## Common flag patterns

- `-p <path>` — point at a config in a non-cwd directory.
- `--gateway <name>` — override the default gateway for one invocation.
- `--vars '{key: value}'` — pass / override config vars at the CLI.
- `--start <date>` / `--end <date>` — bound an interval window for plan,
  evaluate, audit.
- `--no-prompts` — fail rather than prompt; useful in CI.

Always confirm with `--help` — flag names have changed across versions.
