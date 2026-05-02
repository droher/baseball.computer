# baseball.computer

Library powering the [baseball.computer](https://baseball.computer) database.

Starts from a set of Retrosheet files dropped off by the
[Rust parser](https://github.com/droher/baseball.computer.rs) and builds a
DuckDB database (`bc.db`) of the box-score, event, and seasonal models
documented at [docs.baseball.computer](https://docs.baseball.computer).

## Build engine

Phase 1 (current) uses **SQLMesh** as the build engine, importing the
existing dbt project layout under `bc/`. The dbt CLI still works during
cutover — both engines read the same models and macros.

```bash
uv sync --group spikes-sqlmesh
cd bc
uv run --group spikes-sqlmesh sqlmesh plan dev    # create / refresh dev env
uv run --group spikes-sqlmesh sqlmesh run dev     # build the models
```

State lives in `bc/bc_state.db` (auto-created, separate from `bc.db`).

The legacy dbt path still works:

```bash
cd bc
uv run dbt run --target dev
```

## Documentation

- [docs.baseball.computer](https://docs.baseball.computer) — model + column docs
- `notes/phase-1-plan.md` — current phase status
- `notes/migration-evaluation.md` — multi-phase migration plan
