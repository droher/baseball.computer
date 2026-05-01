# M2 dep-upgrade spike (PLE-385)

Risk assessment for the big-jump bump in PLE-386. Read before touching pins.

## Target versions (PyPI verified 2026-04-30)

| Package           | From       | To           | Notes |
|-------------------|------------|--------------|-------|
| `duckdb`          | 0.10.1     | **1.5.2**    | Wheels for Py 3.10–3.14. 1.0 stability promise applies. |
| `dbt-core`        | 1.7.11     | **~=1.10.0** | Match `dbt-duckdb` minor. 1.11 latest but no matching adapter. |
| `dbt-duckdb`      | 1.7.3      | **1.10.1**   | Released 2026-02-17. Requires `dbt-core>=1.8.0`. |
| `requires-python` | `>=3.12,<3.13` | **`>=3.12`** | Cap was JAX/0.10 only; both gone (PLE-383/384). |

## DuckDB 0.10 → 1.5.2 surface check

- **Highest risk: ENUM definitions** in `bc/macros/init_db.sql`.
  - 5 hand-defined (lines 51–56): no risk.
  - 15 single-source `SELECT DISTINCT` enums (lines 58–72).
  - 3 multi-source `UNION` enums (`account_type`, `park_id`, `team_id`).
  - 2 `VARCHAR` aliases (`player_id`, `game_id`) — trivial.
  - DuckDB 1.x may tighten `CREATE TYPE … AS ENUM (SELECT DISTINCT …)` semantics. Likely fix if it breaks: explicit dedup or `ARRAY_AGG(DISTINCT … ORDER BY …)` pattern.
- No usages of `LIST_VALUE`, `STRUCT_PACK`, `enum_range`, `union_by_name` (grep confirmed).
- Window functions: backward-compatible per release notes. `event_states_full` partitioning by `(game_id, frame, inning)` should hold.
- `read_parquet()` HTTP URL handling unchanged. `httpfs` autoload preserved.

## dbt 1.7 → 1.10 surface check

- **`tests:` → `data_tests:`** rename (1.8): 23 YAMLs use legacy key. Backward-compatible through 1.11. **Deferred** — separate Linear issue filed at execution.
- **`source-freshness-run-project-hooks`** flips to `True` default in 1.10. Repo has no project hooks today, but set explicitly to `false` in `bc/dbt_project.yml` to preserve current behavior.
- **Spaces-in-resource-names** enforcement (1.10): repo conformant.
- No `--models` flag usages in `scripts/` or `bc/` (grep confirmed).

## dbt-duckdb 1.7 → 1.10

- `external` materialization unchanged (used in export script, not in models).
- Per-thread output added — no impact on current single-threaded workflow.
- Transitive `logbook<1.9` pin handled by resolver.

## Highest-risk files

| File | Lines | Why |
|------|-------|-----|
| `bc/macros/init_db.sql` | 135 | ENUM creation + `alter_types` macro: most likely to break. |
| `bc/models/intermediate/states/event_states_full.sql` | 130 | Window-function workhorse. |
| `bc/models/intermediate/expectancy/linear_weights.sql` | 129 | Heavy SQL, splits `DoublePlay` from `InPlayOut`. |
| `bc/models/intermediate/states/event_base_out_states.sql` | 91 | State tracking, downstream of `event_states_full`. |

## Decision log

- **`tests:` → `data_tests:` rename**: deferred (backward-compat through 1.11). Cleanup Linear issue filed at execution.
- **Python upper bound**: lift to `>=3.12` only (no upper bound) — JAX/0.10 caps gone.
- **dbt-core minor**: 1.10.x to match `dbt-duckdb` 1.10.1. Flag at PR review if user prefers 1.11 (no matching adapter today).

## Out of scope (this milestone)

- Full-data `dbt run` (M3.1 / PLE-390).
- R2 publish (M3.3).
- ML layer re-enable.
- CI workflow (M4).
