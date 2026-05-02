"""Before-all hooks that materialize source tables, ENUMs, and seeds.

`init_db`, `create_enums`, `alter_types`, and `load_seeds` run as
`before_all` hooks; each returns a list of SQL strings SQLMesh executes
in order on the gateway connection. Source-table metadata lives in
`bc/external_models.yaml` (also auto-loaded by SQLMesh's external-model
loader); seeds live under `bc/seeds/`.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator


def _logger() -> logging.Logger:
    """Lazy logger factory.

    Module-level Logger instances break SQLMesh's `serialize_env`, which
    only accepts literals / modules / callables.
    """
    return logging.getLogger(__name__)


def _project_root() -> Path:
    """Lazy path lookup. Module-level Path objects break serialize_env."""
    return Path(__file__).resolve().parent.parent


def _no_quote(s: str, label: str) -> None:
    if "'" in s:
        raise RuntimeError(f"single-quote in {label} breaks DDL: {s!r}")


# ---------------------------------------------------------------------------
# ENUM definitions — single source of truth.
# Order matters: types appear in dependency order so DROP TYPE can run
# child-types before parent-types they reference.

_ENUM_DEFS: list[tuple[str, str]] = [
    ("base", "ENUM ('Home', 'First', 'Second', 'Third')"),
    ("baserunner", "ENUM ('Batter', 'First', 'Second', 'Third')"),
    ("frame", "ENUM ('Top', 'Bottom')"),
    ("side", "ENUM ('Home', 'Away')"),
    # TODO: Standardize hand values.
    ("hand", "ENUM ('L', 'R', 'B', '?', 'Left', 'Right')"),
    ("game_type", "ENUM (SELECT DISTINCT game_type FROM game.games ORDER BY 1)"),
    (
        "account_type",
        (
            "ENUM ("
            + "SELECT DISTINCT account_type FROM game.games "
            + "UNION SELECT DISTINCT account_type FROM box_score.box_score_games)"
        ),
    ),
    ("doubleheader_status", "ENUM (SELECT DISTINCT doubleheader_status FROM game.games ORDER BY 1)"),
    ("time_of_day", "ENUM (SELECT DISTINCT time_of_day FROM game.games ORDER BY 1)"),
    ("sky", "ENUM (SELECT DISTINCT sky FROM game.games ORDER BY 1)"),
    ("field_condition", "ENUM (SELECT DISTINCT field_condition FROM game.games ORDER BY 1)"),
    ("precipitation", "ENUM (SELECT DISTINCT precipitation FROM game.games ORDER BY 1)"),
    ("wind_direction", "ENUM (SELECT DISTINCT wind_direction FROM game.games ORDER BY 1)"),
    (
        "plate_appearance_result",
        (
            "ENUM (SELECT DISTINCT plate_appearance_result FROM event.events "
            + "WHERE plate_appearance_result IS NOT NULL ORDER BY 1)"
        ),
    ),
    ("pitch_sequence_item", "ENUM (SELECT DISTINCT sequence_item FROM event.event_pitch_sequences ORDER BY 1)"),
    (
        "park_id",
        (
            "ENUM ("
            + "SELECT DISTINCT park_id FROM misc.park "
            + "UNION SELECT DISTINCT park_id FROM box_score.box_score_games WHERE park_id IS NOT NULL "
            + "UNION SELECT DISTINCT park_id FROM game.games WHERE park_id IS NOT NULL "
            + "UNION SELECT DISTINCT park_id FROM misc.schedule WHERE park_id IS NOT NULL)"
        ),
    ),
    (
        "team_id",
        (
            "ENUM ("
            + "SELECT DISTINCT team_id FROM misc.roster "
            + "UNION SELECT DISTINCT visiting_team FROM misc.gamelog "
            + "UNION SELECT DISTINCT home_team FROM misc.gamelog "
            + "UNION SELECT DISTINCT away_team_id FROM box_score.box_score_games "
            + "UNION SELECT DISTINCT home_team_id FROM box_score.box_score_games)"
        ),
    ),
    ("game_id", "VARCHAR"),
    ("player_id", "VARCHAR"),
    (
        "trajectory",
        (
            "ENUM (SELECT DISTINCT batted_trajectory FROM event.events "
            + "WHERE batted_trajectory IS NOT NULL ORDER BY 1)"
        ),
    ),
    (
        "location_general",
        (
            "ENUM (SELECT DISTINCT batted_location_general FROM event.events "
            + "WHERE batted_location_general IS NOT NULL ORDER BY 1)"
        ),
    ),
    (
        "location_depth",
        (
            "ENUM (SELECT DISTINCT batted_location_depth FROM event.events "
            + "WHERE batted_location_depth IS NOT NULL ORDER BY 1)"
        ),
    ),
    (
        "location_angle",
        (
            "ENUM (SELECT DISTINCT batted_location_angle FROM event.events "
            + "WHERE batted_location_angle IS NOT NULL ORDER BY 1)"
        ),
    ),
    (
        "baserunning_play",
        (
            "ENUM (SELECT DISTINCT baserunning_play_type FROM event.event_baserunners "
            + "WHERE baserunning_play_type IS NOT NULL ORDER BY 1)"
        ),
    ),
    ("fielding_play", "ENUM (SELECT DISTINCT fielding_play FROM event.event_fielding_play ORDER BY 1)"),
]


def _drop_type_statements() -> list[str]:
    """DROP TYPE IF EXISTS for every project ENUM, in reverse-definition order
    so child types drop before parents that reference them."""
    return [f"DROP TYPE IF EXISTS {name}" for name, _ in reversed(_ENUM_DEFS)]


# ---------------------------------------------------------------------------
# external_models.yaml parsing


def _parsed_sources() -> list[dict[str, Any]]:
    """Read bc/external_models.yaml; return one entry per external model.

    Each entry has:
      - schema: duckdb schema (= name.split('.')[0])
      - name:   table name within the schema
      - columns: dict[col_name, data_type]  (cast manifest; may be partial)
    """
    path = _project_root() / "external_models.yaml"
    if not path.exists():
        raise RuntimeError(f"external_models.yaml missing: {path}")
    doc = yaml.safe_load(path.read_text()) or []
    out: list[dict[str, Any]] = []
    for entry in doc:
        fqn = entry["name"]
        if "." not in fqn:
            raise RuntimeError(
                f"external model name must be schema.table, got '{fqn}'"
            )
        schema, name = fqn.split(".", 1)
        out.append(
            {
                "schema": schema,
                "name": name,
                "columns": entry.get("columns") or {},
            }
        )
    return out


# ---------------------------------------------------------------------------
# Macros


@macro()
def init_db(
    evaluator: MacroEvaluator,
    sample_factor: int = 1,
    seed: int = 0,
) -> list[str]:
    """Build CREATE SCHEMA + CREATE TABLE DDL for every source table.

    Idempotent by default (CREATE TABLE IF NOT EXISTS); pass
    `--var force_reload=true` to re-load via CREATE OR REPLACE.
    """
    if sample_factor != 1 or seed != 0:
        raise NotImplementedError(
            "Event sampling (sample_factor/seed) is not wired up."
        )

    roots = evaluator.var("source_roots")
    if not isinstance(roots, dict):
        raise RuntimeError("source_roots variable missing or not a dict")
    force_reload = bool(evaluator.var("force_reload", False))
    bust = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S")

    statements: list[str] = []
    if force_reload:
        # CREATE OR REPLACE TABLE fails on tables whose columns reference
        # an ENUM; drop the ENUMs first so the cascade clears.
        statements.extend(_drop_type_statements())
    seen_schemas: set[str] = set()
    for node in _parsed_sources():
        schema = node["schema"]
        name = node["name"]
        _no_quote(schema, "source schema")
        _no_quote(name, "source table name")

        try:
            root = roots[schema]
        except KeyError as e:
            raise RuntimeError(
                f"No source_root configured for schema '{schema}' (table {name})"
            ) from e
        _no_quote(root, "source_root")

        bust_qs = f"?v={bust}" if root.startswith("http") else ""
        url = f"{root}/{name}.parquet{bust_qs}"

        if schema not in seen_schemas:
            statements.append(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            seen_schemas.add(schema)

        verb = "CREATE OR REPLACE TABLE" if force_reload else "CREATE TABLE IF NOT EXISTS"
        statements.append(
            f"{verb} {schema}.{name} AS (SELECT * FROM read_parquet('{url}'))"
        )

    _logger().info(
        "init_db: %d statements across %d schemas (force_reload=%s)",
        len(statements),
        len(seen_schemas),
        force_reload,
    )
    return statements


@macro()
def create_enums(evaluator: MacroEvaluator) -> list[str]:
    """Drop and recreate every project ENUM type.

    The SELECT DISTINCT branches require init_db to have populated
    event/game/box_score first.

    Idempotency: ENUM types can't be dropped while in use by a column, so
    re-running this against an established DB explodes. We skip the whole
    block unless `force_reload=true` OR the canonical `base` type doesn't
    yet exist. The escape hatch is `--var force_reload=true`.
    """
    force_reload = bool(evaluator.var("force_reload", False))
    if not force_reload:
        try:
            row = evaluator.engine_adapter.fetchone(
                "SELECT COUNT(*) FROM duckdb_types() WHERE type_name = 'base'"
            )
            base_exists = bool(row and row[0])
        except Exception as e:
            _logger().warning(
                "create_enums: type-existence check failed (%s); proceeding with full DDL",
                e,
            )
            base_exists = False
        if base_exists:
            _logger().info(
                "create_enums: base type already exists, skipping (set force_reload=true to recreate)"
            )
            return []

    statements: list[str] = _drop_type_statements()
    statements.extend(f"CREATE TYPE {name} AS {ddl}" for name, ddl in _ENUM_DEFS)
    _logger().info("create_enums: %d statements", len(statements))
    return statements


@macro()
def alter_types(_evaluator: MacroEvaluator) -> list[str]:
    """Cast source columns to their declared types.

    Reads the `columns:` mapping from each external_models.yaml entry and
    emits one ALTER per column. Must run after `create_enums` so that
    ENUM type references resolve.
    """
    statements: list[str] = []
    for node in _parsed_sources():
        schema = node["schema"]
        name = node["name"]
        _no_quote(schema, "source schema")
        _no_quote(name, "source table name")
        for col_name, data_type in node["columns"].items():
            if '"' in col_name or "'" in col_name:
                raise RuntimeError(
                    f"unsafe character in source column name: {col_name!r}"
                )
            _no_quote(data_type, f"data_type for {schema}.{name}.{col_name}")
            statements.append(
                f'ALTER TABLE {schema}.{name} ALTER COLUMN "{col_name}" TYPE {data_type}'
            )
    _logger().info("alter_types: %d ALTER statements", len(statements))
    return statements


# ---------------------------------------------------------------------------
# Seed loader.
#
# DuckDB's `read_csv` with `nullstr=['', ' ']` means only empty fields and
# the single-space sentinel become NULL — literal "NA" survives as a string,
# which the National Association rows in seed_franchises.csv depend on.
# Column types come from the seed YAML's `data_type:` entries.


def _parsed_seeds() -> list[dict[str, Any]]:
    """Return one entry per seed YAML (yml + matching .csv)."""
    out: list[dict[str, Any]] = []
    for yml_path in sorted((_project_root() / "seeds").rglob("*.yml")):
        doc = yaml.safe_load(yml_path.read_text())
        for seed in doc.get("seeds", []) or []:
            name = seed["name"]
            csv_path = yml_path.with_name(f"{name}.csv")
            if not csv_path.exists():
                _logger().warning("seed CSV missing for %s: %s", name, csv_path)
                continue
            out.append(
                {
                    "name": name,
                    "csv_path": str(csv_path),
                    "columns": seed.get("columns", []) or [],
                }
            )
    return out


@macro()
def load_seeds(_evaluator: MacroEvaluator) -> list[str]:
    """Load every seed CSV into `main_seeds.<name>`."""
    statements: list[str] = ["CREATE SCHEMA IF NOT EXISTS main_seeds"]
    for seed in _parsed_seeds():
        name = seed["name"]
        csv_path = seed["csv_path"]
        _no_quote(name, "seed name")
        _no_quote(csv_path, "seed CSV path")
        cols_with_type = [
            (col["name"], col["data_type"])
            for col in seed["columns"]
            if col.get("data_type")
        ]
        for cname, ctype in cols_with_type:
            _no_quote(cname, f"seed column name in {name}")
            _no_quote(ctype, f"seed data_type in {name}.{cname}")
        if cols_with_type:
            cols_clause = ", ".join(
                f"'{cname}': '{ctype.upper()}'" for cname, ctype in cols_with_type
            )
            columns_arg = f", columns={{{cols_clause}}}"
        else:
            columns_arg = ""
        statements.append(
            f"CREATE OR REPLACE TABLE main_seeds.{name} AS "
            f"SELECT * FROM read_csv("
            f"'{csv_path}', header=true, nullstr=['', ' ']"
            f"{columns_arg})"
        )
    _logger().info("load_seeds: %d seeds", len(statements) - 1)
    return statements
