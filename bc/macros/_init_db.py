"""SQLMesh ports of the three init_db jinja macros.

These run as `before_all` hooks. Each macro returns a list of SQL strings;
SQLMesh executes them in order on the gateway connection.

Source-table metadata lives in `bc/external_models.yaml` (auto-loaded by
SQLMesh's external-model loader). `init_db` and `alter_types` walk that file
to emit DDL.
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

    A module-level `logger = logging.getLogger(__name__)` would be captured
    as a free variable by SQLMesh's `serialize_env`, which only accepts
    literals / modules / callables — Logger instances raise
    "Object cannot be serialized".
    """
    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# external_models.yaml parsing


def _external_models_path() -> Path:
    """Compute the external_models.yaml path lazily.

    Module-level Path objects break SQLMesh's `make_python_env` serializer
    (closures over Path globals can't round-trip through pydantic). Building
    inside a function keeps Path off the module surface.
    """
    return Path(__file__).resolve().parent.parent / "external_models.yaml"


def _parsed_sources() -> list[dict[str, Any]]:
    """Read bc/external_models.yaml; return one entry per external model.

    Each entry has:
      - schema: duckdb schema (= name.split('.')[0])
      - name:   table name within the schema
      - columns: dict[col_name, data_type]  (cast manifest; may be partial)
    """
    path = _external_models_path()
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
# Helpers


def _source_roots(evaluator: MacroEvaluator) -> dict[str, str]:
    roots = evaluator.var("source_roots")
    if not isinstance(roots, dict):
        raise RuntimeError(
            "source_roots variable missing or not a dict — check sqlmesh.yaml"
        )
    return roots


def _force_reload(evaluator: MacroEvaluator) -> bool:
    return bool(evaluator.var("force_reload", False))


def _cache_bust() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S")


def _drop_type_statements() -> list[str]:
    """DROP TYPE IF EXISTS for every project ENUM, in dependency order.

    Used by `create_enums` (always — needs to drop before re-creating)
    and by `init_db` when `force_reload=true` (must drop ENUMs before
    `CREATE OR REPLACE TABLE` on tables whose columns reference them,
    otherwise DuckDB rejects the replace).
    """
    return [
        f"DROP TYPE IF EXISTS {t}"
        for t in (
            "base", "baserunner", "frame", "side", "hand",
            "game_type", "account_type", "doubleheader_status", "time_of_day",
            "sky", "field_condition", "precipitation", "wind_direction",
            "plate_appearance_result", "pitch_sequence_item",
            "park_id", "team_id", "game_id", "player_id",
            "trajectory", "location_general", "location_depth", "location_angle",
            "baserunning_play", "fielding_play",
        )
    ]


# ---------------------------------------------------------------------------
# Macros


@macro()
def init_db(
    evaluator: MacroEvaluator,
    sample_factor: int = 1,
    seed: int = 0,
) -> list[str]:
    """Build CREATE SCHEMA + CREATE TABLE DDL for every source table.

    Replaces `init_db.sql:1-39`. Idempotent by default (CREATE TABLE IF NOT
    EXISTS); pass `--var force_reload=true` to re-load via CREATE OR REPLACE.
    """
    if sample_factor != 1 or seed != 0:
        raise NotImplementedError(
            "Event sampling (sample_factor/seed) is not ported in Phase 1. "
            "See bc/macros/init_db.sql:30-32 for the original branch."
        )

    roots = _source_roots(evaluator)
    force_reload = _force_reload(evaluator)
    bust = _cache_bust()

    statements: list[str] = []
    if force_reload:
        # CREATE OR REPLACE TABLE fails on tables whose columns reference
        # an ENUM; drop the ENUMs first so the cascade clears.
        statements.extend(_drop_type_statements())
    seen_schemas: set[str] = set()
    for node in _parsed_sources():
        schema = node["schema"]
        name = node["name"]

        if "'" in name or "'" in schema:
            raise RuntimeError(
                f"single-quote in source identifier breaks DDL: {schema}.{name!r}"
            )

        try:
            root = roots[schema]
        except KeyError as e:
            raise RuntimeError(
                f"No source_root configured for schema '{schema}' "
                f"(table {name})"
            ) from e

        if "'" in root:
            raise RuntimeError(f"single-quote in source_root breaks DDL: {root!r}")

        is_remote = root.startswith("http")
        bust_qs = f"?v={bust}" if is_remote else ""
        url = f"{root}/{name}.parquet{bust_qs}"

        if schema not in seen_schemas:
            statements.append(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            seen_schemas.add(schema)

        verb = "CREATE OR REPLACE TABLE" if force_reload else "CREATE TABLE IF NOT EXISTS"
        statements.append(
            f"{verb} {schema}.{name} AS ("
            f"SELECT * FROM read_parquet('{url}'))"
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
    """Return the DROP/CREATE TYPE block, one statement per list entry.

    Replaces `init_db.sql:41-143`. Inlined verbatim — the SELECT DISTINCT
    branches require init_db to have populated event/game/box_score first.

    Idempotency: ENUM types can't be dropped while in use by a column, so
    re-running this against an established DB explodes. We skip the whole
    block unless `force_reload=true` OR the canonical `base` type doesn't
    yet exist. The escape hatch is `--var force_reload=true`.
    """
    force_reload = _force_reload(evaluator)
    if not force_reload:
        try:
            row = evaluator.engine_adapter.fetchone(
                "SELECT COUNT(*) FROM duckdb_types() WHERE type_name = 'base'"
            )
            base_exists = bool(row and row[0])
        except Exception as e:
            _logger().warning("create_enums: type-existence check failed (%s); proceeding with full DDL", e)
            base_exists = False
        if base_exists:
            _logger().info("create_enums: base type already exists, skipping (set force_reload=true to recreate)")
            return []

    statements: list[str] = _drop_type_statements()

    statements.extend(
        [
            "CREATE TYPE base AS ENUM ('Home', 'First', 'Second', 'Third')",
            "CREATE TYPE baserunner AS ENUM ('Batter', 'First', 'Second', 'Third')",
            "CREATE TYPE frame AS ENUM ('Top', 'Bottom')",
            "CREATE TYPE side AS ENUM ('Home', 'Away')",
            # TODO: Standardize hand values.
            "CREATE TYPE hand AS ENUM ('L', 'R', 'B', '?', 'Left', 'Right')",
            "CREATE TYPE game_type AS ENUM (SELECT DISTINCT game_type FROM game.games ORDER BY 1)",
            "CREATE TYPE doubleheader_status AS ENUM (SELECT DISTINCT doubleheader_status FROM game.games ORDER BY 1)",
            "CREATE TYPE time_of_day AS ENUM (SELECT DISTINCT time_of_day FROM game.games ORDER BY 1)",
            "CREATE TYPE sky AS ENUM (SELECT DISTINCT sky FROM game.games ORDER BY 1)",
            "CREATE TYPE field_condition AS ENUM (SELECT DISTINCT field_condition FROM game.games ORDER BY 1)",
            "CREATE TYPE precipitation AS ENUM (SELECT DISTINCT precipitation FROM game.games ORDER BY 1)",
            "CREATE TYPE wind_direction AS ENUM (SELECT DISTINCT wind_direction FROM game.games ORDER BY 1)",
            "CREATE TYPE plate_appearance_result AS ENUM (SELECT DISTINCT plate_appearance_result FROM event.events WHERE plate_appearance_result IS NOT NULL ORDER BY 1)",
            "CREATE TYPE pitch_sequence_item AS ENUM (SELECT DISTINCT sequence_item FROM event.event_pitch_sequences ORDER BY 1)",
            "CREATE TYPE trajectory AS ENUM (SELECT DISTINCT batted_trajectory FROM event.events WHERE batted_trajectory IS NOT NULL ORDER BY 1)",
            "CREATE TYPE location_general AS ENUM (SELECT DISTINCT batted_location_general FROM event.events WHERE batted_location_general IS NOT NULL ORDER BY 1)",
            "CREATE TYPE location_depth AS ENUM (SELECT DISTINCT batted_location_depth FROM event.events WHERE batted_location_depth IS NOT NULL ORDER BY 1)",
            "CREATE TYPE location_angle AS ENUM (SELECT DISTINCT batted_location_angle FROM event.events WHERE batted_location_angle IS NOT NULL ORDER BY 1)",
            "CREATE TYPE baserunning_play AS ENUM (SELECT DISTINCT baserunning_play_type FROM event.event_baserunners WHERE baserunning_play_type IS NOT NULL ORDER BY 1)",
            "CREATE TYPE fielding_play AS ENUM (SELECT DISTINCT fielding_play FROM event.event_fielding_play ORDER BY 1)",
            (
                "CREATE TYPE account_type AS ENUM ("
                "SELECT DISTINCT account_type FROM game.games "
                "UNION SELECT DISTINCT account_type FROM box_score.box_score_games)"
            ),
            (
                "CREATE TYPE park_id AS ENUM ("
                "SELECT DISTINCT park_id FROM misc.park "
                "UNION SELECT DISTINCT park_id FROM box_score.box_score_games WHERE park_id IS NOT NULL "
                "UNION SELECT DISTINCT park_id FROM game.games WHERE park_id IS NOT NULL "
                "UNION SELECT DISTINCT park_id FROM misc.schedule WHERE park_id IS NOT NULL)"
            ),
            (
                "CREATE TYPE team_id AS ENUM ("
                "SELECT DISTINCT team_id FROM misc.roster "
                "UNION SELECT DISTINCT visiting_team FROM misc.gamelog "
                "UNION SELECT DISTINCT home_team FROM misc.gamelog "
                "UNION SELECT DISTINCT away_team_id FROM box_score.box_score_games "
                "UNION SELECT DISTINCT home_team_id FROM box_score.box_score_games)"
            ),
            "CREATE TYPE player_id AS VARCHAR",
            "CREATE TYPE game_id AS VARCHAR",
        ]
    )

    _logger().info("create_enums: %d statements", len(statements))
    return statements


@macro()
def alter_types(evaluator: MacroEvaluator) -> list[str]:  # noqa: ARG001
    """Cast source columns to their declared types.

    Reads the `columns:` mapping from each external_models.yaml entry and
    emits one ALTER per column. Must run after `create_enums` so that
    ENUM type references resolve.
    """
    statements: list[str] = []
    for node in _parsed_sources():
        schema = node["schema"]
        name = node["name"]
        for col_name, data_type in node["columns"].items():
            statements.append(
                f'ALTER TABLE {schema}.{name} ALTER COLUMN "{col_name}" TYPE {data_type}'
            )
    _logger().info("alter_types: %d ALTER statements", len(statements))
    return statements


# ---------------------------------------------------------------------------
# Seed loader
#
# dbt's seed loader uses agate, which preserves literal "NA" / "NULL" / etc.
# in CSV cells. Phase 1 patched SQLMesh's pandas-based seed loader
# (`PatchedDbtLoader`) to mirror that behavior. Phase 1.5 drops the dbt-import
# path entirely, so we no longer get any seed loading for free — this macro
# replaces it.
#
# Strategy: emit `CREATE OR REPLACE TABLE main_seeds.<name>` DDL using DuckDB's
# `read_csv` with `nullstr=['', ' ']` so only empty fields and the single-space
# sentinel become NULL (matching agate). Literal "NA" survives as a string.
# Column types come from the seed YAML's `data_type:` entries.


def _seeds_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "seeds"


def _parsed_seeds() -> list[dict[str, Any]]:
    """Return one entry per seed YAML (yml + matching .csv)."""
    out: list[dict[str, Any]] = []
    seeds_dir = _seeds_dir()
    for yml_path in sorted(seeds_dir.rglob("*.yml")):
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
def load_seeds(evaluator: MacroEvaluator) -> list[str]:  # noqa: ARG001
    """Load every seed CSV into `main_seeds.<name>`.

    Replaces dbt's `dbt seed` step. NA-preserving via `nullstr=['', ' ']`;
    column types come from the seed YAML.
    """
    statements: list[str] = ["CREATE SCHEMA IF NOT EXISTS main_seeds"]
    for seed in _parsed_seeds():
        name = seed["name"]
        csv_path = seed["csv_path"]
        if "'" in name or "'" in csv_path:
            raise RuntimeError(
                f"single-quote in seed identifier breaks DDL: {name!r} {csv_path!r}"
            )
        cols_with_type = [
            (col["name"], col["data_type"])
            for col in seed["columns"]
            if col.get("data_type")
        ]
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
