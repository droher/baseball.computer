"""SQLMesh ports of the three init_db jinja macros.

Phase 1 replaces `bc/macros/init_db.sql`. Reasons:

- SQLMesh's dbt-import jinja runtime does not expose `graph.sources`, so the
  original `{% for node in graph.sources.values() %}` cannot run. We walk the
  six `source.yml` files directly.
- These run as `before_all` hooks. Each macro returns a list of SQL strings;
  SQLMesh executes them in order on the gateway connection.
- `_` prefix on the filename keeps dbt's macro loader from ever picking this
  file up, so the legacy jinja macros stay loadable while both engines coexist.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# source.yml parsing


_SCHEMAS: tuple[str, ...] = (
    "event",
    "game",
    "box_score",
    "misc",
    "baseballdatabank",
    "biodata",
)

_STAGING_DIR = Path(__file__).resolve().parent.parent / "models" / "staging"


def _source_files() -> list[Path]:
    return [_STAGING_DIR / schema / "source.yml" for schema in _SCHEMAS]


_source_cache: list[dict[str, Any]] | None = None


def _parsed_sources() -> list[dict[str, Any]]:
    """Flatten all six source.yml files into a list of source-table dicts.

    Returns one entry per `tables[]` element, augmented with the parent
    `sources[].name` (= duckdb schema). Cached at module load.
    """
    global _source_cache
    if _source_cache is not None:
        return _source_cache

    out: list[dict[str, Any]] = []
    for path in _source_files():
        if not path.exists():
            logger.warning("source.yml missing: %s", path)
            continue
        doc = yaml.safe_load(path.read_text())
        for source in doc.get("sources", []):
            schema = source["name"]
            for table in source.get("tables", []):
                out.append(
                    {
                        "schema": schema,
                        "name": table["name"],
                        "identifier": table.get("identifier", table["name"]),
                        "meta": table.get("meta", {}) or {},
                        "columns": table.get("columns", []) or [],
                    }
                )
    _source_cache = out
    return out


# ---------------------------------------------------------------------------
# Helpers


_CSV_READ_ARGS = (
    ", header=true, all_varchar=true, delim=',', quote='\"', escape='\"',"
    " null_padding=true, ignore_errors=true"
)


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
    seen_schemas: set[str] = set()
    for node in _parsed_sources():
        schema = node["schema"]
        name = node["name"]
        identifier = node["identifier"]
        ext = node["meta"].get("source_extension", "parquet")

        try:
            root = roots[schema]
        except KeyError as e:
            raise RuntimeError(
                f"No source_root configured for schema '{schema}' "
                f"(table {name})"
            ) from e

        is_remote = root.startswith("http")
        bust_qs = f"?v={bust}" if is_remote else ""
        read_args = _CSV_READ_ARGS if ext == "csv" else ""
        url = f"{root}/{identifier}.{ext}{bust_qs}"

        if schema not in seen_schemas:
            statements.append(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            seen_schemas.add(schema)

        verb = "CREATE OR REPLACE TABLE" if force_reload else "CREATE TABLE IF NOT EXISTS"
        statements.append(
            f"{verb} {schema}.{name} AS ("
            f"SELECT * FROM read_{ext}('{url}'{read_args}))"
        )

    logger.info(
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
            logger.warning("create_enums: type-existence check failed (%s); proceeding with full DDL", e)
            base_exists = False
        if base_exists:
            logger.info("create_enums: base type already exists, skipping (set force_reload=true to recreate)")
            return []

    drop_types = [
        "base", "baserunner", "frame", "side", "hand",
        "game_type", "account_type", "doubleheader_status", "time_of_day",
        "sky", "field_condition", "precipitation", "wind_direction",
        "plate_appearance_result", "pitch_sequence_item",
        "park_id", "team_id", "game_id", "player_id",
        "trajectory", "location_general", "location_depth", "location_angle",
        "baserunning_play", "fielding_play",
    ]
    statements: list[str] = [f"DROP TYPE IF EXISTS {t}" for t in drop_types]

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

    logger.info("create_enums: %d statements", len(statements))
    return statements


@macro()
def alter_types(evaluator: MacroEvaluator) -> list[str]:
    """Cast source columns to their declared `data_type:`.

    Replaces `init_db.sql:146-156`. Must run after `create_enums` so that
    references to ENUM types resolve.
    """
    statements: list[str] = []
    for node in _parsed_sources():
        schema = node["schema"]
        name = node["name"]
        for col in node["columns"]:
            data_type = col.get("data_type")
            if not data_type:
                continue
            statements.append(
                f'ALTER TABLE {schema}.{name} ALTER COLUMN "{col["name"]}" TYPE {data_type}'
            )
    logger.info("alter_types: %d ALTER statements", len(statements))
    return statements
