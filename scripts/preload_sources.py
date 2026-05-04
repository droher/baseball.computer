"""Parallel-load source parquet into bc.db before SQLMesh runs.

SQLMesh's `before_all` executes returned DDL serially. The 45 source
parquet loads from R2 are network-bound and trivially parallel, so
running them through SQLMesh's adapter wastes wall-clock time. This
script opens bc.db directly (one DuckDB Database, many cursors), reads
the same `external_models.yaml` source list the macro uses, and runs
CREATE TABLE IF NOT EXISTS in a thread pool.

After this runs, SQLMesh's `init_db` macro emits the same DDL but every
statement is a no-op (table already populated), so the build proceeds
unchanged. Force-reload is handled here too: pass `--force-reload` and
the script drops project ENUMs first (so dependent columns clear), then
issues CREATE OR REPLACE TABLE.

Concurrency: `--workers N` (default 8) or `BC_INIT_DB_PARALLELISM`.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
BC_DIR = REPO_ROOT / "bc"
DB_PATH = REPO_ROOT / "bc.db"
EXTERNAL_MODELS = BC_DIR / "external_models.yaml"

DEFAULT_SOURCE_ROOTS: dict[str, str] = {
    "event": "https://data.baseball.computer/event",
    "game": "https://data.baseball.computer/event",
    "box_score": "https://data.baseball.computer/event",
    "misc": "https://data.baseball.computer/misc",
    "baseballdatabank": "https://data.baseball.computer/baseballdatabank",
    "biodata": "https://data.baseball.computer/biodata",
}

# Same order as bc/macros/_init_db.py — child types before parents on drop.
_ENUM_DROP_ORDER: list[str] = [
    "fielding_play",
    "baserunning_play",
    "location_angle",
    "location_depth",
    "location_general",
    "trajectory",
    "player_id",
    "game_id",
    "team_id",
    "park_id",
    "pitch_sequence_item",
    "plate_appearance_result",
    "wind_direction",
    "precipitation",
    "field_condition",
    "sky",
    "time_of_day",
    "doubleheader_status",
    "account_type",
    "game_type",
    "hand",
    "side",
    "frame",
    "baserunner",
    "base",
]

logger = logging.getLogger("preload_sources")


def _no_quote(s: str, label: str) -> None:
    if "'" in s:
        raise RuntimeError(f"single-quote in {label} breaks DDL: {s!r}")


def _parsed_sources() -> list[dict[str, Any]]:
    if not EXTERNAL_MODELS.exists():
        raise RuntimeError(f"external_models.yaml missing: {EXTERNAL_MODELS}")
    doc = yaml.safe_load(EXTERNAL_MODELS.read_text()) or []
    out: list[dict[str, Any]] = []
    for entry in doc:
        fqn = entry["name"]
        if "." not in fqn:
            raise RuntimeError(f"external model name must be schema.table, got '{fqn}'")
        schema, name = fqn.split(".", 1)
        out.append({"schema": schema, "name": name})
    return out


def _build_items(
    sources: list[dict[str, Any]],
    roots: dict[str, str],
    force_reload: bool,
    bust: str,
) -> tuple[list[tuple[str, str, str]], set[str]]:
    items: list[tuple[str, str, str]] = []
    schemas: set[str] = set()
    for node in sources:
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
        verb = "CREATE OR REPLACE TABLE" if force_reload else "CREATE TABLE IF NOT EXISTS"
        items.append(
            (schema, name, f"{verb} {schema}.{name} AS (SELECT * FROM read_parquet('{url}'))")
        )
        schemas.add(schema)
    return items, schemas


def preload(
    workers: int,
    force_reload: bool,
    roots: dict[str, str],
) -> None:
    sources = _parsed_sources()
    bust = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S")
    items, schemas = _build_items(sources, roots, force_reload, bust)

    logger.info(
        "preload: %d tables across %d schemas, %d workers, force_reload=%s, db=%s",
        len(items),
        len(schemas),
        workers,
        force_reload,
        DB_PATH,
    )

    db = duckdb.connect(str(DB_PATH))
    try:
        # Match the gateway's runtime knobs so parquet reads behave the
        # same in preload and in SQLMesh.
        for stmt in [
            "INSTALL httpfs",
            "LOAD httpfs",
            "INSTALL parquet",
            "LOAD parquet",
            "SET enable_http_metadata_cache=true",
            "SET parquet_metadata_cache=true",
            "SET preserve_insertion_order=false",
            "SET memory_limit='48GB'",
        ]:
            db.execute(stmt)

        if force_reload:
            for name in _ENUM_DROP_ORDER:
                db.execute(f"DROP TYPE IF EXISTS {name}")
        for schema in sorted(schemas):
            db.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        db.commit()

        t0 = time.monotonic()

        def _run(item: tuple[str, str, str]) -> tuple[str, str, float]:
            schema, name, ddl = item
            ts = time.monotonic()
            cur = db.cursor()
            try:
                cur.execute(ddl)
            finally:
                cur.close()
            return schema, name, time.monotonic() - ts

        errors: list[BaseException] = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_run, it): it for it in items}
            for fut in as_completed(futures):
                try:
                    schema, name, elapsed = fut.result()
                    logger.info("preload: %s.%s in %.1fs", schema, name, elapsed)
                except BaseException as e:
                    sch, nm, _ = futures[fut]
                    logger.error("preload: %s.%s FAILED: %s", sch, nm, e)
                    errors.append(e)

        if errors:
            raise RuntimeError(f"preload: {len(errors)} task(s) failed; first: {errors[0]!r}")

        db.commit()
        logger.info("preload: all %d tables done in %.1fs", len(items), time.monotonic() - t0)
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--workers",
        type=int,
        default=int(os.environ.get("BC_INIT_DB_PARALLELISM", "8")),
        help="Concurrent worker count (default 8).",
    )
    p.add_argument(
        "--force-reload",
        action="store_true",
        help="DROP TYPE the ENUMs and CREATE OR REPLACE every source table.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        help="stdlib logging level (DEBUG, INFO, WARNING, ERROR).",
    )
    args = p.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    preload(
        workers=max(1, args.workers),
        force_reload=args.force_reload,
        roots=DEFAULT_SOURCE_ROOTS,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
