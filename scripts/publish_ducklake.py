"""Publish main_models + main_seeds from bc.db into a DuckLake catalog.

Phase 4 publish target. Runs alongside scripts/create_web_db.py during the
validation phase — does not replace it. scripts/upload_ducklake.py uploads
the resulting catalog + data files to R2.

Outputs:
  bc/bc_publish.ducklake     SQLite catalog (consumer attaches to this URL)
  bc/bc_publish_data/        parquet data files referenced by the catalog
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import shutil
import sys
import time
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BC_DB = PROJECT_ROOT / "bc.db"
BC_DIR = PROJECT_ROOT / "bc"
# Relative names — script chdirs to BC_DIR before ATTACH so the catalog
# stores relative paths. That keeps the catalog portable: uploaded as a
# blob alongside bc_publish_data/, consumers attach by URL and DuckLake
# resolves data files against the catalog URL's parent.
CATALOG_NAME = "bc_publish.ducklake"
DATA_DIR_NAME = "bc_publish_data"
CATALOG_PATH = BC_DIR / CATALOG_NAME
DATA_PATH = BC_DIR / DATA_DIR_NAME
DATA_VERSION_FILE = BC_DIR / "data_version.txt"

PUBLISH_SCHEMAS = ("main_models", "main_seeds")
COMPRESSION = "zstd"
ROW_GROUP_SIZE = "1966080"
SNAPSHOT_RETENTION = "30 days"
SMOKE_SAMPLE = 5
# Force every row to land in parquet files so R2 has the full artifact.
# Default inlining keeps small tables inside the catalog DuckDB; we want
# the on-wire layout to be uniform across all 122 tables.
DATA_INLINING_ROW_LIMIT = "0"

_log = logging.getLogger("publish_ducklake")


def read_data_version() -> str:
    text = DATA_VERSION_FILE.read_text().strip()
    if not text.isdigit() or int(text) < 1:
        raise SystemExit(
            f"{DATA_VERSION_FILE} must contain a positive integer, got {text!r}"
        )
    return text


_LIST_TABLES_SQL = (
    "SELECT table_name FROM information_schema.tables"
    " WHERE table_catalog = 'bc' AND table_schema = ? AND table_type = 'BASE TABLE'"
    " ORDER BY table_name"
)
_ENUM_TYPES_SQL = (
    "SELECT type_name FROM duckdb_types()"
    " WHERE database_name = 'bc' AND logical_type = 'ENUM'"
)
_COLUMN_TYPES_SQL = (
    "SELECT column_name, data_type FROM information_schema.columns"
    " WHERE table_catalog = 'bc' AND table_schema = ? AND table_name = ?"
    " ORDER BY ordinal_position"
)
_SAMPLE_TABLES_SQL = (
    "SELECT table_schema, table_name FROM information_schema.tables"
    " WHERE table_catalog = 'bc_publish' AND table_type = 'BASE TABLE'"
    " ORDER BY table_schema, table_name LIMIT ?"
)


def list_tables(con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
    rows = con.execute(_LIST_TABLES_SQL, [schema]).fetchall()
    return [r[0] for r in rows]


def enum_type_count(con: duckdb.DuckDBPyConnection) -> int:
    """Count of ENUM types registered in bc.db (for logging only).

    DuckDB inlines ENUM definitions in `information_schema.columns.data_type`
    even when the column was typed via a named user-defined type, so we
    detect ENUM columns by the inline `ENUM(...)` prefix rather than
    matching type names. This count is only logged.
    """
    rows = con.execute(_ENUM_TYPES_SQL).fetchall()
    return len(rows)


def column_types(
    con: duckdb.DuckDBPyConnection, schema: str, table: str
) -> list[tuple[str, str]]:
    rows = con.execute(_COLUMN_TYPES_SQL, [schema, table]).fetchall()
    return [(r[0], r[1]) for r in rows]


def is_enum(data_type: str) -> bool:
    return data_type.startswith("ENUM(")


def select_with_enum_casts(cols: list[tuple[str, str]]) -> str:
    parts: list[str] = []
    for name, dtype in cols:
        quoted = f'"{name}"'
        if is_enum(dtype):
            parts.append(f"CAST({quoted} AS VARCHAR) AS {quoted}")
        else:
            parts.append(quoted)
    return ", ".join(parts)


def attach_catalog(
    con: duckdb.DuckDBPyConnection, *, read_only: bool = False
) -> None:
    """ATTACH the local DuckLake catalog using relative paths.

    Caller must have chdir'd to BC_DIR first so the relative paths resolve
    correctly. We use relative paths (not absolute) so the catalog file is
    portable: when uploaded to R2 alongside the data dir, consumers can
    attach by URL and DuckLake resolves data files against the catalog
    URL's parent.
    """
    _ = con.execute("INSTALL ducklake")
    _ = con.execute("LOAD ducklake")
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    suffix = ", READ_ONLY" if read_only else ""
    sql = (
        f"ATTACH 'ducklake:{CATALOG_NAME}' AS bc_publish"
        f" (DATA_PATH '{DATA_DIR_NAME}/'{suffix})"
    )
    _ = con.execute(sql)


def set_catalog_options(con: duckdb.DuckDBPyConnection) -> None:
    _ = con.execute(
        "CALL ducklake_set_option('bc_publish', 'parquet_compression', ?)",
        [COMPRESSION],
    )
    _ = con.execute(
        "CALL ducklake_set_option('bc_publish', 'parquet_row_group_size', ?)",
        [ROW_GROUP_SIZE],
    )
    _ = con.execute(
        "CALL ducklake_set_option('bc_publish', 'data_inlining_row_limit', ?)",
        [DATA_INLINING_ROW_LIMIT],
    )


def publish_table(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
) -> int:
    cols = column_types(con, schema, table)
    enum_cols = [name for name, dtype in cols if is_enum(dtype)]
    select_list = select_with_enum_casts(cols)
    fqn_src = f'"bc"."{schema}"."{table}"'
    fqn_dst = f'"bc_publish"."{schema}"."{table}"'
    _ = con.execute(
        f"CREATE OR REPLACE TABLE {fqn_dst} AS SELECT {select_list} FROM {fqn_src}"
    )
    row = con.execute(f"SELECT COUNT(*) FROM {fqn_dst}").fetchone()
    rowcount = int(row[0]) if row else 0
    _log.info(
        "published %s.%s rows=%d enum_cols=%d",
        schema,
        table,
        rowcount,
        len(enum_cols),
    )
    return rowcount


def expire_snapshots(con: duckdb.DuckDBPyConnection) -> None:
    sql = (
        "CALL ducklake_expire_snapshots('bc_publish',"
        f" older_than => now() - INTERVAL '{SNAPSHOT_RETENTION}')"
    )
    rows = con.execute(sql).fetchall()
    _log.info("expire_snapshots returned %d rows", len(rows))


def report_sizes() -> tuple[int, int]:
    catalog_bytes = CATALOG_PATH.stat().st_size if CATALOG_PATH.exists() else 0
    data_bytes = sum(
        f.stat().st_size for f in DATA_PATH.rglob("*") if f.is_file()
    )
    _log.info(
        "artifact sizes: catalog=%.1f MB, data_path=%.1f MB",
        catalog_bytes / 1e6,
        data_bytes / 1e6,
    )
    return catalog_bytes, data_bytes


def smoke_check() -> None:
    with contextlib.chdir(BC_DIR):
        con = duckdb.connect(":memory:")
        attach_catalog(con, read_only=True)
        _smoke_run(con)


def _smoke_run(con: duckdb.DuckDBPyConnection) -> None:
    sample = con.execute(_SAMPLE_TABLES_SQL, [SMOKE_SAMPLE]).fetchall()
    for schema, table in sample:
        cols = con.execute(
            f'DESCRIBE "bc_publish"."{schema}"."{table}"'
        ).fetchall()
        type_set = sorted({c[1] for c in cols})
        _log.info(
            "DESCRIBE bc_publish.%s.%s cols=%d distinct_types=%s",
            schema,
            table,
            len(cols),
            type_set,
        )

    snaps = con.execute("FROM ducklake_snapshots('bc_publish')").fetchall()
    _log.info("snapshots in bc_publish: %d", len(snaps))


def publish() -> None:
    if not BC_DB.exists():
        raise SystemExit(f"source database not found: {BC_DB}")
    data_version = read_data_version()
    _log.info("publishing DATA_VERSION=%s from %s", data_version, BC_DB)
    bc_db_abs = str(BC_DB.resolve())

    with contextlib.chdir(BC_DIR):
        con = duckdb.connect(bc_db_abs)
        attach_catalog(con)
        set_catalog_options(con)

        for schema in PUBLISH_SCHEMAS:
            _ = con.execute(f'CREATE SCHEMA IF NOT EXISTS "bc_publish"."{schema}"')

        n_enums = enum_type_count(con)
        _log.info(
            "detected %d ENUM types in bc.db (cast to VARCHAR on publish)", n_enums
        )

        total_tables = 0
        total_rows = 0
        started = time.monotonic()
        for schema in PUBLISH_SCHEMAS:
            tables = list_tables(con, schema)
            _log.info("schema %s: %d tables", schema, len(tables))
            for table in tables:
                total_rows += publish_table(con, schema, table)
                total_tables += 1

        expire_snapshots(con)
        con.close()

    elapsed = time.monotonic() - started
    _log.info(
        "published %d tables (%d rows total) in %.1fs",
        total_tables,
        total_rows,
        elapsed,
    )
    report_sizes()


def reset_artifacts() -> None:
    if CATALOG_PATH.exists():
        CATALOG_PATH.unlink()
        _log.info("removed %s", CATALOG_PATH)
    if DATA_PATH.exists():
        shutil.rmtree(DATA_PATH)
        _log.info("removed %s", DATA_PATH)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--smoke-only",
        action="store_true",
        help="Skip publish; only run the re-attach smoke check on the existing catalog.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Remove the local catalog file + data path before publishing (forces a fresh first snapshot).",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.reset:
        reset_artifacts()
    if not args.smoke_only:
        publish()
    smoke_check()
    return 0


if __name__ == "__main__":
    sys.exit(main())
