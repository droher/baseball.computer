"""Publish main_models + main_seeds from bc.db into a DuckLake catalog.

Runs alongside scripts/create_web_db.py during validation — does not
displace it. scripts/upload_ducklake.py uploads the resulting catalog +
data files to R2.

Outputs:
  bc/bc_publish.ducklake     SQLite catalog (consumer attaches to this URL)
  bc/bc_publish_data/        parquet data files referenced by the catalog
  docs/llm/baseball.lsf      LSF-1 context packet, regenerated after publish

The catalog also carries a `semantic` schema with one view per BSL
semantic table and a `metrics` schema with one macro per metric name.
`--semantic-only` recreates those objects on an existing catalog without
copying any table.
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import shutil
import subprocess
import sys
import time
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BC_DB = PROJECT_ROOT / "bc.db"
BC_DIR = PROJECT_ROOT / "bc"

if str(BC_DIR) not in sys.path:
    sys.path.insert(0, str(BC_DIR))

from python_models.metrics.registry import (
    MetricKind,
    MetricSource,
    metrics_for,
)
from python_models.metrics.sql_render import (
    MacroSpec,
    MetricSlice,
    macro_specs,
)
from semantic._views import (
    SEMANTIC_SCHEMA,
    SEMANTIC_VIEWS,
    semantic_view_name,
    semantic_view_sql,
)

# Relative names — script chdirs to BC_DIR before ATTACH so the catalog
# stores relative paths. That keeps the catalog portable: uploaded as a
# blob alongside bc_publish_data/, consumers attach by URL and DuckLake
# resolves data files against the catalog URL's parent.
CATALOG_NAME = "bc_publish.ducklake"
DATA_DIR_NAME = "bc_publish_data"
CATALOG_PATH = BC_DIR / CATALOG_NAME
DATA_PATH = BC_DIR / DATA_DIR_NAME
DATA_VERSION_FILE = BC_DIR / "data_version.txt"
PUBLIC_HOST = "data.baseball.computer"
PACKET_PATH = PROJECT_ROOT / "docs" / "llm" / "baseball.lsf"
GENERATOR_PATH = PROJECT_ROOT / "scripts" / "generate_llm_context.py"

PUBLISH_SCHEMAS = ("main_models", "main_seeds")
METRICS_SCHEMA = "metrics"
METRIC_KINDS: tuple[MetricKind, ...] = ("offense", "pitching", "fielding")
METRIC_SOURCES: tuple[MetricSource, ...] = ("season", "event")
ESTIMATED_ROW_COUNT_FLOORS: dict[str, int] = {
    "scorer_observation_propensities": 100_000,
    "imputed_ball_handler_probabilities": 100_000,
    "imputed_batted_ball_geometry": 100_000,
    "imputed_fielding_credit": 100_000,
    "park_factor_summary": 500,
    "run_expectancy_summary": 1_000,
    "pitch_summary_distribution": 1_000,
    "state_transition_summary": 10_000,
    "linear_weights_estimated": 1_000,
    "assist_count_distribution": 100,
}
COMPRESSION = "zstd"
ROW_GROUP_SIZE = "1966080"
KEEP_LAST_N_SNAPSHOTS = 5
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


def public_data_path(data_version: str) -> str:
    return f"https://{PUBLIC_HOST}/baseball/v{data_version}/{DATA_DIR_NAME}/"


def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def attach_source_database(con: duckdb.DuckDBPyConnection, source_path: Path) -> None:
    _ = con.execute(
        f"ATTACH '{sql_literal(str(source_path.resolve()))}' AS bc (READ_ONLY)"
    )


_LIST_TABLES_SQL = (
    "SELECT table_name FROM information_schema.tables"
    " WHERE table_catalog = ? AND table_schema = ? AND table_type = 'BASE TABLE'"
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


def list_tables(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    catalog: str = "bc",
) -> list[str]:
    rows = con.execute(_LIST_TABLES_SQL, [catalog, schema]).fetchall()
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


def table_row_count(
    con: duckdb.DuckDBPyConnection, catalog: str, schema: str, table: str
) -> int:
    row = con.execute(
        f'SELECT COUNT(*) FROM "{catalog}"."{schema}"."{table}"'
    ).fetchone()
    if row is None:
        return 0
    count: object = row[0]
    return int(str(count))


def assert_estimated_tables_populated(
    con: duckdb.DuckDBPyConnection,
    *,
    catalog: str = "bc",
    floors: dict[str, int] = ESTIMATED_ROW_COUNT_FLOORS,
) -> dict[str, int]:
    """Refuse to publish estimated tables that fell back to their empty frame.

    Mirrors the ``min_row_count`` audit each of these ``@model``s declares;
    a build whose published pointers did not resolve yields zero rows, and
    this is the last stop before those rows reach R2.
    """
    counts: dict[str, int] = {}
    short: list[str] = []
    for table, floor in floors.items():
        count = table_row_count(con, catalog, "main_models", table)
        counts[table] = count
        _log.info("main_models.%s rows=%d floor=%d", table, count, floor)
        if count < floor:
            short.append(f"main_models.{table}: {count} rows < {floor}")
    if short:
        listing = "\n  ".join(short)
        raise SystemExit(
            "estimated tables below their row-count floor; refusing to publish."
            + " Check that BC_STATS_ARTIFACTS_ROOT resolved the published pointers"
            + f" before the SQLMesh plan.\n  {listing}"
        )
    return counts


def attach_catalog(
    con: duckdb.DuckDBPyConnection,
    *,
    data_version: str,
    read_only: bool = False,
) -> None:
    _ = con.execute("INSTALL ducklake")
    _ = con.execute("LOAD ducklake")
    if CATALOG_PATH.exists():
        assert_catalog_data_path(con, CATALOG_PATH, data_version)
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    suffix = ", READ_ONLY" if read_only else ""
    sql = (
        f"ATTACH 'ducklake:{CATALOG_NAME}' AS bc_publish"
        f" (DATA_PATH '{sql_literal(str(DATA_PATH))}/', OVERRIDE_DATA_PATH true{suffix})"
    )
    if not CATALOG_PATH.exists():
        _ = con.execute(
            f"ATTACH 'ducklake:{CATALOG_NAME}' AS bc_publish"
            f" (DATA_PATH '{public_data_path(data_version)}')"
        )
        _ = con.execute("DETACH bc_publish")
    _ = con.execute(sql)


def assert_catalog_data_path(
    con: duckdb.DuckDBPyConnection,
    catalog_path: Path,
    data_version: str,
) -> None:
    catalog = "bc_publish_validation"
    _ = con.execute(
        f"ATTACH 'ducklake:{sql_literal(str(catalog_path))}' AS {catalog} (READ_ONLY)"
    )
    row = con.execute(
        f"SELECT data_path FROM ducklake_settings('{catalog}')"
    ).fetchone()
    _ = con.execute(f"DETACH {catalog}")
    actual = str(row[0]) if row is not None else ""
    expected = public_data_path(data_version)
    if actual != expected:
        raise SystemExit(
            f"DuckLake catalog data path {actual!r} does not match {expected!r}; "
            "rerun with --reset to create a catalog for the current DATA_VERSION"
        )


def set_catalog_options(con: duckdb.DuckDBPyConnection) -> None:
    _ = con.execute(
        "CALL ducklake_set_option('bc_publish', 'target_file_size', '128MB')"
    )
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
    source_rows = table_row_count(con, "bc", schema, table)
    _ = con.execute(
        f"CREATE OR REPLACE TABLE {fqn_dst} AS SELECT {select_list} FROM {fqn_src}"
    )
    rowcount = table_row_count(con, "bc_publish", schema, table)
    if rowcount != source_rows:
        raise RuntimeError(
            f"published row-count mismatch for {schema}.{table}: "
            f"source={source_rows} published={rowcount}"
        )
    _log.info(
        "published %s.%s rows=%d enum_cols=%d",
        schema,
        table,
        rowcount,
        len(enum_cols),
    )
    return rowcount


def reconcile_published_tables(con: duckdb.DuckDBPyConnection) -> None:
    for schema in PUBLISH_SCHEMAS:
        source = set(list_tables(con, schema))
        published = set(list_tables(con, schema, "bc_publish"))
        for table in sorted(published - source):
            _ = con.execute(f'DROP TABLE "bc_publish"."{schema}"."{table}"')
            _log.info("dropped stale published table %s.%s", schema, table)


def metric_slices() -> list[MetricSlice]:
    return [
        (kind, source, metrics_for(kind, source))
        for kind in METRIC_KINDS
        for source in METRIC_SOURCES
    ]


_LIST_VIEWS_SQL = (
    "SELECT view_name FROM duckdb_views()"
    " WHERE database_name = 'bc_publish' AND schema_name = ? ORDER BY view_name"
)
_LIST_MACROS_SQL = (
    "SELECT DISTINCT function_name FROM duckdb_functions()"
    " WHERE database_name = 'bc_publish' AND schema_name = ? AND function_type = 'macro'"
    " ORDER BY function_name"
)


def list_views(con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
    return [r[0] for r in con.execute(_LIST_VIEWS_SQL, [schema]).fetchall()]


def list_macros(con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
    return [r[0] for r in con.execute(_LIST_MACROS_SQL, [schema]).fetchall()]


def publish_semantic_views(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Create the six semantic views; drop any view the layout no longer defines."""
    _ = con.execute(f'CREATE SCHEMA IF NOT EXISTS "bc_publish"."{SEMANTIC_SCHEMA}"')
    names: list[str] = []
    for name, kind, grain in SEMANTIC_VIEWS:
        _ = con.execute(
            f'CREATE OR REPLACE VIEW "bc_publish"."{SEMANTIC_SCHEMA}"."{name}" AS\n'
            + semantic_view_sql(kind, grain)
        )
        names.append(name)
        _log.info("published view %s.%s", SEMANTIC_SCHEMA, name)
    for stale in sorted(set(list_views(con, SEMANTIC_SCHEMA)) - set(names)):
        _ = con.execute(f'DROP VIEW "bc_publish"."{SEMANTIC_SCHEMA}"."{stale}"')
        _log.info("dropped stale view %s.%s", SEMANTIC_SCHEMA, stale)
    return names


def publish_metric_macros(
    con: duckdb.DuckDBPyConnection, specs: list[MacroSpec]
) -> list[str]:
    """Create one aggregate macro per metric name; drop macros no longer registered."""
    _ = con.execute(f'CREATE SCHEMA IF NOT EXISTS "bc_publish"."{METRICS_SCHEMA}"')
    names: list[str] = []
    for spec in specs:
        params = ", ".join(spec.params)
        _ = con.execute(
            f'CREATE OR REPLACE MACRO "bc_publish"."{METRICS_SCHEMA}"."{spec.name}"'
            f"({params}) AS {spec.body}"
        )
        names.append(spec.name)
    _log.info("published %d macros in %s", len(names), METRICS_SCHEMA)
    for stale in sorted(set(list_macros(con, METRICS_SCHEMA)) - set(names)):
        _ = con.execute(f'DROP MACRO "bc_publish"."{METRICS_SCHEMA}"."{stale}"')
        _log.info("dropped stale macro %s.%s", METRICS_SCHEMA, stale)
    return names


def publish_semantic_objects(con: duckdb.DuckDBPyConnection) -> None:
    _ = publish_semantic_views(con)
    _ = publish_metric_macros(con, macro_specs(metric_slices()))


def generate_packet() -> Path:
    """Regenerate docs/llm/baseball.lsf with the LSF-1 validator on."""
    command = [sys.executable, str(GENERATOR_PATH), "--validate"]
    _log.info("regenerating %s", PACKET_PATH)
    PACKET_PATH.unlink(missing_ok=True)
    _ = subprocess.run(command, check=True, cwd=PROJECT_ROOT)
    if not PACKET_PATH.exists():
        raise SystemExit(f"generator ran but {PACKET_PATH} is missing")
    return PACKET_PATH


def assert_catalog_exists(catalog_path: Path) -> None:
    if not catalog_path.exists():
        raise SystemExit(
            f"catalog not found at {catalog_path}; run publish_ducklake.py first"
        )


def expire_snapshots(con: duckdb.DuckDBPyConnection) -> None:
    """Expire all snapshots except the most recent KEEP_LAST_N_SNAPSHOTS.

    DuckLake's `older_than` cutoff doesn't fit a daily-dispatch cadence —
    we want a fixed working set, not a time window. Query the snapshots
    catalogue, take everything past the keep-window, and pass the IDs
    explicitly via `versions`.
    """
    rows = con.execute(
        "SELECT snapshot_id FROM ducklake_snapshots('bc_publish') ORDER BY snapshot_id DESC"
    ).fetchall()
    all_ids = [int(r[0]) for r in rows]
    expire_ids = all_ids[KEEP_LAST_N_SNAPSHOTS:]
    if not expire_ids:
        _log.info(
            "snapshot count %d <= keep %d, nothing to expire",
            len(all_ids),
            KEEP_LAST_N_SNAPSHOTS,
        )
        return
    versions_literal = ", ".join(str(i) for i in expire_ids)
    expired = con.execute(
        f"CALL ducklake_expire_snapshots('bc_publish', versions => [{versions_literal}])"
    ).fetchall()
    _log.info(
        "expired %d snapshots (kept %d, total before=%d)",
        len(expired),
        KEEP_LAST_N_SNAPSHOTS,
        len(all_ids),
    )


def report_sizes() -> tuple[int, int]:
    catalog_bytes = CATALOG_PATH.stat().st_size if CATALOG_PATH.exists() else 0
    data_bytes = sum(f.stat().st_size for f in DATA_PATH.rglob("*") if f.is_file())
    _log.info(
        "artifact sizes: catalog=%.1f MB, data_path=%.1f MB",
        catalog_bytes / 1e6,
        data_bytes / 1e6,
    )
    return catalog_bytes, data_bytes


def smoke_check() -> None:
    assert_catalog_exists(CATALOG_PATH)
    with contextlib.chdir(BC_DIR):
        con = duckdb.connect(":memory:")
        attach_catalog(con, data_version=read_data_version(), read_only=True)
        _smoke_run(con)


def _smoke_run(con: duckdb.DuckDBPyConnection) -> None:
    sample = con.execute(_SAMPLE_TABLES_SQL, [SMOKE_SAMPLE]).fetchall()
    if not sample:
        raise RuntimeError("smoke check found no published tables")
    for schema, table in sample:
        row = con.execute(
            f'SELECT * FROM "bc_publish"."{schema}"."{table}" LIMIT 1'
        ).fetchone()
        if row is None:
            raise RuntimeError(f"smoke query returned no rows for {schema}.{table}")
        cols = con.execute(f'DESCRIBE "bc_publish"."{schema}"."{table}"').fetchall()
        type_set = sorted({c[1] for c in cols})
        _log.info(
            "DESCRIBE bc_publish.%s.%s cols=%d distinct_types=%s",
            schema,
            table,
            len(cols),
            type_set,
        )

    for name, kind, grain in SEMANTIC_VIEWS:
        row = con.execute(
            f'SELECT * FROM "bc_publish"."{SEMANTIC_SCHEMA}"."{name}" LIMIT 1'
        ).fetchone()
        if row is None:
            raise RuntimeError(f"semantic view {name} returned no rows")
        _log.info("view %s.%s ok (%s/%s)", SEMANTIC_SCHEMA, name, kind, grain)

    for spec in macro_specs(metric_slices()):
        kind, source = spec.members[0]
        view = semantic_view_name(kind, source)
        args = ", ".join(spec.params)
        _ = con.execute(
            f'SELECT "bc_publish"."{METRICS_SCHEMA}"."{spec.name}"({args})'
            f' FROM (SELECT * FROM "bc_publish"."{SEMANTIC_SCHEMA}"."{view}" LIMIT 1000)'
        ).fetchone()
    _log.info("all metric macros resolve against their semantic views")

    snaps = con.execute("FROM ducklake_snapshots('bc_publish')").fetchall()
    _log.info("snapshots in bc_publish: %d", len(snaps))


def publish() -> None:
    if not BC_DB.exists():
        raise SystemExit(f"source database not found: {BC_DB}")
    data_version = read_data_version()
    _log.info("publishing DATA_VERSION=%s from %s", data_version, BC_DB)
    bc_db_abs = str(BC_DB.resolve())

    with contextlib.chdir(BC_DIR):
        con = duckdb.connect(":memory:")
        attach_source_database(con, Path(bc_db_abs))
        _ = assert_estimated_tables_populated(con)
        attach_catalog(con, data_version=data_version)
        set_catalog_options(con)

        for schema in PUBLISH_SCHEMAS:
            _ = con.execute(f'CREATE SCHEMA IF NOT EXISTS "bc_publish"."{schema}"')
        reconcile_published_tables(con)

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

        publish_semantic_objects(con)
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
    _ = generate_packet()


def publish_semantic() -> None:
    """Recreate the semantic views and metric macros on the existing catalog."""
    assert_catalog_exists(CATALOG_PATH)
    data_version = read_data_version()
    with contextlib.chdir(BC_DIR):
        con = duckdb.connect(":memory:")
        attach_catalog(con, data_version=data_version)
        publish_semantic_objects(con)
        expire_snapshots(con)
        con.close()
    _ = generate_packet()


def reset_artifacts() -> None:
    if CATALOG_PATH.exists():
        CATALOG_PATH.unlink()
        _log.info("removed %s", CATALOG_PATH)
    if DATA_PATH.exists():
        shutil.rmtree(DATA_PATH)
        _log.info("removed %s", DATA_PATH)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--smoke-only",
        action="store_true",
        help="Skip publish; only run the re-attach smoke check on the existing catalog.",
    )
    mode.add_argument(
        "--reset",
        action="store_true",
        help="Remove the local catalog file + data path before publishing (forces a fresh first snapshot).",
    )
    mode.add_argument(
        "--semantic-only",
        action="store_true",
        help="Skip table copies; recreate the semantic views, metric macros, and packet on the existing catalog.",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.reset:
        reset_artifacts()
    if args.semantic_only:
        publish_semantic()
    elif not args.smoke_only:
        publish()
    smoke_check()
    return 0


if __name__ == "__main__":
    sys.exit(main())
