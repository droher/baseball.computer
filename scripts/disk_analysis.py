"""Per-table and per-column disk-usage analysis for the built bc.db.

Two outputs:

1. ``logs/disk/disk_tables.csv`` — every production table sorted by
   on-disk bytes (sum of unique 256 KB blocks across non-validity
   segments).
2. ``logs/disk/disk_columns.csv`` — per-column bytes for the same
   tables, ordered by table then by column bytes desc.

Bytes here are DuckDB's internal columnar storage (segments × block
size). Validity bitmaps are excluded; they fit in spare bits and add
noise to the per-column ranking. Block count × 256 KB matches the
``database_size`` pragma's ``used_blocks * block_size`` to within a
few percent at the table level.

Usage::

    uv run --group migration python scripts/disk_analysis.py
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from collections.abc import Iterable
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "bc.db"
OUT_DIR = PROJECT_ROOT / "logs" / "disk"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PHYSICAL_SCHEMA = "sqlmesh__main_models"
BLOCK_SIZE = 262144  # DuckDB default

logger = logging.getLogger("disk_analysis")


def fetch_tables(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str, int]]:
    """Return [(physical_table, model_name, row_count_estimate)] desc by row count."""
    rows: Iterable[tuple[object, ...]] = con.execute(
        """
        SELECT
            t.table_name,
            regexp_replace(
                regexp_replace(t.table_name, '^main_models__', ''),
                '__\\d+__dev$', ''
            ),
            t.estimated_size
        FROM duckdb_tables() t
        WHERE t.schema_name = ?
          AND t.table_name LIKE 'main_models__%__dev'
        ORDER BY t.estimated_size DESC NULLS LAST
        """,
        [PHYSICAL_SCHEMA],
    ).fetchall()
    return [(str(r[0]), str(r[1]), int(r[2] or 0)) for r in rows]


def per_column_blocks(
    con: duckdb.DuckDBPyConnection,
    physical_table: str,
) -> list[tuple[str, str, int, int]]:
    """Return [(column_name, dtype, unique_blocks, segment_count)] desc by blocks."""
    rows: Iterable[tuple[object, ...]] = con.execute(
        f"""
        SELECT
            column_name,
            ANY_VALUE(segment_type) AS dtype,
            COUNT(DISTINCT block_id) FILTER (WHERE block_id IS NOT NULL) AS blocks,
            COUNT(*) FILTER (WHERE block_id IS NOT NULL) AS segs
        FROM pragma_storage_info('{PHYSICAL_SCHEMA}.{physical_table}')
        WHERE segment_type NOT IN ('VALIDITY')
        GROUP BY column_name
        ORDER BY blocks DESC NULLS LAST
        """
    ).fetchall()
    return [
        (str(r[0]), str(r[1]), int(r[2] or 0), int(r[3] or 0))
        for r in rows
    ]


def write_tables_csv(rows: list[tuple[str, str, int, int]]) -> Path:
    out = OUT_DIR / "disk_tables.csv"
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["model_name", "physical_table", "row_count", "blocks", "bytes_mb"])
        for model, physical, rc, blocks in rows:
            w.writerow([model, physical, rc, blocks, round(blocks * BLOCK_SIZE / (1024**2), 1)])
    return out


def write_columns_csv(rows: list[tuple[str, str, str, int, int, int]]) -> Path:
    out = OUT_DIR / "disk_columns.csv"
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "model_name",
                "column",
                "dtype",
                "blocks",
                "bytes_mb",
                "table_row_count",
            ]
        )
        for model, col, dtype, blocks, _segs, rc in rows:
            w.writerow(
                [model, col, dtype, blocks, round(blocks * BLOCK_SIZE / (1024**2), 1), rc]
            )
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    _ = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if not DB_PATH.exists():
        logger.error("bc.db not found at %s — run the build first", DB_PATH)
        return 1

    _ = duckdb.connect(str(DB_PATH), read_only=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    logger.info("connected read-only to %s", DB_PATH)

    tables = fetch_tables(con)
    logger.info("found %d production tables", len(tables))

    table_records: list[tuple[str, str, int, int]] = []
    column_records: list[tuple[str, str, str, int, int, int]] = []

    for physical, model, row_count in tables:
        cols = per_column_blocks(con, physical)
        total_blocks = sum(b for _, _, b, _ in cols)
        table_records.append((model, physical, row_count, total_blocks))
        for col, dtype, blocks, segs in cols:
            column_records.append((model, col, dtype, blocks, segs, row_count))

    table_records.sort(key=lambda r: r[3], reverse=True)
    tables_path = write_tables_csv(table_records)
    cols_path = write_columns_csv(column_records)
    logger.info("wrote %s (%d rows)", tables_path, len(table_records))
    logger.info("wrote %s (%d rows)", cols_path, len(column_records))

    print()
    print("Top 15 tables by on-disk bytes:")
    print(f"{'model':<55} {'rows':>12} {'MB':>8}")
    for model, _phys, rc, blocks in table_records[:15]:
        print(f"{model:<55} {rc:>12,} {blocks * BLOCK_SIZE / (1024**2):>8.1f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
