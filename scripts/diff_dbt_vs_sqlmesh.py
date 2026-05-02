"""Cross-engine diff between a dbt-built and SQLMesh-built bc.db.

Phase 1 verification harness. Walks the union of tables in both DuckDB
files, compares row counts + column sets + an order-independent row hash,
and exits non-zero on any mismatch.

Hashing strategy: per-column canonicalization, then hash a single
record-separator-joined string per row.

  - Numeric (DECIMAL/NUMERIC/DOUBLE/REAL/FLOAT) is cast to DOUBLE then
    VARCHAR so DECIMAL(p,s) precision drift between engines (which only
    affects the type, not the values) does not register as a diff.
  - ENUM is cast to VARCHAR so the digest depends on the label, not the
    ordinal (which differs across builds).
  - Everything else is cast to VARCHAR.

NULLs are replaced with `CHR(0)` per column; columns are joined with
`CHR(31)` (unit separator) so `'a|b'` does not collide with two cols
`'a'`, `'b'`.

Usage:
    python scripts/diff_dbt_vs_sqlmesh.py \\
        --dbt-db bc_dbt.db --sqlmesh-db bc_sqlmesh.db [--schema NAME]
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb

logger = logging.getLogger("diff")


@dataclass(frozen=True)
class TableVerdict:
    schema: str
    table: str
    only_in_dbt: bool = False
    only_in_sqlmesh: bool = False
    row_count_dbt: int | None = None
    row_count_sqlmesh: int | None = None
    cols_only_in_dbt: tuple[str, ...] = ()
    cols_only_in_sqlmesh: tuple[str, ...] = ()
    type_diffs: tuple[tuple[str, str, str], ...] = ()  # (col, dbt_type, sqlmesh_type)
    sum_hash_dbt: int | None = None
    sum_hash_sqlmesh: int | None = None
    xor_hash_dbt: int | None = None
    xor_hash_sqlmesh: int | None = None
    error: str | None = None

    @property
    def matches(self) -> bool:
        if self.only_in_dbt or self.only_in_sqlmesh or self.error:
            return False
        if self.row_count_dbt != self.row_count_sqlmesh:
            return False
        if self.cols_only_in_dbt or self.cols_only_in_sqlmesh:
            return False
        if self.type_diffs:
            return False
        if self.sum_hash_dbt != self.sum_hash_sqlmesh:
            return False
        if self.xor_hash_dbt != self.xor_hash_sqlmesh:
            return False
        return True


_TABLE_QUERY = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type IN ('BASE TABLE', 'VIEW')
"""

_COLUMNS_QUERY = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = ? AND table_name = ?
    ORDER BY ordinal_position
"""

_NUMERIC_PREFIXES = ("DECIMAL", "NUMERIC")
_NUMERIC_TYPES = {"DOUBLE", "REAL", "FLOAT"}


def _base_type(data_type: str) -> str:
    """Strip parameters so DECIMAL(2,2) and DECIMAL(3,2) compare equal.

    Engines that widen precision on the same numeric column are not
    type-diverging in the sense the harness cares about; the canonical
    hash absorbs the value-level equivalence already.
    """
    return data_type.upper().split("(", 1)[0].strip()


def _list_tables(conn: duckdb.DuckDBPyConnection, schema: str | None) -> set[tuple[str, str]]:
    sql = _TABLE_QUERY
    params: list[str] = []
    if schema:
        sql += " AND table_schema = ?"
        params.append(schema)
    rows = conn.execute(sql, params).fetchall()
    return {(r[0], r[1]) for r in rows}


def _list_columns(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[tuple[str, str]]:
    rows = conn.execute(_COLUMNS_QUERY, [schema, table]).fetchall()
    return [(r[0], r[1]) for r in rows]


def _column_expr(name: str, data_type: str) -> str:
    """Return the canonical VARCHAR expression for one column.

    Float-family types round-trip through DOUBLE so DECIMAL(p,s) drift
    between engines (where the type widens but values are identical)
    does not register as a diff.
    """
    quoted = f'"{name}"'
    upper = data_type.upper()
    if upper.startswith(_NUMERIC_PREFIXES) or upper in _NUMERIC_TYPES:
        cast_expr = f"CAST(CAST({quoted} AS DOUBLE) AS VARCHAR)"
    else:
        cast_expr = f"CAST({quoted} AS VARCHAR)"
    return f"COALESCE({cast_expr}, CHR(0))"


def _row_hash(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
) -> tuple[int, int, int]:
    """Return (count, sum_hash, xor_hash) over an order-independent digest.

    Columns are sorted by name before being concatenated, so engines that
    happen to materialize the same logical schema with a different column
    ordinal_position do not register as a diff. Per-row HASH still binds
    cells within a row together, so a swap of values between rows would
    still register.
    """
    qualified = f'"{schema}"."{table}"'
    if not columns:
        sql = f"SELECT COUNT(*) AS cnt, 0 AS sum_hash, 0 AS xor_hash FROM {qualified}"
        row = conn.execute(sql).fetchone()
        assert row is not None
        return int(row[0]), 0, 0
    sorted_cols = sorted(columns, key=lambda c: c[0])
    col_exprs = ", ".join(_column_expr(n, t) for n, t in sorted_cols)
    row_expr = f"HASH(concat_ws(CHR(31), {col_exprs}))::HUGEINT"
    sql = (
        f"SELECT COUNT(*) AS cnt, "
        f"COALESCE(SUM({row_expr}), 0) AS sum_hash, "
        f"COALESCE(BIT_XOR({row_expr}), 0) AS xor_hash "
        f"FROM {qualified}"
    )
    row = conn.execute(sql).fetchone()
    assert row is not None
    return int(row[0]), int(row[1]), int(row[2])


def _log_verdict(v: TableVerdict) -> None:
    qn = f"{v.schema}.{v.table}"
    if v.matches:
        logger.info("MATCH  %s rows=%s", qn, v.row_count_dbt)
        return
    if v.only_in_dbt:
        logger.error("MISSING-IN-SQLMESH  %s", qn)
        return
    if v.only_in_sqlmesh:
        logger.error("MISSING-IN-DBT  %s", qn)
        return
    if v.error:
        logger.error("ERROR  %s: %s", qn, v.error)
        return
    if v.row_count_dbt != v.row_count_sqlmesh:
        logger.warning(
            "ROW-COUNT-MISMATCH  %s dbt=%s sqlmesh=%s",
            qn,
            v.row_count_dbt,
            v.row_count_sqlmesh,
        )
    if v.cols_only_in_dbt or v.cols_only_in_sqlmesh:
        logger.warning(
            "COLUMN-DIFF  %s only_in_dbt=%s only_in_sqlmesh=%s",
            qn,
            list(v.cols_only_in_dbt),
            list(v.cols_only_in_sqlmesh),
        )
    if v.type_diffs:
        logger.error("TYPE-DIFF  %s %s", qn, list(v.type_diffs))
    if v.sum_hash_dbt != v.sum_hash_sqlmesh or v.xor_hash_dbt != v.xor_hash_sqlmesh:
        logger.error(
            "HASH-MISMATCH  %s sum_dbt=%s sum_sm=%s xor_dbt=%s xor_sm=%s",
            qn,
            v.sum_hash_dbt,
            v.sum_hash_sqlmesh,
            v.xor_hash_dbt,
            v.xor_hash_sqlmesh,
        )


def run_diff(
    dbt_db: Path,
    sqlmesh_db: Path,
    dbt_schema: str | None = None,
    sqlmesh_schema: str | None = None,
) -> list[TableVerdict]:
    """Walk every table common to both sides and emit a verdict per table.

    `dbt_schema` and `sqlmesh_schema` may differ — useful when both engines
    write into the same DuckDB file but under different schemas (e.g.
    `main_models` vs `main_models__dev`). Verdicts are reported under the
    dbt-side schema name to keep the output stable.
    """
    logger.info(
        "opening dbt=%s:%s sqlmesh=%s:%s",
        dbt_db,
        dbt_schema or "<all>",
        sqlmesh_db,
        sqlmesh_schema or "<all>",
    )
    dbt_conn = duckdb.connect(str(dbt_db), read_only=True)
    sm_conn = duckdb.connect(str(sqlmesh_db), read_only=True)
    try:
        dbt_raw = _list_tables(dbt_conn, dbt_schema)
        sm_raw = _list_tables(sm_conn, sqlmesh_schema)

        # Strip schema so cross-schema comparisons line up by table name.
        dbt_names = {t for _, t in dbt_raw}
        sm_names = {t for _, t in sm_raw}
        all_names = sorted(dbt_names | sm_names)
        logger.info(
            "found %d tables (%d in dbt, %d in sqlmesh, %d shared)",
            len(all_names),
            len(dbt_names),
            len(sm_names),
            len(dbt_names & sm_names),
        )

        report_schema = dbt_schema or sqlmesh_schema or ""
        verdicts: list[TableVerdict] = []
        for name in all_names:
            in_dbt = name in dbt_names
            in_sm = name in sm_names
            # Normalize schema-qualified lookups per side.
            v = _diff_pair(
                dbt_conn,
                sm_conn,
                report_schema=report_schema,
                table=name,
                dbt_schema=dbt_schema,
                sm_schema=sqlmesh_schema,
                in_dbt=in_dbt,
                in_sm=in_sm,
            )
            _log_verdict(v)
            verdicts.append(v)
        return verdicts
    finally:
        dbt_conn.close()
        sm_conn.close()


def _diff_pair(
    dbt_conn: duckdb.DuckDBPyConnection,
    sm_conn: duckdb.DuckDBPyConnection,
    *,
    report_schema: str,
    table: str,
    dbt_schema: str | None,
    sm_schema: str | None,
    in_dbt: bool,
    in_sm: bool,
) -> TableVerdict:
    if in_dbt and not in_sm:
        return TableVerdict(schema=report_schema, table=table, only_in_dbt=True)
    if in_sm and not in_dbt:
        return TableVerdict(schema=report_schema, table=table, only_in_sqlmesh=True)
    if not dbt_schema or not sm_schema:
        # Need explicit schemas to address tables on both sides.
        return TableVerdict(
            schema=report_schema,
            table=table,
            error="both --dbt-schema and --sqlmesh-schema must be set",
        )
    try:
        dbt_cols = _list_columns(dbt_conn, dbt_schema, table)
        sm_cols = _list_columns(sm_conn, sm_schema, table)
        dbt_names = [n for n, _ in dbt_cols]
        sm_names = [n for n, _ in sm_cols]
        dbt_types = dict(dbt_cols)
        sm_types = dict(sm_cols)
        cols_only_in_dbt = tuple(c for c in dbt_names if c not in sm_names)
        cols_only_in_sqlmesh = tuple(c for c in sm_names if c not in dbt_names)
        # Detect base-type divergence on shared columns. Per-column hash
        # canonicalization absorbs DECIMAL precision drift and ENUM
        # ordinal differences but would mask e.g. INTEGER vs VARCHAR.
        type_diffs: tuple[tuple[str, str, str], ...] = tuple(
            (c, dbt_types[c], sm_types[c])
            for c in dbt_names
            if c in sm_types and _base_type(dbt_types[c]) != _base_type(sm_types[c])
        )
        cnt_d, sh_d, xh_d = _row_hash(dbt_conn, dbt_schema, table, dbt_cols)
        cnt_s, sh_s, xh_s = _row_hash(sm_conn, sm_schema, table, sm_cols)
        return TableVerdict(
            schema=report_schema,
            table=table,
            row_count_dbt=cnt_d,
            row_count_sqlmesh=cnt_s,
            cols_only_in_dbt=cols_only_in_dbt,
            cols_only_in_sqlmesh=cols_only_in_sqlmesh,
            type_diffs=type_diffs,
            sum_hash_dbt=sh_d,
            sum_hash_sqlmesh=sh_s,
            xor_hash_dbt=xh_d,
            xor_hash_sqlmesh=xh_s,
        )
    except Exception as e:
        return TableVerdict(schema=report_schema, table=table, error=str(e))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dbt-db", type=Path, required=True)
    p.add_argument("--sqlmesh-db", type=Path, required=True)
    p.add_argument("--dbt-schema", type=str, default=None,
                   help="dbt-side schema (e.g. main_models)")
    p.add_argument("--sqlmesh-schema", type=str, default=None,
                   help="sqlmesh-side schema (e.g. main_models__dev)")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Enable DEBUG logging")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    verdicts = run_diff(
        args.dbt_db,
        args.sqlmesh_db,
        dbt_schema=args.dbt_schema,
        sqlmesh_schema=args.sqlmesh_schema,
    )
    mismatches = [v for v in verdicts if not v.matches]
    matched = len(verdicts) - len(mismatches)
    logger.info("summary: %d matched, %d mismatched (of %d total)", matched, len(mismatches), len(verdicts))
    return 0 if not mismatches else 1


if __name__ == "__main__":
    sys.exit(main())
