"""Split-registry leakage checks for frozen modeling-dataset snapshots.

A modeling dataset leaks when a unit that defines a split or stress
holdout splits across multiple fold/flag values. For the doc-02 split
policy every assignment is deterministic from a keying covariate, so
leakage indicates either a policy drift, a dataset bug, or a SQL view
that combined incompatible sources.

The module reads a frozen ``dataset.parquet`` via DuckDB ``read_parquet``
(no live DB), runs one ``GROUP BY`` per checked unit, and returns a tuple
of :class:`LeakageViolation` rows. Empty tuple = clean dataset.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import duckdb
import polars as pl
from pydantic import BaseModel

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class LeakageUnit:
    """A single (assignment_column, unit_columns) leakage check.

    ``assignment_column`` is the dotted path into the row that should be a
    pure function of ``unit_columns``. ``struct_field`` is non-None when
    the column lives inside a DuckDB STRUCT; in that case
    ``assignment_column`` is the struct column and ``struct_field`` is the
    field name inside it.
    """

    name: str
    assignment_column: str
    unit_columns: tuple[str, ...]
    struct_field: str | None = None


class LeakageViolation(BaseModel):
    unit_kind: str
    unit_id: str
    distinct_value_count: int
    distinct_values: tuple[str, ...]
    row_count: int


_HOLDOUT_STRUCT_COLUMN: str = "holdout_flags"


PRIMARY_FOLD_UNIT: LeakageUnit = LeakageUnit(
    name="primary_fold",
    assignment_column="primary_fold",
    unit_columns=("game_id",),
)


STRESS_HOLDOUT_UNITS: tuple[LeakageUnit, ...] = (
    LeakageUnit(
        name="is_heldout_scorer",
        assignment_column=_HOLDOUT_STRUCT_COLUMN,
        struct_field="is_heldout_scorer",
        unit_columns=("scorer",),
    ),
    LeakageUnit(
        name="is_heldout_park",
        assignment_column=_HOLDOUT_STRUCT_COLUMN,
        struct_field="is_heldout_park",
        unit_columns=("park_id", "season"),
    ),
    LeakageUnit(
        name="is_heldout_alignment_regime",
        assignment_column=_HOLDOUT_STRUCT_COLUMN,
        struct_field="is_heldout_alignment_regime",
        unit_columns=("alignment_regime",),
    ),
    LeakageUnit(
        name="is_heldout_source_acquisition_block",
        assignment_column=_HOLDOUT_STRUCT_COLUMN,
        struct_field="is_heldout_source_acquisition_block",
        unit_columns=("source_type", "season"),
    ),
    LeakageUnit(
        name="is_heldout_season_block",
        assignment_column=_HOLDOUT_STRUCT_COLUMN,
        struct_field="is_heldout_season_block",
        unit_columns=("season",),
    ),
    LeakageUnit(
        name="is_heldout_aggregate_total",
        assignment_column=_HOLDOUT_STRUCT_COLUMN,
        struct_field="is_heldout_aggregate_total",
        unit_columns=("game_id", "fielding_team_id"),
    ),
)


ALL_LEAKAGE_UNITS: tuple[LeakageUnit, ...] = (PRIMARY_FOLD_UNIT, *STRESS_HOLDOUT_UNITS)


_SAMPLE_VALUE_LIMIT: int = 5


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _from_clause(parquet_path: Path) -> str:
    return f"read_parquet({_quote_literal(str(parquet_path))})"


def _describe_columns(
    con: duckdb.DuckDBPyConnection, parquet_path: Path
) -> dict[str, str]:
    rows = con.execute(
        f"DESCRIBE SELECT * FROM {_from_clause(parquet_path)}"
    ).fetchall()
    out: dict[str, str] = {}
    for r in rows:
        name = r[0]
        dtype = r[1]
        out[str(name)] = str(dtype)
    return out


def _holdout_struct_fields(
    con: duckdb.DuckDBPyConnection, parquet_path: Path
) -> set[str]:
    sample = con.execute(
        f"SELECT {_HOLDOUT_STRUCT_COLUMN} FROM {_from_clause(parquet_path)} "
        f"WHERE {_HOLDOUT_STRUCT_COLUMN} IS NOT NULL LIMIT 1"
    ).fetchone()
    if sample is None:
        return set()
    payload = sample[0]
    if isinstance(payload, dict):
        keys: set[str] = set()
        for k in payload:
            keys.add(str(k))
        return keys
    return set()


def _applicable_units(
    columns: dict[str, str],
    struct_fields: set[str],
    units: Sequence[LeakageUnit],
) -> list[LeakageUnit]:
    out: list[LeakageUnit] = []
    for unit in units:
        if any(c not in columns for c in unit.unit_columns):
            continue
        if unit.struct_field is None:
            if unit.assignment_column not in columns:
                continue
        else:
            if unit.assignment_column not in columns:
                continue
            if not struct_fields:
                continue
            if unit.struct_field not in struct_fields:
                continue
        out.append(unit)
    return out


def _assignment_expr(unit: LeakageUnit) -> str:
    if unit.struct_field is None:
        return f"{_quote_ident(unit.assignment_column)}::VARCHAR"
    return (
        f"({_quote_ident(unit.assignment_column)}."
        f"{_quote_ident(unit.struct_field)})::VARCHAR"
    )


def _unit_id_expr(unit: LeakageUnit) -> str:
    parts = [
        f"COALESCE({_quote_ident(c)}::VARCHAR, '__NULL__')" for c in unit.unit_columns
    ]
    if len(parts) == 1:
        return parts[0]
    return "CONCAT_WS('|', " + ", ".join(parts) + ")"


def _unit_not_null_predicate(unit: LeakageUnit) -> str:
    if not unit.unit_columns:
        return "TRUE"
    return " AND ".join(f"{_quote_ident(c)} IS NOT NULL" for c in unit.unit_columns)


def _run_unit_check(
    con: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    unit: LeakageUnit,
) -> tuple[LeakageViolation, ...]:
    assign_expr = _assignment_expr(unit)
    unit_id_expr = _unit_id_expr(unit)
    where = _unit_not_null_predicate(unit)
    sql = (
        f"WITH grouped AS ( "
        f"  SELECT {unit_id_expr} AS unit_id, "
        f"         COUNT(*) AS row_count, "
        f"         COUNT(DISTINCT {assign_expr}) AS distinct_value_count, "
        f"         LIST(DISTINCT {assign_expr} ORDER BY {assign_expr}) AS distinct_values "
        f"  FROM {_from_clause(parquet_path)} "
        f"  WHERE {where} "
        f"  GROUP BY 1 "
        f"  HAVING COUNT(DISTINCT {assign_expr}) > 1 "
        f") "
        f"SELECT unit_id, distinct_value_count, distinct_values, row_count "
        f"FROM grouped ORDER BY distinct_value_count DESC, unit_id LIMIT 1000"
    )
    rows = con.execute(sql).fetchall()
    out: list[LeakageViolation] = []
    for row in rows:
        raw_unit_id = row[0]
        raw_distinct_count = row[1]
        raw_distinct_values = row[2]
        raw_row_count = row[3]
        values: tuple[str, ...] = tuple(str(v) for v in (raw_distinct_values or ()))
        if len(values) > _SAMPLE_VALUE_LIMIT:
            values = values[:_SAMPLE_VALUE_LIMIT]
        out.append(
            LeakageViolation(
                unit_kind=unit.name,
                unit_id=str(raw_unit_id),
                distinct_value_count=int(raw_distinct_count),
                distinct_values=values,
                row_count=int(raw_row_count),
            )
        )
    return tuple(out)


def check_split_leakage(
    parquet_path: Path,
    *,
    con: duckdb.DuckDBPyConnection | None = None,
    units: Sequence[LeakageUnit] = ALL_LEAKAGE_UNITS,
) -> tuple[LeakageViolation, ...]:
    """Return any leakage violations for a frozen dataset snapshot.

    A unit is checked only when every column it references exists on the
    snapshot — datasets that don't carry ``primary_fold`` or
    ``holdout_flags`` (legacy stubs, ad-hoc exports) simply skip those
    units.
    """
    owns_connection = con is None
    if con is None:
        con = duckdb.connect(":memory:")
    try:
        columns = _describe_columns(con, parquet_path)
        struct_fields = (
            _holdout_struct_fields(con, parquet_path)
            if _HOLDOUT_STRUCT_COLUMN in columns
            else set()
        )
        applicable = _applicable_units(columns, struct_fields, units)
        violations: list[LeakageViolation] = []
        for unit in applicable:
            violations.extend(_run_unit_check(con, parquet_path, unit))
            _log.debug(
                "split_leakage unit=%s violations=%d",
                unit.name,
                sum(1 for v in violations if v.unit_kind == unit.name),
            )
        return tuple(violations)
    finally:
        if owns_connection:
            con.close()


def violations_to_dataframe(
    violations: Sequence[LeakageViolation],
) -> pl.DataFrame:
    if not violations:
        return pl.DataFrame(
            schema={
                "unit_kind": pl.Utf8,
                "unit_id": pl.Utf8,
                "distinct_value_count": pl.Int64,
                "distinct_values": pl.Utf8,
                "row_count": pl.Int64,
            }
        )
    rows = [
        {
            "unit_kind": v.unit_kind,
            "unit_id": v.unit_id,
            "distinct_value_count": v.distinct_value_count,
            "distinct_values": ",".join(v.distinct_values),
            "row_count": v.row_count,
        }
        for v in violations
    ]
    return pl.DataFrame(rows)


def summarize_violations(
    violations: Sequence[LeakageViolation],
) -> dict[str, int]:
    """Return ``{unit_kind: violation_count}`` for a tuple of violations."""
    out: dict[str, int] = {}
    for v in violations:
        out[v.unit_kind] = out.get(v.unit_kind, 0) + 1
    return out
